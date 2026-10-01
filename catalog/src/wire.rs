//! Strict text validation before pgwire's lossy C-string decoder.
//!
//! Keep upstream dispatch, authentication, query handlers, and encoding. This
//! adapter only frames bounded input and rejects invalid UTF-8 before decoding.
use crate::CataError;
use bytes::BytesMut;
use datafusion_postgres::pgwire::api::{
    ClientInfo, DefaultClient, PgWireConnectionState, PgWireServerHandlers,
};
use datafusion_postgres::pgwire::messages::PgWireFrontendMessage;
use datafusion_postgres::pgwire::tokio::server::{
    PgWireMessageServerCodec, process_error, process_message,
};
use std::collections::HashSet;
use std::io;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout_at};
use tokio_util::codec::{Decoder, Framed};

const MAX_FRAME: usize = 1024 * 1024;

pub(crate) async fn process_socket<H: PgWireServerHandlers>(
    stream: TcpStream,
    handlers: H,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let address = stream.peer_addr()?;
    let mut socket = Framed::new(
        stream,
        PgWireMessageServerCodec::new(DefaultClient::new(address, false)),
    );
    socket.set_state(PgWireConnectionState::AwaitingStartup);
    let startup_deadline = Instant::now() + Duration::from_secs(10);
    let mut input = BytesMut::new();
    let startup_handler = handlers.startup_handler();
    let query = handlers.simple_query_handler();
    let extended = handlers.extended_query_handler();
    let copy = handlers.copy_handler();
    let cancel = handlers.cancel_handler();
    loop {
        let startup = matches!(socket.state(), PgWireConnectionState::AwaitingStartup);
        let initial = startup
            || matches!(
                socket.state(),
                PgWireConnectionState::AuthenticationInProgress
            );
        let frame = next_frame(&mut socket, &mut input, startup);
        let size = if initial {
            match timeout_at(startup_deadline, frame).await {
                Ok(result) => result?,
                Err(_) => return Ok(()),
            }
        } else {
            frame.await?
        };
        let Some(size) = size else {
            return Ok(());
        };
        if startup
            && size == 8
            && matches!(
                u32::from_be_bytes(input[4..8].try_into().unwrap()),
                80877103 | 80877104
            )
        {
            // LIP-0001 is isolated-development plaintext transport, never
            // silently claim TLS. PostgreSQL SSL/GSS probes get refusal.
            let _ = input.split_to(size);
            socket.get_mut().write_all(b"N").await?;
            continue;
        }
        if let Err(error) = validate(&input[..size], startup) {
            process_error(&mut socket, error.into(), false).await?;
            return Ok(());
        }
        let message = match socket.codec_mut().decode(&mut input) {
            Ok(Some(message)) => message,
            _ => return Ok(()),
        };
        if matches!(message, PgWireFrontendMessage::Terminate(_)) {
            return Ok(());
        }
        let is_extended = message.is_extended_query();
        if let Err(error) = process_message(
            message,
            &mut socket,
            startup_handler.clone(),
            query.clone(),
            extended.clone(),
            copy.clone(),
            cancel.clone(),
        )
        .await
        {
            process_error(&mut socket, error, is_extended).await?;
        }
    }
}

async fn next_frame<S>(
    socket: &mut Framed<TcpStream, PgWireMessageServerCodec<S>>,
    input: &mut BytesMut,
    startup: bool,
) -> io::Result<Option<usize>> {
    loop {
        let header = if startup { 0 } else { 1 };
        if input.len() >= header + 4 {
            let length = u32::from_be_bytes(input[header..header + 4].try_into().unwrap()) as usize;
            let minimum = if startup { 8 } else { 4 };
            if length < minimum || length > if startup { 16 * 1024 } else { MAX_FRAME } {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid protocol frame length",
                ));
            }
            if input.len() >= header + length {
                return Ok(Some(header + length));
            }
        }
        let mut chunk = [0; 8192];
        let read = socket.get_mut().read(&mut chunk).await?;
        if read == 0 {
            return Ok(None);
        }
        input.extend_from_slice(&chunk[..read]);
    }
}

fn validate(frame: &[u8], startup: bool) -> crate::Result<()> {
    if startup {
        let protocol = u32::from_be_bytes(frame[4..8].try_into().unwrap());
        if protocol == 80877102 {
            return if frame.len() == 16 {
                Ok(())
            } else {
                Err(protocol_error())
            };
        } // CancelRequest: binary
        if protocol != 196608 {
            return Err(protocol_error());
        }
        let mut body = &frame[8..];
        let mut keys = HashSet::new();
        while body.len() > 1 {
            let key = cstring(&mut body)?;
            if key.is_empty() || !keys.insert(key) {
                return Err(protocol_error());
            }
            cstring(&mut body)?;
        }
        return if body == [0] {
            Ok(())
        } else {
            Err(protocol_error())
        };
    }
    let mut body = &frame[5..];
    match frame[0] {
        b'Q' => {
            cstring(&mut body)?;
        }
        b'P' => {
            cstring(&mut body)?;
            cstring(&mut body)?;
            let count = u16(&mut body)? as usize;
            take(&mut body, count * 4)?;
        }
        b'B' => {
            cstring(&mut body)?;
            cstring(&mut body)?;
            let n = u16(&mut body)? as usize;
            if n > 1024 {
                return Err(protocol_error());
            }
            let mut formats = Vec::new();
            for _ in 0..n {
                let format = u16(&mut body)?;
                if format > 1 {
                    return Err(protocol_error());
                }
                formats.push(format);
            }
            let parameters = u16(&mut body)? as usize;
            if parameters > 1024 || (n > 1 && n != parameters) {
                return Err(protocol_error());
            }
            for i in 0..parameters {
                let length = i32::from_be_bytes(take(&mut body, 4)?.try_into().unwrap());
                if length == -1 {
                    continue;
                }
                if length < 0 {
                    return Err(protocol_error());
                }
                let value = take(&mut body, length as usize)?;
                let format = if n == 0 {
                    0
                } else if n == 1 {
                    formats[0]
                } else {
                    formats[i]
                };
                if format == 0 {
                    std::str::from_utf8(value).map_err(|_| encoding_error())?;
                    if value.contains(&0) {
                        return Err(encoding_error());
                    }
                }
            }
            let n = u16(&mut body)? as usize;
            for _ in 0..n {
                if u16(&mut body)? > 1 {
                    return Err(protocol_error());
                }
            }
        }
        b'D' | b'C' => {
            take(&mut body, 1)?;
            cstring(&mut body)?;
        }
        b'E' => {
            cstring(&mut body)?;
            take(&mut body, 4)?;
        }
        b'S' | b'H' | b'X' => {}
        // Authentication input is bounded again by the SCRAM implementation.
        b'p' => return Ok(()),
        _ => return Err(protocol_error()),
    }
    if body.is_empty() {
        Ok(())
    } else {
        Err(protocol_error())
    }
}
fn take<'a>(body: &mut &'a [u8], n: usize) -> crate::Result<&'a [u8]> {
    if n > body.len() {
        return Err(protocol_error());
    }
    let (head, tail) = body.split_at(n);
    *body = tail;
    Ok(head)
}
fn u16(body: &mut &[u8]) -> crate::Result<u16> {
    Ok(u16::from_be_bytes(take(body, 2)?.try_into().unwrap()))
}
fn cstring<'a>(body: &mut &'a [u8]) -> crate::Result<&'a str> {
    let length = body
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(protocol_error)?;
    let bytes = take(body, length + 1)?;
    std::str::from_utf8(&bytes[..length]).map_err(|_| encoding_error())
}
fn protocol_error() -> CataError {
    CataError::sql("08P01", "invalid protocol message")
}
fn encoding_error() -> CataError {
    CataError::sql("22021", "invalid UTF8 text")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_invalid_text_but_allows_binary_values() {
        assert!(validate(&[b'Q', 0, 0, 0, 6, 0xff, 0], false).is_err());
        assert!(validate(&[b'Q', 0, 0, 0, 6, b'x', 0], false).is_ok());
        let mut bind = vec![
            b'B', 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0xff, 0, 0,
        ];
        assert!(validate(&bind, false).is_ok());
        bind[10] = 0;
        assert!(validate(&bind, false).is_err());
        let mut cancel = vec![0, 0, 0, 16, 4, 210, 22, 46];
        assert!(validate(&cancel, true).is_err());
        cancel.extend_from_slice(&[0; 8]);
        assert!(validate(&cancel, true).is_ok());
        assert!(validate(&[0, 0, 0, 9, 0, 4, 0, 0, 0], true).is_err());
    }
}
