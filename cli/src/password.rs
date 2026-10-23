use lyra_meta::proto::pb_meta::ScramSha256Verifier;
use lyra_meta::utils::verifier::make_verifier;
use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use zeroize::Zeroizing;

pub fn read_verifier(path: &Path) -> Result<ScramSha256Verifier, &'static str> {
    // O_NONBLOCK avoids hanging on a named pipe before fstat can reject it.
    // Follow projected Secret symlinks, then validate the opened descriptor.
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| "password file cannot be read")?;
    let metadata = file
        .metadata()
        .map_err(|_| "password file metadata unavailable")?;
    if !metadata.is_file()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(
            "password file must be regular, owned by this user, and have no group/other permissions",
        );
    }
    let mut input = Zeroizing::new(Vec::new());
    file.take(1027)
        .read_to_end(&mut input)
        .map_err(|_| "password file cannot be read")?;
    let len = input.len();
    if input.ends_with(b"\r\n") {
        input.truncate(len - 2);
    } else if input.ends_with(b"\n") {
        input.truncate(len - 1);
    }
    if input.is_empty()
        || input.len() > 1024
        || input.iter().any(|b| matches!(b, 0 | b'\r' | b'\n'))
    {
        return Err("password must contain 1 to 1024 UTF-8 bytes on one line without NUL");
    }
    let password = std::str::from_utf8(&input).map_err(|_| "password must be UTF-8")?;
    make_verifier(password).map_err(|_| "password verifier could not be derived")
}

#[cfg(test)]
mod tests {
    use super::*;
    use lyra_meta::utils::verifier::verify_verifier;
    use std::fs::{self, Permissions};
    use std::os::unix::fs::PermissionsExt;
    #[test]
    fn protected_password_file_rules() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("password");
        for suffix in ["", "\n", "\r\n"] {
            fs::write(&path, format!("  keep whitespace  {suffix}")).unwrap();
            fs::set_permissions(&path, Permissions::from_mode(0o600)).unwrap();
            let verifier = read_verifier(&path).unwrap();
            assert!(verify_verifier("  keep whitespace  ", &verifier));
        }
        for input in [
            vec![],
            vec![b'x'; 1025],
            b"a\nb".to_vec(),
            b"a\0b".to_vec(),
            b"x\n\n".to_vec(),
            vec![0xff],
        ] {
            fs::write(&path, input).unwrap();
            assert!(read_verifier(&path).is_err());
        }
        fs::write(&path, "valid").unwrap();
        fs::set_permissions(&path, Permissions::from_mode(0o644)).unwrap();
        assert!(read_verifier(&path).is_err());
        assert!(read_verifier(dir.path()).is_err());
    }
}
