use tracing::{error, info};

pub async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.ok();
    }
}

pub fn write_pid_file(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let pid = std::process::id();
    std::fs::write(path, pid.to_string())
        .map_err(|e| format!("failed to write PID file '{}': {}", path, e))?;
    info!(pid = pid, path = path, "pid file written");
    Ok(())
}

pub fn remove_pid_file(path: &str) {
    if let Err(e) = std::fs::remove_file(path) {
        error!("failed to remove PID file '{}': {}", path, e);
    }
}

pub fn read_pid_file(path: &str) -> Result<i32, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read PID file '{}': {}", path, e))?;
    let pid: i32 = contents
        .trim()
        .parse()
        .map_err(|e| format!("invalid PID in '{}': {}", path, e))?;
    Ok(pid)
}

pub fn send_sigterm(pid: i32) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    {
        let ret = unsafe { libc::kill(pid, libc::SIGTERM) };
        if ret != 0 {
            return Err(format!(
                "failed to send SIGTERM to pid {}: {}",
                pid,
                std::io::Error::last_os_error()
            )
            .into());
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        Err("stop command is only supported on Unix systems".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_reads_and_removes_pid_files() {
        let path = std::env::temp_dir().join(format!("lyra-catalog-test-{}.pid", std::process::id()));

        write_pid_file(path.to_str().unwrap()).unwrap();
        let pid = read_pid_file(path.to_str().unwrap()).unwrap();
        assert_eq!(pid, std::process::id() as i32);

        remove_pid_file(path.to_str().unwrap());
        remove_pid_file(path.to_str().unwrap());
        assert!(read_pid_file(path.to_str().unwrap()).is_err());
    }

    #[test]
    fn rejects_invalid_pid_files() {
        let path = std::env::temp_dir().join(format!("lyra-catalog-invalid-{}.pid", std::process::id()));
        std::fs::write(&path, "not-a-number").unwrap();
        assert!(read_pid_file(path.to_str().unwrap()).is_err());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn signals_running_and_missing_processes() {
        let child = std::process::Command::new("sleep")
            .arg("5")
            .spawn()
            .unwrap();
        send_sigterm(child.id() as i32).unwrap();
        let _ = child.wait_with_output();

        assert!(send_sigterm(i32::MAX).is_err());
    }
}
