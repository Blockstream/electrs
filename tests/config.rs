use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output};

fn run_electrs(temp_dir: &Path, extra_args: &[&str]) -> Output {
    let monitoring_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let monitoring_addr = monitoring_listener.local_addr().unwrap().to_string();

    let mut command = Command::new(env!("CARGO_BIN_EXE_electrs"));
    command.args([
        "--db-dir",
        temp_dir.join("db").to_str().unwrap(),
        "--daemon-dir",
        temp_dir.to_str().unwrap(),
        "--daemon-rpc-addr",
        "127.0.0.1:1",
        "--monitoring-addr",
        monitoring_addr.as_str(),
    ]);

    #[cfg(feature = "liquid")]
    command.args(["--network", "liquidregtest"]);

    command
        .args(extra_args)
        .output()
        .expect("failed to run electrs")
}

#[test]
fn startup_never_logs_static_auth_password() {
    let password = "poc-PASSWORD-123";
    let cookie = format!("poc-user:{}", password);

    for verbosity in [None, Some("-v"), Some("-vv")] {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut extra_args = vec!["--cookie", cookie.as_str()];
        if let Some(verbosity) = verbosity {
            extra_args.push(verbosity);
        }

        let output = run_electrs(temp_dir.path(), &extra_args);
        let stderr = String::from_utf8(output.stderr).unwrap();

        assert!(!output.status.success(), "electrs unexpectedly succeeded");
        assert!(
            !stderr.contains(password),
            "password was logged at verbosity {:?}: {}",
            verbosity,
            stderr
        );

        if verbosity.is_some() {
            assert!(
                stderr.contains(r#"daemon authentication: UserPass("poc-user", "<sensitive>")"#),
                "redacted authentication mode missing from stderr: {}",
                stderr
            );
        }
    }
}

#[test]
fn startup_debug_log_identifies_cookie_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output = run_electrs(temp_dir.path(), &["-v"]);
    let stderr = String::from_utf8(output.stderr).unwrap();
    let daemon_dir = temp_dir.path().to_path_buf();
    #[cfg(feature = "liquid")]
    let daemon_dir = daemon_dir.join("liquidregtest");
    let expected = format!(
        "daemon authentication: CookieFile({:?})",
        daemon_dir.join(".cookie")
    );

    assert!(!output.status.success(), "electrs unexpectedly succeeded");
    assert!(
        stderr.contains(&expected),
        "cookie-file authentication mode missing from stderr: {}",
        stderr
    );
}

#[cfg(feature = "liquid")]
#[test]
fn removed_asset_db_path_is_a_startup_error() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output = run_electrs(
        temp_dir.path(),
        &["--asset-db-path", temp_dir.path().to_str().unwrap()],
    );
    let stderr = String::from_utf8(output.stderr).unwrap();

    assert!(!output.status.success());
    assert!(stderr.contains("--asset-db-path is no longer supported"));
    assert!(stderr.contains("--asset-registry-url"));
}

#[cfg(feature = "liquid")]
#[test]
fn credentialed_asset_registry_url_is_rejected_without_echoing_credentials() {
    let temp_dir = tempfile::tempdir().unwrap();
    let password = "registry-PASSWORD-123";
    let url = format!("https://user:{}@registry.example/api", password);
    let output = run_electrs(temp_dir.path(), &["--asset-registry-url", &url]);
    let stderr = String::from_utf8(output.stderr).unwrap();

    assert!(!output.status.success());
    assert!(stderr.contains("must not contain a username or password"));
    assert!(!stderr.contains(password));
}
