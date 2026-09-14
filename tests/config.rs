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

#[test]
fn checkpoint_proof_concurrency_limit_defaults_to_half_the_cpu_cores() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output = run_electrs(temp_dir.path(), &["-vv"]);
    let stderr = String::from_utf8(output.stderr).unwrap();

    let expected = (num_cpus::get() / 2).max(1);
    let needle = format!("electrum_checkpoint_proof_concurrency_limit: {}", expected);
    assert!(
        stderr.contains(&needle),
        "expected '{}' in stderr, got: {}",
        needle,
        stderr
    );
}

#[test]
fn checkpoint_proof_concurrency_limit_can_be_overridden() {
    let temp_dir = tempfile::tempdir().unwrap();
    let output = run_electrs(
        temp_dir.path(),
        &["-vv", "--electrum-checkpoint-proof-concurrency-limit", "7"],
    );
    let stderr = String::from_utf8(output.stderr).unwrap();

    assert!(
        stderr.contains("electrum_checkpoint_proof_concurrency_limit: 7"),
        "explicit override was not honored, stderr: {}",
        stderr
    );
}
