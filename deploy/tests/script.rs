#![cfg(unix)]

use std::{fs, os::unix::fs::PermissionsExt, process::Command};
use uuid::Uuid;

#[test]
fn scripted_build_embeds_the_current_frontend() {
    let tag = format!("alto-build-test-{}", Uuid::new_v4());
    let output = std::env::temp_dir().join(&tag);
    for directory in ["bin", "deploy", "explorer/build"] {
        fs::create_dir_all(output.join(directory)).unwrap();
    }
    fs::write(output.join("deploy.sh"), include_str!("../../deploy.sh")).unwrap();
    fs::write(output.join("deploy/dashboard.json"), "{}").unwrap();
    fs::write(output.join("explorer/build/index.html"), "old frontend").unwrap();

    // Run the real script with isolated tools that expose its producer/consumer handoff.
    let stub = r#"#!/bin/sh
set -eu
case "${0##*/}" in
    uname) echo Linux ;;
    cargo)
        mkdir -p assets
        printf 'tag: %s\n' "$ALTO_BUILD_TEST_TAG" > assets/config.yaml
        ;;
    npm)
        if [ "$3" = run ] && [ "$4" = build ]; then
            mkdir -p "explorer/${BUILD_PATH:-build}"
            frontend_base="${PUBLIC_URL:-}"
            printf '<script src="%s/runtime-config.js"></script>current frontend' "${frontend_base%/}" > "explorer/${BUILD_PATH:-build}/index.html"
        fi
        ;;
    just) cp explorer/build/index.html assets/embedded.html ;;
    *) ;;
esac
"#;
    for tool in [
        "cargo",
        "just",
        "docker",
        "deployer",
        "npm",
        "wasm-pack",
        "uname",
    ] {
        let path = output.join("bin").join(tool);
        fs::write(&path, stub).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let result = Command::new("bash")
        .arg(output.join("deploy.sh"))
        .arg("rotating")
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", output.join("bin").display()),
        )
        .env("BUILD_PATH", "alternate")
        .env("PUBLIC_URL", "/alternate")
        .env("ALTO_BUILD_TEST_TAG", &tag)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let embedded = fs::read_to_string(output.join("assets/embedded.html")).unwrap();
    assert!(embedded.ends_with("current frontend"), "{embedded}");
    assert!(
        embedded.contains(r#"src="/runtime-config.js""#),
        "{embedded}"
    );
    fs::remove_dir_all(output).unwrap();
}
