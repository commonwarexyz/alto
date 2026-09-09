use serde_yaml::Value;
use std::{fs, process::Command};
use uuid::Uuid;

fn exported_array(config: &str, name: &str) -> Value {
    let (_, declaration) = config.split_once(&format!("export const {name}:")).unwrap();
    let (_, value) = declaration.split_once(" = ").unwrap();
    serde_yaml::from_str(value.split(';').next().unwrap()).unwrap()
}

#[test]
fn indexer_generation_preserves_unmapped_participants() {
    for (mode, certificate_mode) in [("stable", "standard"), ("rotating", "vrf")] {
        let output = std::env::temp_dir().join(format!("alto-deploy-region-{}", Uuid::new_v4()));
        let mut command = Command::new(env!("CARGO_BIN_EXE_deploy"));
        command.args([
            "generate",
            "--peers",
            "4",
            "--bootstrappers",
            "1",
            "--worker-threads",
            "1",
            "--log-level",
            "info",
            "--mailbox-size",
            "16384",
            "--deque-size",
            "256",
            "--signature-threads",
            "1",
            "--leader-mode",
            mode,
            "--leader-delay-ms",
            "10",
        ]);
        if mode == "stable" {
            command.args([
                "--leader-term-length",
                "1000",
                "--leader-optimistic-views",
                "48",
            ]);
        }
        command
            .arg("--output")
            .arg(&output)
            .args([
                "remote",
                "--regions",
                "us-east-1,eu-west-2,us-west-1",
                "--monitoring-instance-type",
                "c7gd.4xlarge",
                "--monitoring-storage-size",
                "100",
                "--instance-type",
                "c7gd.4xlarge",
                "--storage-size",
                "25",
                "--dashboard",
            ])
            .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/dashboard.json"))
            .arg("--indexer");
        let result = command.output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );

        let deployment: Value =
            serde_yaml::from_str(&fs::read_to_string(output.join("config.yaml")).unwrap()).unwrap();
        let indexer: Value =
            serde_yaml::from_str(&fs::read_to_string(output.join("indexer.yaml")).unwrap())
                .unwrap();
        let validators = deployment["instances"]
            .as_sequence()
            .unwrap()
            .iter()
            .filter(|instance| instance["binary"] == "validator")
            .collect::<Vec<_>>();
        let participants = indexer["explorer"]["participants"].as_sequence().unwrap();
        let locations = indexer["explorer"]["locations"].as_sequence().unwrap();
        assert_eq!(validators.len(), 4);
        assert_eq!(participants.len(), 4);
        assert_eq!(locations.len(), 4);
        assert_eq!(indexer["certificate_mode"], certificate_mode);
        for (participant, validator) in participants.iter().zip(&validators) {
            assert_eq!(participant, &validator["name"]);
        }

        // Missing coordinates keep their slot between known participants.
        assert_eq!(locations[0][1], "Ashburn");
        assert!(locations[1].is_null());
        assert_eq!(locations[2][1], "San Francisco");
        assert_eq!(locations[3][1], "Ashburn");

        let result = Command::new(env!("CARGO_BIN_EXE_deploy"))
            .arg("explorer")
            .arg("--dir")
            .arg(&output)
            .args(["--backend-url", "localhost:8080", "remote"])
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let config = fs::read_to_string(output.join("config.ts")).unwrap();
        assert_eq!(
            &exported_array(&config, "PARTICIPANTS"),
            &indexer["explorer"]["participants"]
        );
        assert_eq!(
            &exported_array(&config, "LOCATIONS"),
            &indexer["explorer"]["locations"]
        );
        fs::remove_dir_all(output).unwrap();
    }
}
