use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn collect_files(directory: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("failed to read explorer build entry").path();
        if path.is_dir() {
            collect_files(&path, files);
        } else {
            files.push(path);
        }
    }
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("css") => "text/css; charset=utf-8",
        Some("html") => "text/html; charset=utf-8",
        Some("ico") => "image/x-icon",
        Some("js") => "application/javascript; charset=utf-8",
        Some("json") | Some("map") => "application/json; charset=utf-8",
        Some("png") => "image/png",
        Some("svg") => "image/svg+xml",
        Some("txt") => "text/plain; charset=utf-8",
        Some("wasm") => "application/wasm",
        _ => "application/octet-stream",
    }
}

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let explorer_build = manifest_dir.join("../explorer/build");
    println!("cargo:rerun-if-changed={}", explorer_build.display());

    let mut files = Vec::new();
    collect_files(&explorer_build, &mut files);
    files.sort();

    let mut generated =
        String::from("pub(crate) static EXPLORER_ASSETS: &[(&str, &[u8], &str)] = &[\n");
    for path in files {
        let route = path
            .strip_prefix(&explorer_build)
            .expect("explorer asset is outside build directory")
            .to_string_lossy()
            .replace('\\', "/");
        generated.push_str(&format!(
            "    ({route:?}, include_bytes!({path:?}), {content_type:?}),\n",
            path = path.to_string_lossy(),
            content_type = content_type(&path),
        ));
    }
    generated.push_str("];\n");

    let output = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("explorer_assets.rs");
    fs::write(output, generated).expect("failed to generate embedded explorer assets");
}
