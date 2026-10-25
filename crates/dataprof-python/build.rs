//! Records the commit the extension is built from; see `build_identity.rs`.
//!
//! This lives in the leaf crate on purpose: the script reruns whenever the
//! compiled sources change, and a rerun rebuilds every crate that depends on
//! the one that owns it.

use std::env;
use std::path::PathBuf;

#[path = "build_identity.rs"]
mod build_identity;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=build_identity.rs");
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("set by cargo"));
    // Watched whether or not git answered, so a build that could not be
    // identified is asked again on the next source change. Only existing
    // paths: cargo reruns on every build for one that is missing.
    if let Some(root) = build_identity::workspace_root(&manifest_dir) {
        for source in build_identity::COMPILED_SOURCES {
            let path = root.join(source);
            if path.exists() {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }
    let identity = build_identity::identify(&manifest_dir);
    for path in identity.iter().flat_map(|identity| &identity.git_paths) {
        println!("cargo:rerun-if-changed={}", path.display());
    }
    // Both are always set, empty when unknown, so a variable of the same name
    // in the caller's environment cannot stand in for what git did not say.
    let commit = identity
        .as_ref()
        .map_or("", |identity| identity.commit.as_str());
    let dirty = match identity.as_ref().and_then(|identity| identity.dirty) {
        Some(true) => "true",
        Some(false) => "false",
        None => "",
    };
    println!("cargo:rustc-env=DATAPROF_BUILD_COMMIT={commit}");
    println!("cargo:rustc-env=DATAPROF_BUILD_DIRTY={dirty}");
}
