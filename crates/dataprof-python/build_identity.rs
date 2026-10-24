//! The commit the extension is built from, read from git by `build.rs`.
//!
//! The crate's unit tests include this file through `#[path]`, so the rules
//! that decide when a git answer describes this build are tested directly.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The sources `dirty` describes: what the native module is compiled from.
/// `build.rs` watches the same paths, so the flag is recomputed whenever one
/// of them changes. The Python package is left out: the extension build
/// writes into it, and watching it would rebuild on every build.
pub const COMPILED_SOURCES: [&str; 3] = ["Cargo.toml", "Cargo.lock", "crates"];

/// What git says about the checkout a build runs in.
pub struct Identity {
    /// The full hash of `HEAD`.
    pub commit: String,
    /// Whether [`COMPILED_SOURCES`] differ from `HEAD`, including untracked
    /// files; `None` when `git status` failed.
    pub dirty: Option<bool>,
    /// The existing git files whose change moves `HEAD`.
    pub git_paths: Vec<PathBuf>,
}

/// The workspace root of the crate at `manifest_dir`.
pub fn workspace_root(manifest_dir: &Path) -> Option<&Path> {
    manifest_dir.parent()?.parent()
}

/// The identity of the checkout the crate at `manifest_dir` is built from.
///
/// `None` unless the workspace root is itself the top of a git checkout. A
/// crate unpacked from a package carries `.cargo_vcs_info.json` and is built
/// outside any checkout of ours; an unpacked sdist or a copy of the sources
/// can sit inside someone else's repository, whose commit would describe
/// that repository instead of this build.
pub fn identify(manifest_dir: &Path) -> Option<Identity> {
    if manifest_dir.join(".cargo_vcs_info.json").exists() {
        return None;
    }
    let root = workspace_root(manifest_dir)?;
    let toplevel = git(root, &["rev-parse", "--show-toplevel"])?;
    if std::fs::canonicalize(&toplevel).ok()? != std::fs::canonicalize(root).ok()? {
        return None;
    }
    let commit = git(root, &["rev-parse", "--verify", "HEAD"])?;
    let mut status = vec!["status", "--porcelain", "--untracked-files=normal", "--"];
    status.extend(COMPILED_SOURCES);
    let dirty = git(root, &status).map(|changes| !changes.is_empty());
    let git_paths = ["HEAD", "refs/heads", "packed-refs"]
        .into_iter()
        .filter_map(|name| git(root, &["rev-parse", "--git-path", name]))
        .map(|path| root.join(path))
        .filter(|path| path.exists())
        .collect();
    Some(Identity {
        commit,
        dirty,
        git_paths,
    })
}

/// Trimmed stdout of a git command run at `root`, or `None` when git is
/// missing or the command fails.
fn git(root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        // A checkout owned by another user, as in a manylinux container, is
        // refused without this. It trusts only the tree being built, whose
        // code this build already runs.
        .arg("-c")
        .arg(format!("safe.directory={}", root.display()))
        .arg("--no-optional-locks")
        .arg("-C")
        .arg(root)
        .args(args)
        // A git hook running cargo exports these, and they would point the
        // commands at the hook's repository whatever `-C` says.
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn run_git(dir: &Path, args: &[&str]) -> bool {
        Command::new("git")
            .args([
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@example.com",
                "-c",
                "commit.gpgsign=false",
                "-c",
                "init.defaultBranch=main",
            ])
            .arg("-C")
            .arg(dir)
            .args(args)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .env_remove("GIT_COMMON_DIR")
            .env_remove("GIT_OBJECT_DIRECTORY")
            .output()
            .is_ok_and(|output| output.status.success())
    }

    /// A workspace at `root` with one crate, committed, returning the crate's
    /// manifest directory; `None` where git is not installed.
    fn committed_workspace(root: &Path) -> Option<PathBuf> {
        let manifest_dir = root.join("crates").join("dataprof-python");
        fs::create_dir_all(&manifest_dir).unwrap();
        fs::write(root.join("Cargo.toml"), "[workspace]\n").unwrap();
        fs::write(manifest_dir.join("lib.rs"), "\n").unwrap();
        fs::write(root.join("README.md"), "\n").unwrap();
        (run_git(root, &["init", "-q"])
            && run_git(root, &["add", "."])
            && run_git(root, &["commit", "-q", "-m", "init"]))
        .then_some(manifest_dir)
    }

    #[test]
    fn a_checkout_rooted_at_the_workspace_records_head_and_compiled_changes() {
        let temp = tempfile::tempdir().unwrap();
        let Some(manifest_dir) = committed_workspace(temp.path()) else {
            eprintln!("git is not available; skipping");
            return;
        };
        let identity = identify(&manifest_dir).expect("a checkout at the root is identified");
        let head = git(temp.path(), &["rev-parse", "HEAD"]).unwrap();
        assert_eq!(identity.commit, head);
        assert_eq!(identity.dirty, Some(false));
        assert!(identity.git_paths.iter().any(|path| path.ends_with("HEAD")));

        // Outside the compiled sources: still clean.
        fs::write(temp.path().join("README.md"), "changed\n").unwrap();
        assert_eq!(identify(&manifest_dir).unwrap().dirty, Some(false));

        // An untracked file among them counts.
        fs::write(manifest_dir.join("new.rs"), "\n").unwrap();
        assert_eq!(identify(&manifest_dir).unwrap().dirty, Some(true));
    }

    #[test]
    fn a_workspace_inside_another_repository_records_nothing() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("vendor").join("dataprof");
        let manifest_dir = root.join("crates").join("dataprof-python");
        fs::create_dir_all(&manifest_dir).unwrap();
        fs::write(manifest_dir.join("lib.rs"), "\n").unwrap();
        if !(run_git(temp.path(), &["init", "-q"])
            && run_git(temp.path(), &["add", "."])
            && run_git(temp.path(), &["commit", "-q", "-m", "init"]))
        {
            eprintln!("git is not available; skipping");
            return;
        }
        assert!(identify(&manifest_dir).is_none());
    }

    #[test]
    fn a_packaged_crate_records_nothing_even_inside_a_checkout() {
        let temp = tempfile::tempdir().unwrap();
        let Some(manifest_dir) = committed_workspace(temp.path()) else {
            eprintln!("git is not available; skipping");
            return;
        };
        fs::write(manifest_dir.join(".cargo_vcs_info.json"), "{}\n").unwrap();
        assert!(identify(&manifest_dir).is_none());
    }

    #[test]
    fn a_workspace_outside_any_checkout_records_nothing() {
        let temp = tempfile::tempdir().unwrap();
        let manifest_dir = temp.path().join("crates").join("dataprof-python");
        fs::create_dir_all(&manifest_dir).unwrap();
        assert!(identify(&manifest_dir).is_none());
    }
}
