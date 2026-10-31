//! The commands error hints suggest run as pasted, for any path (#1164).
//!
//! The hints for compressed, zip and non-UTF-8 files used to put the path in
//! POSIX single quotes, so a path with an apostrophe ended the quote early,
//! cmd.exe split a path with a space, and the tools named are not on Windows.
//! Each case here writes a real file under `hint dir/o'brien.*`, takes the
//! command from the error's suggestion, runs it through every shell the host
//! has (sh; cmd.exe and PowerShell), and profiles what it produced.
//!
//! `python/tests/test_error_hint_commands.py` is the Python twin.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use dataprof::Profiler;
use tempfile::TempDir;

const CSV: &[u8] = b"id,city\n1,Rome\n2,Milan\n";

fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// A gzip member holding `data` in one stored deflate block.
fn gzip(data: &[u8]) -> Vec<u8> {
    let len = u16::try_from(data.len()).unwrap();
    let mut out = vec![0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff, 1];
    out.extend(len.to_le_bytes());
    out.extend((!len).to_le_bytes());
    out.extend(data);
    out.extend(crc32(data).to_le_bytes());
    out.extend(u32::try_from(data.len()).unwrap().to_le_bytes());
    out
}

/// A zstd frame holding `data` in one raw block.
fn zstd(data: &[u8]) -> Vec<u8> {
    // Single segment, a one-byte content size, no checksum.
    let mut out = vec![
        0x28,
        0xb5,
        0x2f,
        0xfd,
        0x20,
        u8::try_from(data.len()).unwrap(),
    ];
    let block_header = (u32::try_from(data.len()).unwrap() << 3) | 1;
    out.extend(&block_header.to_le_bytes()[..3]);
    out.extend(data);
    out
}

/// A zip archive holding `data` as the stored entry `name`.
fn zip(name: &str, data: &[u8]) -> Vec<u8> {
    let crc = crc32(data).to_le_bytes();
    let size = u32::try_from(data.len()).unwrap().to_le_bytes();
    let name_len = u16::try_from(name.len()).unwrap().to_le_bytes();
    // Version 2.0, no flags, stored, 1980-01-01 00:00.
    let common: Vec<u8> = [
        &[20, 0, 0, 0, 0, 0, 0, 0, 0x21, 0][..],
        &crc,
        &size,
        &size,
        &name_len,
    ]
    .concat();

    let mut out = vec![0x50, 0x4b, 0x03, 0x04];
    out.extend(&common);
    out.extend([0, 0]);
    out.extend(name.as_bytes());
    out.extend(data);

    let directory_offset = u32::try_from(out.len()).unwrap();
    let mut directory = vec![0x50, 0x4b, 0x01, 0x02, 20, 0];
    directory.extend(&common);
    directory.extend([0; 12]);
    directory.extend(0u32.to_le_bytes());
    directory.extend(name.as_bytes());
    let directory_len = u32::try_from(directory.len()).unwrap();
    out.extend(directory);

    out.extend([0x50, 0x4b, 0x05, 0x06, 0, 0, 0, 0, 1, 0, 1, 0]);
    out.extend(directory_len.to_le_bytes());
    out.extend(directory_offset.to_le_bytes());
    out.extend([0, 0]);
    out
}

struct Case {
    name: &'static str,
    /// File name under `hint dir`.
    file: &'static str,
    bytes: Vec<u8>,
    /// What the command writes, relative to the working directory.
    output: &'static str,
    /// The program the command needs, when the host may not have it.
    needs: Option<&'static str>,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "gzip",
            file: "o'brien.csv.gz",
            bytes: gzip(CSV),
            output: "hint dir/o'brien.csv",
            needs: None,
        },
        Case {
            name: "zstd",
            file: "o'brien.csv.zst",
            bytes: zstd(CSV),
            output: "hint dir/o'brien.csv",
            // Windows ships no zstd decoder, and the hint says so.
            needs: Some("zstd"),
        },
        Case {
            name: "zip",
            file: "o'brien.zip",
            bytes: zip("data.csv", CSV),
            output: "data.csv",
            needs: None,
        },
        Case {
            name: "latin-1",
            file: "o'brien.csv",
            bytes: b"id,city\n1,Z\xfcrich\n2,M\xe1laga\n".to_vec(),
            output: "hint dir/o'brien.csv.utf8.csv",
            needs: None,
        },
    ]
}

type Shell = (&'static str, fn(&str, &Path) -> std::io::Result<Output>);

#[cfg(not(windows))]
fn shells() -> Vec<Shell> {
    vec![("sh", |command, cwd| {
        Command::new("sh")
            .arg("-c")
            .arg(command)
            .current_dir(cwd)
            .output()
    })]
}

#[cfg(windows)]
fn shells() -> Vec<Shell> {
    use std::os::windows::process::CommandExt;
    vec![
        ("cmd", |command, cwd| {
            // `/s` strips the outer quotes and leaves the command as typed.
            Command::new("cmd")
                .raw_arg(format!("/d /s /c \"{command}\""))
                .current_dir(cwd)
                .output()
        }),
        ("powershell", |command, cwd| {
            let script = cwd.join("hint.ps1");
            std::fs::write(&script, format!("{command}\r\nexit $LASTEXITCODE\r\n"))?;
            let output = Command::new("powershell")
                .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
                .arg(&script)
                .current_dir(cwd)
                .output();
            std::fs::remove_file(&script)?;
            output
        }),
    ]
}

fn installed(program: &str) -> bool {
    Command::new(program).arg("--version").output().is_ok()
}

/// The command a suggestion quotes between backticks.
fn command_in(suggestion: &str) -> Option<&str> {
    suggestion.split('`').nth(1)
}

/// What is wrong with running `case`'s hint through `shell`, if anything.
fn misrun(case: &Case, (shell, run): &Shell) -> Option<String> {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("hint dir");
    std::fs::create_dir(&dir).unwrap();
    let path: PathBuf = dir.join(case.file);
    std::fs::write(&path, &case.bytes).unwrap();

    let err = match Profiler::new().analyze_file(&path) {
        Ok(_) => return Some("profiled without an error".to_string()),
        Err(err) => err,
    };
    let suggestion = err.suggestion().unwrap_or_default();
    let Some(command) = command_in(&suggestion) else {
        return Some(format!("no command in the suggestion: {suggestion}"));
    };
    if !err.to_string().contains(command) {
        return Some(format!("the message gives another command: {err}"));
    }

    let output = match run(command, tmp.path()) {
        Ok(output) => output,
        Err(err) => return Some(format!("{shell} did not start: {err}")),
    };
    if !output.status.success() {
        return Some(format!(
            "{shell} `{command}` failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let produced = tmp.path().join(case.output);
    match Profiler::new().analyze_file(&produced) {
        Ok(report) if report.execution.rows_processed == 2 => None,
        Ok(report) => Some(format!(
            "{shell} `{command}`: the result has {} rows",
            report.execution.rows_processed
        )),
        Err(err) => Some(format!("{shell} `{command}`: the result fails: {err}")),
    }
}

#[test]
fn every_suggested_command_runs_on_a_path_with_a_space_and_an_apostrophe() {
    let mut wrong = Vec::new();
    for case in cases() {
        if let Some(program) = case.needs
            && !installed(program)
        {
            eprintln!("{}: skipped, `{program}` is not installed", case.name);
            continue;
        }
        for shell in shells() {
            if let Some(problem) = misrun(&case, &shell) {
                wrong.push(format!("{} / {}: {problem}", case.name, shell.0));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
