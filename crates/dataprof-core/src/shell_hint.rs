//! Shell commands in error hints, written for the platform that prints them
//! (#1164).
//!
//! A hint is pasted into a terminal, so it has to run there for any path. On
//! POSIX the path goes in single quotes, with an apostrophe written `'\''`. On
//! Windows it goes in double quotes, which cmd.exe and PowerShell both read and
//! which a Windows file name cannot contain, and the command is Python's, since
//! Windows ships no gzip, unzip or iconv command line tool.
//!
//! Inside double quotes PowerShell still expands `$name`, runs `$(...)` and
//! reads a backtick as an escape, and cmd.exe expands `%NAME%`. A file named
//! `$(Start-Process calc).csv.gz` would run that as pasted, so for a path
//! holding `$`, `%` or a backtick the Windows command says [`PLACEHOLDER`]
//! where the path goes, and [`Shell::placeholder_note`] says so.

/// The shell a hint is written for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Shell {
    /// sh, bash, zsh and the other POSIX shells.
    Posix,
    /// cmd.exe and PowerShell.
    Windows,
}

/// Stands in for a path that the Windows shells would expand inside quotes.
pub(crate) const PLACEHOLDER: &str = "FILE";

impl Shell {
    /// The shell of the platform this build runs on.
    pub(crate) const HOST: Self = if cfg!(windows) {
        Self::Windows
    } else {
        Self::Posix
    };

    /// `path`, or [`PLACEHOLDER`] when this shell would expand it.
    fn shown(self, path: &str) -> &str {
        match self {
            Self::Windows if path.contains(['$', '%', '`']) => PLACEHOLDER,
            _ => path,
        }
    }

    /// The sentence a hint adds when its command names [`PLACEHOLDER`]
    /// instead of `path`, or nothing.
    pub(crate) fn placeholder_note(self, path: &str) -> &'static str {
        if self.shown(path) == PLACEHOLDER {
            " The path holds a dollar sign, a percent sign or a backtick, which \
             PowerShell or cmd.exe would expand inside quotes, so the command says \
             FILE where the path goes."
        } else {
            ""
        }
    }

    /// `path` as one argument of a command line in this shell.
    pub(crate) fn quote(self, path: &str) -> String {
        match self {
            Self::Posix => format!("'{}'", path.replace('\'', r"'\''")),
            Self::Windows => format!("\"{path}\""),
        }
    }

    /// Decompress the gzip file at `path` beside it, keeping the original and
    /// refusing to replace a file already there.
    pub(crate) fn gunzip(self, path: &str) -> String {
        let path = self.shown(path);
        match self {
            Self::Posix => format!("gunzip -k {}", self.quote(path)),
            // Not `python -m gzip -d`, which opens its output with "wb" and
            // replaces a `data.csv` sitting beside `data.csv.gz`.
            Self::Windows => format!(
                "python -c \"import gzip,shutil,sys;p=sys.argv[1];shutil.copyfileobj(gzip.open(p),open(p.removesuffix('.gz'),'xb'))\" {}",
                self.quote(path)
            ),
        }
    }

    /// Decompress the zstd file at `path` beside it. Windows ships no zstd
    /// decoder, so this one needs `zstd` installed there.
    pub(crate) fn unzstd(self, path: &str) -> String {
        let path = self.shown(path);
        format!("zstd -d {}", self.quote(path))
    }

    /// Extract the zip archive at `path` without replacing an existing file:
    /// into the current directory, skipping what is there, on POSIX, and into
    /// a new `<path>.contents` directory on Windows.
    pub(crate) fn unzip(self, path: &str) -> String {
        let path = self.shown(path);
        match self {
            // `-n` never overwrites, and never stops at a prompt either.
            Self::Posix => format!("unzip -n {}", self.quote(path)),
            // Not `tar -xf`: with Git for Windows on the PATH, `tar` is GNU
            // tar, which reads `C:` as a remote host. `python -m zipfile -e`
            // replaces files without asking, hence a directory of its own.
            Self::Windows => format!(
                "python -m zipfile -e {} {}",
                self.quote(path),
                self.quote(&format!("{path}.contents"))
            ),
        }
    }

    /// Re-encode the file at `path` from `encoding` to UTF-8, into
    /// `<path>.utf8.csv`, refusing to replace a file already there.
    pub(crate) fn reencode(self, path: &str, encoding: &str) -> String {
        // libiconv, which macOS and Git for Windows use, does not know
        // `latin-1`; every iconv and Python know `ISO-8859-1`.
        let encoding = match encoding {
            "latin-1" => "ISO-8859-1",
            other => other,
        };
        let path = self.shown(path);
        let output = format!("{path}.utf8.csv");
        match self {
            // `set -C` refuses to truncate an existing file; the subshell
            // keeps it from staying set in the user's shell.
            Self::Posix => format!(
                "(set -C; iconv -f {encoding} -t UTF-8 {} > {})",
                self.quote(path),
                self.quote(&output)
            ),
            // Without `newline=''` Python would rewrite line endings, and a
            // line break inside a quoted field is data. `copyfileobj` streams,
            // so a large file is not read into memory whole.
            Self::Windows => format!(
                "python -c \"import shutil,sys;p=sys.argv[1];shutil.copyfileobj(open(p,encoding='{encoding}',newline=''),open(p+'.utf8.csv','x',encoding='utf-8',newline=''))\" {}",
                self.quote(path)
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PATH: &str = "hint dir/o'brien.csv";

    #[test]
    fn posix_quotes_close_and_reopen_around_an_apostrophe() {
        assert_eq!(Shell::Posix.quote(PATH), r"'hint dir/o'\''brien.csv'");
        assert_eq!(
            Shell::Posix.gunzip("a b/o'b.csv.gz"),
            r"gunzip -k 'a b/o'\''b.csv.gz'"
        );
    }

    #[test]
    fn windows_quotes_are_double_and_leave_an_apostrophe_alone() {
        assert_eq!(Shell::Windows.quote(PATH), "\"hint dir/o'brien.csv\"");
        assert_eq!(
            Shell::Windows.unzip(r"C:\a b\o'b.zip"),
            r#"python -m zipfile -e "C:\a b\o'b.zip" "C:\a b\o'b.zip.contents""#
        );
    }

    #[test]
    fn windows_names_a_placeholder_for_a_path_its_shells_would_expand() {
        for path in [
            r"C:\d\$(Start-Process calc).gz",
            r"C:\d\%TEMP%.zip",
            r"C:\d\a`b.csv",
        ] {
            for command in [
                Shell::Windows.gunzip(path),
                Shell::Windows.unzstd(path),
                Shell::Windows.unzip(path),
                Shell::Windows.reencode(path, "latin-1"),
            ] {
                assert!(!command.contains(r"C:\d"), "{command}");
                assert!(command.contains("\"FILE"), "{command}");
            }
            assert!(!Shell::Windows.placeholder_note(path).is_empty());
            // Single quotes expand nothing.
            assert!(Shell::Posix.gunzip(path).contains(path));
            assert!(Shell::Posix.placeholder_note(path).is_empty());
        }
        assert!(
            Shell::Windows
                .placeholder_note(r"C:\a b\o'b.zip")
                .is_empty()
        );
    }

    #[test]
    fn latin_1_is_named_as_iconv_knows_it() {
        for shell in [Shell::Posix, Shell::Windows] {
            let command = shell.reencode("x.csv", "latin-1");
            assert!(command.contains("ISO-8859-1"), "{command}");
            assert!(!command.contains("latin-1"), "{command}");
            let command = shell.reencode("x.csv", "windows-1252");
            assert!(command.contains("windows-1252"), "{command}");
        }
    }
}
