//! Shell commands in error hints, written for the platform that prints them
//! (#1164).
//!
//! A hint is pasted into a terminal, so it has to run there for any path. On
//! POSIX the path goes in single quotes, with an apostrophe written `'\''`. On
//! Windows it goes in double quotes, which cmd.exe and PowerShell both read and
//! which a Windows file name cannot contain, and the command is Python's, since
//! Windows ships no gzip, unzip or iconv command line tool. Inside
//! double quotes PowerShell still expands `$name` and cmd.exe `%NAME%`, so a
//! path holding those is the one case left that does not paste as is.

/// The shell a hint is written for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Shell {
    /// sh, bash, zsh and the other POSIX shells.
    Posix,
    /// cmd.exe and PowerShell.
    Windows,
}

impl Shell {
    /// The shell of the platform this build runs on.
    pub(crate) const HOST: Self = if cfg!(windows) {
        Self::Windows
    } else {
        Self::Posix
    };

    /// `path` as one argument of a command line in this shell.
    pub(crate) fn quote(self, path: &str) -> String {
        match self {
            Self::Posix => format!("'{}'", path.replace('\'', r"'\''")),
            Self::Windows => format!("\"{path}\""),
        }
    }

    /// Decompress the gzip file at `path` beside it, keeping the original.
    pub(crate) fn gunzip(self, path: &str) -> String {
        match self {
            Self::Posix => format!("gunzip -k {}", self.quote(path)),
            Self::Windows => format!("python -m gzip -d {}", self.quote(path)),
        }
    }

    /// Decompress the zstd file at `path` beside it. Windows ships no zstd
    /// decoder, so this one needs `zstd` installed there.
    pub(crate) fn unzstd(self, path: &str) -> String {
        format!("zstd -d {}", self.quote(path))
    }

    /// Extract the zip archive at `path` into the current directory.
    pub(crate) fn unzip(self, path: &str) -> String {
        match self {
            Self::Posix => format!("unzip {}", self.quote(path)),
            // Not `tar -xf`: with Git for Windows on the PATH, `tar` is GNU
            // tar, which reads `C:` as a remote host.
            Self::Windows => format!("python -m zipfile -e {} .", self.quote(path)),
        }
    }

    /// Re-encode the file at `path` from `encoding` to UTF-8, into
    /// `<path>.utf8.csv`.
    pub(crate) fn reencode(self, path: &str, encoding: &str) -> String {
        // libiconv, which macOS and Git for Windows use, does not know
        // `latin-1`; every iconv and Python know `ISO-8859-1`.
        let encoding = match encoding {
            "latin-1" => "ISO-8859-1",
            other => other,
        };
        let output = format!("{path}.utf8.csv");
        match self {
            Self::Posix => format!(
                "iconv -f {encoding} -t UTF-8 {} > {}",
                self.quote(path),
                self.quote(&output)
            ),
            // Without `newline=''` Python would rewrite line endings, and a
            // line break inside a quoted field is data.
            Self::Windows => format!(
                "python -c \"import sys;p=sys.argv[1];open(p+'.utf8.csv','w',encoding='utf-8',newline='').write(open(p,encoding='{encoding}',newline='').read())\" {}",
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
            r#"python -m zipfile -e "C:\a b\o'b.zip" ."#
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
