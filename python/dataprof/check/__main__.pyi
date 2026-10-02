def main(argv: list[str] | None = ...) -> int:
    """Run a gate; return 0 for pass, 1 for fail, 2 when inconclusive, or 3 on error.

    Raises:
        SystemExit: Argument parsing exits with 0 for ``--help``, or 3 for
            invalid arguments or the unsupported ``--baseline`` option.
    """
    ...
