#!/bin/bash
# Smoke-test each x86_64 Linux baseline wheel on an emulated CPU (#1077).
#
# Runs inside the pinned manylinux image the wheels were built in, with the
# runner's static qemu-x86_64 mounted. The interpreters come from the image's
# /opt/python: they need only the x86-64 baseline and sit on the oldest glibc
# the wheels support. setup-python's interpreters cannot be used: its
# ubuntu-26.04 builds require x86-64-v3, so they stop on any CPU without AVX2.
#
# Environment: VERSIONS (one minor version per line), EMULATED_CPU (a qemu CPU
# model). Mounts: /dist (the wheels), /scripts (this directory), /out (the
# step summary rows are written to /out/summary.md).
set -u

qemu=/usr/local/bin/qemu-x86_64
cp /scripts/wheel_smoke.py /tmp/wheel_smoke.py
summary=/out/summary.md
{
  echo "| Interpreter | Wheel | Interpreter on $EMULATED_CPU | Smoke test on $EMULATED_CPU |"
  echo "|---|---|---|---|"
} > "$summary"

failed=0
for version in $VERSIONS; do
  tag="cp${version/./}"
  python="/opt/python/${tag}-${tag}/bin/python"
  wheel=$(find /dist -maxdepth 1 -name "dataprof-*-${tag}-${tag}-*.whl" | head -n 1)
  built="missing"
  interpreter="not run"
  smoke="not run"
  if [ ! -x "$python" ]; then
    interpreter="not in the image"
  elif [ -n "$wheel" ]; then
    built="$(basename "$wheel")"
    echo "::group::CPython $version on an emulated $EMULATED_CPU CPU: $built"
    # The interpreter alone first, so a failure says which side lacks the CPU.
    if "$qemu" -cpu "$EMULATED_CPU" "$python" -S -c pass; then
      interpreter="runs"
      venv="/tmp/venv-$tag"
      "$python" -m venv "$venv"
      "$venv/bin/python" -m pip install -q --no-index "$wheel"
      if (cd /tmp && "$qemu" -cpu "$EMULATED_CPU" "$venv/bin/python" wheel_smoke.py); then
        smoke="passed"
      else
        # SIGILL (exit 132) means an instruction this CPU lacks.
        smoke="failed (exit $?)"
      fi
    else
      interpreter="failed (exit $?)"
    fi
    echo "::endgroup::"
  fi
  echo "| $version | $built | $interpreter | $smoke |" >> "$summary"
  if [ "$smoke" != "passed" ]; then
    echo "::error::CPython $version: wheel $built on an emulated $EMULATED_CPU CPU: interpreter $interpreter, smoke test $smoke"
    failed=1
  fi
done
exit "$failed"
