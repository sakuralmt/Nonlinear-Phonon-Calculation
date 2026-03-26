#!/usr/bin/env python
"""Compatibility launcher for the remote-stable stage1-only QE bundle.

Some clusters still have `python` pointing to Python 2 or an older Python 3.
This launcher contains NO modern syntax so that `python run_all.py` works.
It will re-exec a suitable Python 3 interpreter to run `run_all_impl.py`.

Usage (server):
  python run_all.py
"""

from __future__ import print_function

import os
import subprocess
import sys


def _find_python3():
    candidates = [
        os.environ.get("PYTHON", ""),
        "python3",
        "python3.12",
        "python3.11",
        "python3.10",
        "python3.9",
        "python3.8",
        "python3.7",
    ]
    for exe in candidates:
        if not exe:
            continue
        try:
            out = subprocess.check_output([exe, "-c", "import sys; print(sys.version_info[0])"], stderr=subprocess.STDOUT)
            major = int(out.decode("utf-8", "ignore").strip() or "0")
            if major >= 3:
                return exe
        except Exception:
            pass
    return None


def main():
    exe = _find_python3()
    if exe is None:
        sys.stderr.write(
            "ERROR: cannot find a usable Python 3 interpreter.\n"
            "Tried: python3, python3.12, python3.11, ...\n"
            "Fix: load a python3 module or set env PYTHON=/path/to/python3\n"
        )
        return 2

    here = os.path.dirname(os.path.abspath(__file__))
    impl = os.path.join(here, "run_all_impl.py")

    # Preserve argv: run_all.py takes no args, but keep pass-through for future.
    argv = [exe, impl] + sys.argv[1:]

    # Replace current process.
    os.execvp(exe, argv)


if __name__ == "__main__":
    raise SystemExit(main())
