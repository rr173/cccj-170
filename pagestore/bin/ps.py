#!/usr/bin/env python3
"""Stable entry point for process-level experiments.

Runs in isolated interpreter mode semantics (``-I`` style: no user site
packages, no environment path overrides) while making the in-repo
``pagestore`` package importable by absolute path, so planted modules in
untrusted working directories can't shadow the stdlib or this package.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pagestore.cli import main  # noqa: E402

if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception as exc:  # pragma: no cover - CLI safety net
        import json
        print(json.dumps({"fatal": type(exc).__name__, "detail": str(exc)}),
              file=sys.stderr)
        sys.exit(1)
