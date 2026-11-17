"""Stable fault-injection points for crash and restart experiments.

Faults are controlled externally through marker files so the test harness
never has to reach into engine internals::

    <store>/faults/<POINT>.fault   -> JSON {"mode": "exit", "code": 99}

When the engine reaches ``POINT`` with an armed marker it performs a
hard process exit via ``os._exit`` - no Python ``finally`` blocks, no clean
close - which is the supported crash model.  Each marker is consumed at
most once (renamed to ``.fired``) unless it is marked ``repeat``, so a
recovery can be crashed at exactly one phase and the next open completes.

Stable point names (part of the public test contract):

* ``commit_before_decision_fsync`` - log decision framed, not yet synced
* ``commit_after_decision_fsync``  - durable decision, reply not sent
* ``abort_before_decision_fsync``
* ``checkpoint_before_publish``    - complete snapshot built, not published
* ``checkpoint_during_switch``     - staged file promoted, current not switched
* ``recovery_after_record_scan``   - WAL analyzed, no pages touched
* ``recovery_mid_cleanup``         - some transactions resolved, some not
* ``recovery_mid_page_write``      - some affected pages rewritten
"""

import json
import os

POINTS = (
    "commit_before_decision_fsync",
    "commit_after_decision_fsync",
    "abort_before_decision_fsync",
    "checkpoint_before_publish",
    "checkpoint_during_switch",
    "recovery_after_record_scan",
    "recovery_mid_cleanup",
    "recovery_mid_page_write",
)
DEFAULT_CODE = 99


class FaultInjector:
    def __init__(self, faults_dir: str, enabled: bool = True):
        self.dir = faults_dir
        self.enabled = enabled

    def arm(self, point: str, code: int = DEFAULT_CODE, repeat: bool = False):
        if point not in POINTS:
            raise KeyError(f"unknown fault point {point}")
        os.makedirs(self.dir, exist_ok=True)
        path = os.path.join(self.dir, point + ".fault")
        with open(path, "w") as f:
            json.dump({"mode": "exit", "code": code, "repeat": repeat}, f)
            f.flush()
            os.fsync(f.fileno())
        _fsync_dir(self.dir)

    def disarm(self, point: str):
        path = os.path.join(self.dir, point + ".fault")
        if os.path.exists(path):
            os.remove(path)
            _fsync_dir(self.dir)

    @staticmethod
    def armed_points(store_dir: str):
        d = os.path.join(store_dir, "faults")
        if not os.path.isdir(d):
            return []
        return sorted(n[:-len(".fault")] for n in os.listdir(d)
                      if n.endswith(".fault"))

    def reach(self, point: str):
        """Called by the engine at a stable point; hard-exits when armed."""
        if not self.enabled:
            return
        path = os.path.join(self.dir, point + ".fault")
        if not os.path.exists(path):
            return
        with open(path) as f:
            try:
                cfg = json.load(f)
            except json.JSONDecodeError:
                cfg = {}
        code = int(cfg.get("code", DEFAULT_CODE))
        if not cfg.get("repeat"):
            try:
                os.replace(path, path + ".fired")
                _fsync_dir(self.dir)
            except OSError:
                pass
        # Hard kill: bypass buffers/finally, emulating process crash.
        os._exit(code)


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
