"""Orchestration for WAV muting requests.

Builds run on a bounded thread pool (the work is subprocess/IO-bound), one
request per task, with the DB as the source of truth for request state. A build
is published only after the engine has fully written and fsynced a temporary
file, which is then atomically renamed into place; every failure path removes
the partial file and flips the request to ``failed`` without ever exposing a
downloadable artifact.
"""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from . import mute

log = logging.getLogger("mute-mgr")


class MuteManager:
    def __init__(self, db, data_dir: str, max_workers: int = 2):
        self.db = db
        self.data_dir = data_dir
        self._pool = ThreadPoolExecutor(max_workers=max_workers,
                                        thread_name_prefix="mute")
        self.ffmpeg = None  # resolved lazily so import time never hard-fails

    # ---- paths ----

    def out_dir(self, job_id: int) -> str:
        return os.path.join(self.data_dir, "muted", str(job_id))

    def output_path(self, job_id: int, mute_id: int) -> str:
        return os.path.join(self.out_dir(job_id), f"muted_{mute_id}.wav")

    def tmp_path(self, job_id: int, mute_id: int) -> str:
        return os.path.join(self.out_dir(job_id), f"muted_{mute_id}.wav.part")

    def recover(self):
        """Startup reconciliation.

        A request left in ``processing`` means the worker died mid-build: mark
        it failed and sweep its partial file so no half-built WAV can ever be
        downloaded. Finished artifacts on disk are left untouched.
        """
        os.makedirs(os.path.join(self.data_dir, "muted"), exist_ok=True)
        for row in self.db.processing_mute_jobs():
            log.warning("mute request %s interrupted by restart, marking failed", row["id"])
            self.db.mute_job_failed(row["id"], "服务在处理期间重启，本次消音未完成")
        for root, _dirs, files in os.walk(os.path.join(self.data_dir, "muted")):
            for name in files:
                if name.endswith(".part"):
                    try:
                        os.remove(os.path.join(root, name))
                    except OSError:
                        pass

    # ---- submission / execution ----

    def submit(self, job_id: int, intervals: list) -> int:
        mute_id = self.db.insert_mute_job(job_id, json.dumps(intervals))
        self._pool.submit(self._run, mute_id, job_id, list(intervals))
        return mute_id

    def _run(self, mute_id: int, job_id: int, intervals: list):
        tmp = self.tmp_path(job_id, mute_id)
        final = self.output_path(job_id, mute_id)
        try:
            if self.ffmpeg is None:
                self.ffmpeg = mute.find_ffmpeg()
            job = self.db.get_job(job_id)
            if job is None or job["state"] != "done" or not job["zip_path"]:
                raise mute.MuteBuildError("源录音未处于完成状态，无法消音")
            stats = mute.build_muted_wav(job["zip_path"], intervals, tmp, self.ffmpeg)
            os.replace(tmp, final)  # atomic publication
            self.db.mute_job_ready(
                mute_id, final, stats["total_samples"],
                stats["muted_samples"], stats["warning"])
            log.info("mute request %s ready: %s", mute_id, stats)
        except mute.MuteError as exc:
            self._fail(mute_id, tmp, str(exc))
        except Exception as exc:  # never leave a task stuck in processing
            log.exception("mute request %s crashed", mute_id)
            self._fail(mute_id, tmp, f"内部错误: {exc}")

    def _fail(self, mute_id: int, tmp: str, message: str):
        log.error("mute request %s failed: %s", mute_id, message)
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        self.db.mute_job_failed(mute_id, message)

    def shutdown(self):
        self._pool.shutdown(wait=False, cancel_futures=True)
