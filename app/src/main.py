import json
import logging
import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field

from .db import DB
from .mute import MuteInputError, validate_intervals
from .mute_manager import MuteManager
from .recorder import Manager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

DATA_DIR = os.environ.get("DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)

db = DB(os.path.join(DATA_DIR, "recorder.db"))
mgr = Manager(db, DATA_DIR)
mute_mgr = MuteManager(db, DATA_DIR)
app = FastAPI(title="HLS 直播连续录音采集")


@app.on_event("startup")
async def _startup():
    mgr.recover()  # resume any recording/finalizing jobs after a restart
    mute_mgr.recover()  # fail any mute request interrupted mid-build


@app.on_event("shutdown")
async def _shutdown():
    await mgr.shutdown()
    mute_mgr.shutdown()


class StartBody(BaseModel):
    url: str


class IntervalBody(BaseModel):
    start: float = Field(..., description="遮蔽起点（按录音起点计的秒数）")
    end: float = Field(..., description="遮蔽终点（按录音起点计的秒数）")


class MuteBody(BaseModel):
    intervals: list[IntervalBody]


def job_json(job_id: int) -> dict:
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    counts = db.counts(job_id)
    out = {
        "id": job["id"],
        "url": job["url"],
        "state": job["state"],
        "created_at": job["created_at"],
        "first_seq": job["first_seq"],
        "last_seq": job["last_seq"],
        "stop_seq": job["stop_seq"],
        "expected": counts["total"],
        "saved": counts["done"],
        "missing": db.missing_seqs(job_id),
        "error": job["error"],
        "download_url": f"/api/jobs/{job_id}/download" if job["state"] == "done" else None,
    }
    return out


@app.post("/api/jobs")
async def start_job(body: StartBody):
    url = body.url.strip()
    if not url.startswith(("http://", "https://")):
        raise HTTPException(400, "只支持 http(s) 清单地址")
    job_id = mgr.create_job(url)
    return job_json(job_id)


@app.post("/api/jobs/{job_id}/stop")
async def stop_job(job_id: int):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    if job["state"] != "recording":
        raise HTTPException(409, f"当前状态 {job['state']} 不可停止")
    mgr.accept_stop(job_id)
    return job_json(job_id)


@app.get("/api/jobs")
async def list_jobs():
    return [job_json(j["id"]) for j in db.list_jobs()]


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: int):
    return job_json(job_id)


@app.get("/api/jobs/{job_id}/download")
async def download(job_id: int):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    if job["state"] != "done" or not job["zip_path"] or not os.path.exists(job["zip_path"]):
        raise HTTPException(409, "全部应收片段保存后才可下载")
    return FileResponse(job["zip_path"], media_type="application/zip",
                        filename=f"recording_{job_id}.zip")


def mute_json(row: dict) -> dict:
    try:
        intervals = json.loads(row["intervals"])
    except (TypeError, ValueError):
        intervals = []
    out = {
        "id": row["id"],
        "job_id": row["job_id"],
        "state": row["state"],
        "intervals": intervals,
        "total_samples": row["total_samples"],
        "muted_samples": row["muted_samples"],
        "warning": row["warning"],
        "error": row["error"],
        "download_url": (f"/api/mutes/{row['id']}/download"
                         if row["state"] == "ready" and row["output_path"] else None),
    }
    return out


@app.post("/api/jobs/{job_id}/mute", status_code=201)
async def create_mute(job_id: int, body: MuteBody):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    if job["state"] != "done" or not job["zip_path"] or not os.path.exists(job["zip_path"]):
        raise HTTPException(409, "仅完成录音可消音，请先停止并等待打包完成")
    try:
        intervals = validate_intervals([i.model_dump() for i in body.intervals])
    except MuteInputError as exc:
        raise HTTPException(400, str(exc))
    mute_id = mute_mgr.submit(job_id, intervals)
    return mute_json(db.get_mute_job(mute_id))


@app.get("/api/jobs/{job_id}/mutes")
async def list_mutes(job_id: int):
    if db.get_job(job_id) is None:
        raise HTTPException(404, "job not found")
    return [mute_json(r) for r in db.list_mute_jobs(job_id)]


@app.get("/api/mutes/{mute_id}")
async def get_mute(mute_id: int):
    row = db.get_mute_job(mute_id)
    if row is None:
        raise HTTPException(404, "mute request not found")
    return mute_json(row)


@app.get("/api/mutes/{mute_id}/download")
async def download_mute(mute_id: int):
    row = db.get_mute_job(mute_id)
    if row is None:
        raise HTTPException(404, "mute request not found")
    if row["state"] != "ready" or not row["output_path"] or not os.path.exists(row["output_path"]):
        raise HTTPException(409, "消音尚未完成或已失败，无文件可下载")
    return FileResponse(row["output_path"], media_type="audio/wav",
                        filename=f"recording_{row['job_id']}_muted_{mute_id}.wav")


@app.get("/")
async def index():
    page = os.path.join(os.path.dirname(__file__), "static", "index.html")
    with open(page, encoding="utf-8") as fh:
        return HTMLResponse(fh.read())


# Test-only hooks for crash/recovery/tamper testing. Enabled exclusively via
# RECORDER_TEST_HOOKS=1; never set in the Compose deployment.
if os.environ.get("RECORDER_TEST_HOOKS") == "1":
    import shutil

    from . import mute as mute_mod
    from .recorder import TEST_HOOKS

    @app.post("/api/_test/crash")
    async def _test_crash():
        os._exit(137)

    @app.post("/api/_test/arm_after_write/{job_id}/{seq}")
    async def _test_arm_after_write(job_id: int, seq: int):
        TEST_HOOKS["crash_after_write"] = (job_id, seq)
        return {"armed": [job_id, seq]}

    @app.post("/api/_test/arm_mute_tamper/{job_id}")
    async def _test_arm_mute_tamper(job_id: int):
        # The next muted-WAV build for this job will see its source ZIP
        # truncated mid-build and must fail without publishing audio.
        mute_mod.TEST_HOOKS["tamper_job_id"] = job_id
        return {"armed": job_id}

    @app.post("/api/_test/restore_zip/{job_id}")
    async def _test_restore_zip(job_id: int):
        # Undo the tamper: restore the backed-up ZIP so its normal download
        # keeps working exactly as before the failed mute attempt.
        job = db.get_job(job_id)
        if job is None or not job["zip_path"]:
            raise HTTPException(404, "job/zip not found")
        backup = job["zip_path"] + ".tamper.bak"
        if not os.path.exists(backup):
            raise HTTPException(404, "no tamper backup found")
        shutil.move(backup, job["zip_path"])
        return {"restored": job["zip_path"]}
