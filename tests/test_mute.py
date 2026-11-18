#!/usr/bin/env python3
"""End-to-end tests for the WAV-muting feature.

Covers: request validation, streaming decode in playlist order, sample-exact
zeroing (including cross-segment ranges), untouched samples, failure on
missing/corrupt segments and on mid-build ZIP tampering (no published audio,
no .part leftovers, original ZIP still downloadable), concurrency, restart
recovery, bounded memory for long recordings, and WAV playability.
"""
import io
import math
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import tracemalloc
import wave
import zipfile

import httpx

SRC = os.environ.get("SRC", "http://127.0.0.1:8081")
PORT = int(os.environ.get("TEST_PORT", "0"))
if PORT == 0:
    import socket
    s = socket.socket(); s.bind(("127.0.0.1", 0)); PORT = s.getsockname()[1]; s.close()
APP = f"http://127.0.0.1:{PORT}"
DATA = f"/tmp/mutedata_{os.getpid()}"
os.makedirs(DATA, exist_ok=True)
FFMPEG = os.environ.get("FFMPEG_BIN") or subprocess.check_output(
    [sys.executable, "-c", "import imageio_ffmpeg; print(imageio_ffmpeg.get_ffmpeg_exe())"]
).decode().strip()

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL "), name, ("" if cond else f"  -> {detail}"))


def api(method, path, **kw):
    return getattr(httpx, method)(f"{APP}{path}", timeout=30, **kw)


def wait_state(mute_id, states, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = api("get", f"/api/mutes/{mute_id}")
        row = r.json()
        if row["state"] in states:
            return row
        time.sleep(0.3)
    return api("get", f"/api/mutes/{mute_id}").json()


def wait_job(jid, state, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        j = api("get", f"/api/jobs/{jid}").json()
        if j["state"] == state:
            return j
        time.sleep(0.4)
    return api("get", f"/api/jobs/{jid}").json()


app_proc = None


def port_in_use():
    try:
        api("get", "/api/jobs")
        return True
    except Exception:
        return False


def start_app():
    global app_proc
    assert not port_in_use(), f"port {PORT} still in use before start"
    log = open(os.environ.get("APP_LOG", "/tmp/app_mute.log"), "ab")
    app_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "src.main:app",
         "--host", "127.0.0.1", "--port", str(PORT)],
        cwd=os.environ.get("APP_DIR", "/workspace/app"),
        env={**os.environ, "DATA_DIR": DATA, "RECORDER_TEST_HOOKS": "1",
             "FFMPEG_BIN": FFMPEG},
        stdout=log, stderr=subprocess.STDOUT)
    for _ in range(60):
        if app_proc.poll() is not None:
            raise RuntimeError("app process exited during startup")
        try:
            api("get", "/api/jobs")
            return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError("app did not start")


def crash_app():
    global app_proc
    try:
        api("post", "/api/_test/crash")
    except Exception:
        pass
    if app_proc:
        app_proc.wait(timeout=10)
    app_proc = None
    for _ in range(40):
        if not port_in_use():
            return
        time.sleep(0.25)
    raise RuntimeError("port still in use after crash")


def record_vod():
    r = api("post", "/api/jobs", json={"url": f"{SRC}/vod.m3u8"})
    assert r.status_code == 200, r.text
    jid = r.json()["id"]
    j = wait_job(jid, "done", 30)
    assert j["state"] == "done", j
    return jid


def fetch_zip_bytes(jid):
    r = api("get", f"/api/jobs/{jid}/download")
    assert r.status_code == 200
    return r.content


def muted_dir():
    return os.path.join(DATA, "muted")


def list_parts():
    out = []
    for root, _d, files in os.walk(muted_dir()):
        out += [os.path.join(root, f) for f in files if f.endswith(".part")]
    return out


def read_wav(content):
    wf = wave.open(io.BytesIO(content), "rb")
    pcm = wf.readframes(wf.getnframes())
    info = (wf.getnchannels(), wf.getsampwidth(), wf.getframerate(), wf.getnframes())
    wf.close()
    return info, pcm


def decode_ts_to_pcm(ts_bytes):
    p = subprocess.run(
        [FFMPEG, "-loglevel", "error", "-f", "mpegts", "-i", "pipe:0",
         "-vn", "-ac", "1", "-ar", "44100", "-f", "s16le", "-acodec", "pcm_s16le", "pipe:1"],
        input=ts_bytes, capture_output=True)
    assert p.returncode == 0, p.stderr[:300]
    return p.stdout


try:
    start_app()

    # ---------- M0: guards and request validation ----------
    r = api("post", "/api/jobs", json={"url": f"{SRC}/live.m3u8"})
    rec_id = r.json()["id"]
    r = api("post", f"/api/jobs/{rec_id}/mute", json={"intervals": [{"start": 1, "end": 2}]})
    check("M0 未完成录音拒绝消音(409)", r.status_code == 409, str(r.status_code))
    api("post", f"/api/jobs/{rec_id}/stop")
    wait_job(rec_id, "done", 60)

    jid = record_vod()
    for bad, kw in [
        ({"intervals": []}, "空区间"),
        ({"intervals": [{"start": 3, "end": 2}]}, "终点<=起点"),
        ({"intervals": [{"start": -1, "end": 2}]}, "负起点"),
        ({"intervals": [{"start": "a", "end": 2}]}, "非数字"),
    ]:
        r = api("post", f"/api/jobs/{jid}/mute", json=bad)
        check(f"M0 非法区间被拒(4xx) {kw}", r.status_code in (400, 422), r.text)
    r = api("post", "/api/jobs/99999/mute", json={"intervals": [{"start": 1, "end": 2}]})
    check("M0 不存在任务 404", r.status_code == 404, str(r.status_code))

    # Reference: decode every TS of the package in playlist order.
    pkg = zipfile.ZipFile(io.BytesIO(fetch_zip_bytes(jid)))
    names = [ln.strip() for ln in pkg.read("playlist.m3u8").decode().splitlines()
             if ln.strip() and not ln.startswith("#")]
    ref_pcm = b"".join(decode_ts_to_pcm(pkg.read(n)) for n in names)
    total_samples = len(ref_pcm) // 2
    per_seg = total_samples // len(names)
    print(f"  package: {len(names)} segs, {total_samples} samples, "
          f"{total_samples/44100:.3f}s, {per_seg}/seg")

    # ---------- M1: basic single-range mute ----------
    r = api("post", f"/api/jobs/{jid}/mute", json={"intervals": [{"start": 1.0, "end": 2.0}]})
    check("M1 消音请求已受理(201)", r.status_code == 201, r.text)
    mid = r.json()["id"]
    row = wait_state(mid, {"ready", "failed"})
    check("M1 构建成功", row["state"] == "ready", str(row))
    check("M1 置零样本数正确(1秒=44100)", row["muted_samples"] == 44100, str(row["muted_samples"]))
    check("M1 总样本数=各片段累计", row["total_samples"] == total_samples, str(row))
    wav_bytes = api("get", f"/api/mutes/{mid}/download").content
    info, pcm = read_wav(wav_bytes)
    check("M1 WAV格式 44.1k/mono/s16", info[:3] == (1, 2, 44100), str(info))
    check("M1 区间内全部置零", pcm[2 * 44100:2 * 88200] == b"\x00" * (2 * 44100))
    check("M1 区间外样本保持原值(前段)", pcm[:2 * 44100] == ref_pcm[:2 * 44100])
    check("M1 区间外样本保持原值(后段)", pcm[2 * 88200:] == ref_pcm[2 * 88200:])
    check("M1 无半成品残留", not list_parts())

    # ---------- M2: cross-segment range, extends past end ----------
    r = api("post", f"/api/jobs/{jid}/mute",
            json={"intervals": [{"start": 3.0, "end": 7.0}]})
    mid2 = r.json()["id"]
    row = wait_state(mid2, {"ready", "failed"})
    check("M2 跨片段+超尾区间构建成功", row["state"] == "ready", str(row))
    check("M2 超尾给出提示", row["warning"] and "超出" in row["warning"], str(row.get("warning")))
    _, pcm2 = read_wav(api("get", f"/api/mutes/{mid2}/download").content)
    cross = 3 * 44100
    check("M2 跨片段处连续置零到结尾",
          pcm2[2 * cross:] == b"\x00" * (len(pcm2) - 2 * cross))
    check("M2 遮蔽前样本保持原值", pcm2[:2 * cross] == ref_pcm[:2 * cross])
    check("M2 置零数=总长-起点", row["muted_samples"] == total_samples - cross, str(row))
    # segment boundary itself must be silence too (range starts before seg 2 end? no:
    # 3.0s lies inside segment 2; boundary at per_seg samples is fully covered)
    b = per_seg
    check("M2 片段边界两侧均为零",
          pcm2[2 * (b - 4):2 * b] == b"\x00" * 8 and pcm2[2 * b:2 * (b + 4)] == b"\x00" * 8)

    # ---------- M3: multiple overlapping ranges merge; non-muted bytes identical ----------
    r = api("post", f"/api/jobs/{jid}/mute",
            json={"intervals": [{"start": 0.5, "end": 1.5}, {"start": 1.2, "end": 2.5},
                                 {"start": 4.0, "end": 4.25}]})
    mid3 = r.json()["id"]
    row = wait_state(mid3, {"ready", "failed"})
    check("M3 多区间构建成功", row["state"] == "ready", str(row))
    _, pcm3 = read_wav(api("get", f"/api/mutes/{mid3}/download").content)
    # Half-open sample ranges matching the engine (floor on start, ceil on end);
    # merged 0.5-2.5 plus 4.0-4.25.
    expect_zero = (set(range(int(0.5 * 44100), math.ceil(2.5 * 44100)))
                   | set(range(int(4.0 * 44100), math.ceil(4.25 * 44100))))
    # Source PCM may contain natural zeros (sine onset, codec padding), so an
    # un-muted zero is fine: expected-muted samples must be zero, and every
    # other sample must be byte-identical to the source.
    not_zero_inside = [i for i in expect_zero
                       if pcm3[2 * i:2 * i + 2] != b"\x00\x00"]
    check("M3 合并区间内逐样本置零", not not_zero_inside, f"first={not_zero_inside[:5]}")
    check("M3 区间外样本与源PCM完全一致(含天然零样本)",
          all(pcm3[2 * i:2 * i + 2] == ref_pcm[2 * i:2 * i + 2]
              for i in range(total_samples) if i not in expect_zero))

    # ---------- M4: every interval beyond the recording ----------
    r = api("post", f"/api/jobs/{jid}/mute",
            json={"intervals": [{"start": 99, "end": 100}]})
    mid4 = r.json()["id"]
    row = wait_state(mid4, {"ready", "failed"})
    check("M4 全部区间超出时长判失败", row["state"] == "failed", str(row))
    r = api("get", f"/api/mutes/{mid4}/download")
    check("M4 失败请求不可下载(409)", r.status_code == 409, str(r.status_code))
    check("M4 无半成品残留", not list_parts())

    # ---------- M5: source package missing a segment -> fail, ZIP download intact ----------
    zip_path = os.path.join(DATA, "zips", f"job_{jid}.zip")
    zip_backup = zip_path + ".testbak"
    shutil.copy2(zip_path, zip_backup)
    try:
        with zipfile.ZipFile(zip_path) as zin:
            items = {n: zin.read(n) for n in zin.namelist()}
        os.remove(zip_path)
        dropped = names[-1]
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zout:
            for n, data in items.items():
                if n != dropped:
                    zout.writestr(n, data)  # playlist still references the dropped TS
        r = api("post", f"/api/jobs/{jid}/mute",
                json={"intervals": [{"start": 0.1, "end": 0.5}]})
        mid5 = r.json()["id"]
        row = wait_state(mid5, {"ready", "failed"})
        check("M5 源包缺段判失败且提示缺段",
              row["state"] == "failed" and "缺段" in (row["error"] or ""), str(row))
        check("M5 无 WAV 发布/无半成品",
              not list_parts() and api("get", f"/api/mutes/{mid5}/download").status_code == 409)
    finally:
        shutil.move(zip_backup, zip_path)
    r = api("get", f"/api/jobs/{jid}/download")
    zcheck = zipfile.ZipFile(io.BytesIO(r.content))
    check("M5 失败后原 ZIP 下载照常可用且结构完整",
          r.status_code == 200 and zcheck.testzip() is None
          and sorted(zcheck.namelist()) == sorted(names + ["playlist.m3u8"]))

    # ---------- M6: corrupt TS -> decode failure, nothing published ----------
    shutil.copy2(zip_path, zip_backup)
    try:
        with zipfile.ZipFile(zip_path) as zin:
            items = {n: zin.read(n) for n in zin.namelist()}
        os.remove(zip_path)
        items[names[1]] = b"not an MPEG-TS stream" * 50
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zout:
            for n, data in items.items():
                zout.writestr(n, data)
        r = api("post", f"/api/jobs/{jid}/mute",
                json={"intervals": [{"start": 0.1, "end": 0.5}]})
        mid6 = r.json()["id"]
        row = wait_state(mid6, {"ready", "failed"})
        check("M6 解码失败判失败", row["state"] == "failed" and "解码" in (row["error"] or ""),
              str(row))
        check("M6 无 WAV 发布/无半成品",
              not list_parts() and api("get", f"/api/mutes/{mid6}/download").status_code == 409)
    finally:
        shutil.move(zip_backup, zip_path)
    check("M6 恢复后原 ZIP 仍可下载", api("get", f"/api/jobs/{jid}/download").status_code == 200)

    # ---------- M7: ZIP rewritten mid-build -> fail, no artifact, restore works ----------
    api("post", f"/api/_test/arm_mute_tamper/{jid}")
    r = api("post", f"/api/jobs/{jid}/mute",
            json={"intervals": [{"start": 0.2, "end": 1.0}]})
    mid7 = r.json()["id"]
    row = wait_state(mid7, {"ready", "failed"}, 90)
    check("M7 处理期间 ZIP 被改写判失败",
          row["state"] == "failed" and "改写" in (row["error"] or ""), str(row))
    check("M7 无半成品/不可下载",
          not list_parts() and api("get", f"/api/mutes/{mid7}/download").status_code == 409)
    r = api("post", f"/api/_test/restore_zip/{jid}")
    check("M7 测试钩子恢复 ZIP", r.status_code == 200, r.text)
    r = api("get", f"/api/jobs/{jid}/download")
    check("M7 原 ZIP 下载照常可用", r.status_code == 200
          and zipfile.ZipFile(io.BytesIO(r.content)).testzip() is None)

    # ---------- M8: concurrent mute requests both succeed, independent WAVs ----------
    ids = []
    for spec in [{"start": 0.2, "end": 0.8}, {"start": 2.0, "end": 2.6}]:
        ids.append(api("post", f"/api/jobs/{jid}/mute",
                       json={"intervals": [spec]}).json()["id"])
    rows = [wait_state(m, {"ready", "failed"}) for m in ids]
    check("M8 两个并发消音均成功", all(r["state"] == "ready" for r in rows), str(rows))
    wavs = [api("get", f"/api/mutes/{m}/download").content for m in ids]
    check("M8 输出为两个不同 WAV", wavs[0] != wavs[1] and len(wavs[0]) == len(wavs[1]))
    check("M8 任务列表接口可见全部请求",
          [m["id"] for m in api("get", f"/api/jobs/{jid}/mutes").json()
           if m["id"] in ids] == ids)

    # ---------- M9: restart while a request is stuck 'processing' -> failed, sweep ----------
    conn = sqlite3.connect(os.path.join(DATA, "recorder.db"))
    now = time.time()
    cur = conn.execute(
        "INSERT INTO mute_jobs(job_id, state, intervals, created_at, updated_at)"
        " VALUES (?, 'processing', ?, ?, ?)",
        (jid, '[{"start": 1, "end": 2}]', now, now))
    orphan_id = cur.lastrowid
    conn.commit(); conn.close()
    os.makedirs(os.path.join(muted_dir(), str(jid)), exist_ok=True)
    open(os.path.join(muted_dir(), str(jid), f"muted_{orphan_id}.wav.part"), "wb").write(b"x" * 99)
    crash_app()
    start_app()
    row = wait_state(orphan_id, {"failed", "ready"})
    check("M9 重启后中断的消音判失败", row["state"] == "failed", str(row))
    check("M9 半成品被清理", not list_parts())

    # ---------- M10: long recording stays bounded in memory ----------
    sys.path.insert(0, os.environ.get("APP_DIR", "/workspace/app"))
    from src.mute import build_muted_wav  # noqa: E402
    big_dir = f"/tmp/bigpkg_{os.getpid()}"
    os.makedirs(big_dir, exist_ok=True)
    big_zip = os.path.join(big_dir, "big.zip")
    seed_ts = os.environ.get("SEED_TS", "/tmp/livesrc/seed.ts")
    n_big = 40
    with zipfile.ZipFile(big_zip, "w", zipfile.ZIP_STORED) as zf:
        pl = ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:2",
              "#EXT-X-MEDIA-SEQUENCE:1"]
        for i in range(1, n_big + 1):
            name = f"seg_{i:08d}.ts"
            zf.write(seed_ts, name)
            pl += [f"#EXTINF:2.000,", name]
        pl.append("#EXT-X-ENDLIST")
        zf.writestr("playlist.m3u8", "\n".join(pl) + "\n")
    tmp_wav = os.path.join(big_dir, "out.wav.part")
    tracemalloc.start()
    stats = build_muted_wav(big_zip, [(5.0, 8.0), (30.0, 33.0)], tmp_wav, FFMPEG)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    wav_size = os.path.getsize(tmp_wav)
    check("M10 长录音流式构建成功",
          stats["total_samples"] == n_big * per_seg and wav_size > 44 * 1024 * 1024 // 10,
          f"{stats} wav={wav_size}")
    check("M10 Python 侧峰值内存有界(<4MB, 输出>7MB)",
          peak < 4 * 1024 * 1024, f"peak={peak/1024/1024:.2f}MB")
    os.remove(tmp_wav)

    # ---------- M11: produced WAV is playable by ffmpeg ----------
    good = api("get", f"/api/mutes/{mid}/download").content
    dec = subprocess.run([FFMPEG, "-v", "error", "-i", "pipe:0", "-f", "null", "-"],
                         input=good, capture_output=True)
    check("M11 消音 WAV 可被 ffmpeg 完整解码",
          dec.returncode == 0 and not dec.stderr.strip(), dec.stderr[:200])

    # ---------- M12: original ZIP download remains fully functional throughout ----------
    r = api("get", f"/api/jobs/{jid}/download")
    final_zip = zipfile.ZipFile(io.BytesIO(r.content))
    check("M12 原 ZIP 始终可下载且内容完整",
          r.status_code == 200 and final_zip.testzip() is None
          and sorted(final_zip.namelist()) == sorted(names + ["playlist.m3u8"]))

finally:
    try:
        crash_app()
    except Exception:
        pass

print("\n==== SUMMARY ====")
failed = [n for n, ok in results if not ok]
print(f"{len(results) - len(failed)}/{len(results)} passed")
if failed:
    print("FAILED:", *failed, sep="\n  - ")
    sys.exit(1)
print("ALL PASS")
