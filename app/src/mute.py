"""Streaming WAV muting for completed recording packages.

The recorder publishes an immutable ZIP (local ``playlist.m3u8`` plus the raw
MPEG-TS segments). This module turns such a package into one continuous WAV in
which the requested second ranges (measured from the start of the recording)
are replaced with digital silence:

* segments are decoded strictly in the order given by the in-ZIP playlist;
* PCM sample positions are accumulated across segments, so a muted range can
  start in one segment and end in another;
* PCM flows through fixed-size chunks (ZIP -> ffmpeg -> WAV), so a recording of
  any length is processed with bounded memory;
* any failure -- a segment missing from the package, a decode error, or the
  source ZIP changing on disk mid-build -- aborts the request before anything
  is published. Only a fully built, fsynced file is atomically renamed into its
  downloadable location, and partial ``.part`` files are always removed.
"""

import logging
import math
import os
import shutil
import struct
import subprocess
import threading
import zipfile

log = logging.getLogger("mute")

# Every output WAV uses one fixed PCM format; ffmpeg resamples each segment to
# it, which also guarantees every segment shares the same parameters.
SAMPLE_RATE = 44100
CHANNELS = 1
SAMPLE_WIDTH = 2                       # signed 16-bit little-endian
BYTE_RATE = SAMPLE_RATE * CHANNELS * SAMPLE_WIDTH
BLOCK_ALIGN = CHANNELS * SAMPLE_WIDTH
PCM_CHUNK = 64 * 1024                  # bytes pulled from ffmpeg at a time
SEG_CHUNK = 64 * 1024                  # bytes pushed from the ZIP at a time
# Standard RIFF/WAVE headers use 32-bit size fields; refuse rather than overflow.
MAX_DATA_BYTES = 0xFFFFFFF0 - 36

# Test-only hook (active solely when RECORDER_TEST_HOOKS=1): while building the
# muted WAV for the armed job, rewrite its source ZIP after the first segment so
# the tamper-detection path can be exercised. Maps job_id -> backup path.
TEST_HOOKS = {"tamper_job_id": None}


class MuteError(Exception):
    """Base class for muting failures."""


class MuteInputError(MuteError):
    """The request itself is invalid (maps to HTTP 400)."""


class MuteBuildError(MuteError):
    """The WAV could not be built from the source package."""


def find_ffmpeg() -> str:
    exe = os.environ.get("FFMPEG_BIN") or shutil.which("ffmpeg")
    if not exe:
        raise MuteBuildError("容器内未找到 ffmpeg，无法解码 TS 音轨")
    return exe


def validate_intervals(raw) -> list:
    """Shape-check and merge the requested second ranges.

    Accepts a list of ``{"start": float, "end": float}`` dicts. Returns a
    sorted list of non-overlapping ``(start, end)`` tuples.
    """
    if not isinstance(raw, list) or not raw:
        raise MuteInputError("intervals 必须是非空数组，每项含 start/end 秒数")
    clean = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise MuteInputError(f"第 {i + 1} 个区间格式非法，须为 {{start, end}}")
        try:
            start = float(item["start"])
            end = float(item["end"])
        except (KeyError, TypeError, ValueError):
            raise MuteInputError(f"第 {i + 1} 个区间的 start/end 必须是数字")
        if start < 0:
            raise MuteInputError(f"第 {i + 1} 个区间起点不能为负: {start}")
        if end <= start:
            raise MuteInputError(f"第 {i + 1} 个区间终点必须大于起点: {start}..{end}")
        clean.append((start, end))
    clean.sort()
    merged = []
    for start, end in clean:
        if merged and start <= merged[-1][1]:
            if end > merged[-1][1]:
                merged[-1] = (merged[-1][0], end)
        else:
            merged.append((start, end))
    return merged


def parse_playlist_order(playlist_text: str) -> list:
    """Return the segment file names in playlist order."""
    names = []
    for line in playlist_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if not line.lower().endswith(".ts"):
            raise MuteBuildError(f"清单引用了非 TS 片段: {line}")
        names.append(line)
    if not names:
        raise MuteBuildError("清单内没有 TS 片段")
    if len(set(names)) != len(names):
        raise MuteBuildError("清单内片段重复，无法按顺序解码")
    return names


def _wav_header(data_bytes: int) -> bytes:
    return struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF", 36 + data_bytes, b"WAVE",
        b"fmt ", 16,
        1,                       # PCM format tag
        CHANNELS,
        SAMPLE_RATE,
        BYTE_RATE,
        BLOCK_ALIGN,
        SAMPLE_WIDTH * 8,
        b"data", data_bytes,
    )


class _ByteRanges:
    """Muted ranges in output-byte coordinates, kept sorted for linear scans."""

    def __init__(self, intervals: list):
        self.ranges = []
        for start_sec, end_sec in intervals:
            start_byte = int(start_sec * SAMPLE_RATE) * BLOCK_ALIGN  # floor
            end_byte = math.ceil(end_sec * SAMPLE_RATE) * BLOCK_ALIGN
            if self.ranges and start_byte <= self.ranges[-1][1]:
                # intervals were merged in seconds; guard the rounding seam too
                self.ranges[-1] = (self.ranges[-1][0], max(self.ranges[-1][1], end_byte))
            else:
                self.ranges.append((start_byte, end_byte))

    def zero_in(self, buf: bytearray, chunk_start: int) -> None:
        """Zero every muted byte inside buf (which starts at chunk_start)."""
        chunk_end = chunk_start + len(buf)
        for lo, hi in self.ranges:
            if hi <= chunk_start:
                continue
            if lo >= chunk_end:
                break
            a = max(lo, chunk_start) - chunk_start
            b = min(hi, chunk_end) - chunk_start
            buf[a:b] = b"\x00" * (b - a)

    def intersecting_bytes(self, total_bytes: int) -> int:
        total = 0
        for lo, hi in self.ranges:
            if lo >= total_bytes:
                break
            total += min(hi, total_bytes) - lo
        return total


def _decode_segment(ffmpeg: str, seg_reader, out_fh, ranges: _ByteRanges,
                    byte_cursor: int) -> int:
    """Decode one TS segment to PCM and stream it (with muting) into out_fh.

    ``seg_reader`` is a ZipFile.open() handle positioned at the segment. A
    feeder thread pushes TS bytes into ffmpeg while this thread drains PCM, so
    neither pipe ever buffers a whole segment in memory.
    """
    cmd = [
        ffmpeg, "-loglevel", "error",
        "-f", "mpegts", "-i", "pipe:0",
        "-vn", "-ac", str(CHANNELS), "-ar", str(SAMPLE_RATE),
        "-f", "s16le", "-acodec", "pcm_s16le", "pipe:1",
    ]
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, bufsize=0)
    feed_error = []

    def _feed():
        try:
            while True:
                chunk = seg_reader.read(SEG_CHUNK)
                if not chunk:
                    break
                proc.stdin.write(chunk)
        except Exception as exc:  # broken ZIP / read error: surface to the main thread
            feed_error.append(f"{type(exc).__name__}: {exc}")
        finally:
            try:
                proc.stdin.close()
            except Exception:
                pass
    feeder = threading.Thread(target=_feed, name="ts-feeder", daemon=True)
    feeder.start()
    produced = 0
    while True:
        raw = proc.stdout.read(PCM_CHUNK)
        if not raw:
            break
        buf = bytearray(raw)
        ranges.zero_in(buf, byte_cursor)
        out_fh.write(buf)
        byte_cursor += len(buf)
        produced += len(buf)
    proc.stdout.close()
    feeder.join()
    stderr = proc.stderr.read().decode("utf-8", "replace")
    proc.stderr.close()
    rc = proc.wait()
    if feed_error:
        raise MuteBuildError(f"读取源片段失败: {feed_error[0]}")
    if rc != 0:
        raise MuteBuildError(f"ffmpeg 解码失败(rc={rc}): {stderr.strip()[:300]}")
    if produced == 0:
        raise MuteBuildError("ffmpeg 未从该 TS 片段解出任何 PCM 样本（片段可能已损坏）")
    return byte_cursor


def build_muted_wav(zip_path: str, intervals: list, tmp_path: str,
                    ffmpeg: str) -> dict:
    """Build the muted WAV at ``tmp_path`` (caller renames it on success).

    Raises MuteInputError/MuteBuildError on any problem; never leaves a usable
    file behind on failure.
    """
    try:
        st_before = os.stat(zip_path)
    except OSError as exc:
        raise MuteBuildError(f"源 ZIP 不可读: {exc}")
    snapshot = (st_before.st_size, st_before.st_mtime_ns, st_before.st_ino)

    try:
        zf = zipfile.ZipFile(zip_path, "r")
    except (OSError, zipfile.BadZipFile) as exc:
        raise MuteBuildError(f"源 ZIP 无法打开: {exc}")

    with zf:
        try:
            playlist_text = zf.read("playlist.m3u8").decode("utf-8")
        except KeyError:
            raise MuteBuildError("源 ZIP 缺少 playlist.m3u8")
        except (OSError, zipfile.BadZipFile) as exc:
            raise MuteBuildError(f"读取清单失败: {exc}")
        order = parse_playlist_order(playlist_text)
        infos = {name: zf.getinfo(name) for name in zf.namelist()}
        for name in order:
            info = infos.get(name)
            if info is None:
                raise MuteBuildError(f"源包缺段: 清单引用的 {name} 不在 ZIP 内")
            if info.file_size <= 0:
                raise MuteBuildError(f"源片段为空: {name}")

        ranges = _ByteRanges(intervals)
        os.makedirs(os.path.dirname(os.path.abspath(tmp_path)), exist_ok=True)
        byte_cursor = 0
        warning = None
        try:
            with open(tmp_path, "wb") as out:
                out.write(_wav_header(0))  # sizes patched after the real length is known
                for idx, name in enumerate(order):
                    try:
                        with zf.open(name) as seg:
                            byte_cursor = _decode_segment(
                                ffmpeg, seg, out, ranges, byte_cursor)
                    except (OSError, zipfile.BadZipFile, MuteBuildError) as exc:
                        # A read/decode failure can mean the source was swapped
                        # out underneath us; check before reporting the detail.
                        try:
                            st_now = os.stat(zip_path)
                            now_snap = (st_now.st_size, st_now.st_mtime_ns, st_now.st_ino)
                        except OSError:
                            raise MuteBuildError("处理期间源 ZIP 消失，已中止本次消音")
                        if now_snap != snapshot:
                            raise MuteBuildError("处理期间源 ZIP 被改写，已中止本次消音")
                        raise MuteBuildError(f"读取/解码源片段 {name} 失败: {exc}")
                    if byte_cursor > MAX_DATA_BYTES:
                        raise MuteBuildError("录音超出 WAV 32 位长度上限，请分段处理")
                    if idx == 0 and TEST_HOOKS["tamper_job_id"] is not None:
                        _tamper_hook(zip_path)
        except MuteBuildError:
            raise
        except Exception as exc:
            try:
                st_now = os.stat(zip_path)
                now_snap = (st_now.st_size, st_now.st_mtime_ns, st_now.st_ino)
            except OSError:
                raise MuteBuildError("处理期间源 ZIP 消失，已中止本次消音")
            if now_snap != snapshot:
                raise MuteBuildError("处理期间源 ZIP 被改写，已中止本次消音")
            raise MuteBuildError(f"构建失败: {type(exc).__name__}: {exc}")

        # The source package must not have been rewritten while we read it.
        try:
            st_after = os.stat(zip_path)
        except OSError as exc:
            raise MuteBuildError(f"处理期间源 ZIP 消失: {exc}")
        if (st_after.st_size, st_after.st_mtime_ns, st_after.st_ino) != snapshot:
            raise MuteBuildError("处理期间源 ZIP 被改写，已中止本次消音")

        total_bytes = byte_cursor
        muted_bytes = ranges.intersecting_bytes(total_bytes)
        if muted_bytes == 0:
            raise MuteInputError(
                f"所有区间都超出录音时长（{total_bytes / BYTE_RATE:.3f} 秒），未遮蔽任何样本")
        clipped = sum(1 for lo, hi in ranges.ranges if hi > total_bytes)
        if clipped:
            warning = (f"有 {clipped} 个区间超出录音时长，超出部分已按录音结尾截断")
        # Patch the RIFF/data size fields, then make the bytes durable before
        # the caller publishes this file via atomic rename.
        with open(tmp_path, "r+b") as out:
            out.seek(0)
            out.write(_wav_header(total_bytes))
            out.flush()
            os.fsync(out.fileno())

    return {
        "total_samples": total_bytes // BLOCK_ALIGN,
        "muted_samples": muted_bytes // BLOCK_ALIGN,
        "duration_sec": total_bytes / BYTE_RATE,
        "warning": warning,
    }


def _tamper_hook(zip_path: str):
    """Test-only: back the ZIP up and truncate it mid-build."""
    job_id = TEST_HOOKS["tamper_job_id"]
    TEST_HOOKS["tamper_job_id"] = None
    backup = zip_path + ".tamper.bak"
    if not os.path.exists(backup):
        shutil.copy2(zip_path, backup)
    size = os.path.getsize(zip_path)
    with open(zip_path, "r+b") as fh:
        fh.truncate(max(1, size // 2))
    log.warning("TEST HOOK: source ZIP of job %s truncated mid-build", job_id)
