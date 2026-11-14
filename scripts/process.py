#!/usr/bin/env python3
import os
import re
import sys
import time
import json
import random
import hashlib
import shutil
import zipfile
import argparse
import subprocess
import threading
import tempfile
import multiprocessing as mp
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote
from queue import Queue
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

import requests
from PIL import Image

# 支持的图片扩展名
IMG_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp"}
DEFAULT_DOWNLOAD_WORKERS = 5
DEFAULT_UA = "970656489@qq.com"
FALLBACK_UAS = [
    DEFAULT_UA,
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36",
]

# 解包工具：rar 优先用 unrar，zip/7z 用 7z
UNRAR = shutil.which("unrar") or shutil.which("unar")
SEVEN_ZIP = os.environ.get("SEVEN_ZIP_PATH") or shutil.which("7z") or shutil.which("7zz") or shutil.which("7za")


# ==================== 环境配置 ====================
def get_download_env():
    return {
        "ua": os.environ.get("DOWNLOAD_USER_AGENT", "").strip() or DEFAULT_UA,
        "cookie": os.environ.get("DOWNLOAD_COOKIE", "").strip(),
        "headers_raw": os.environ.get("DOWNLOAD_HEADERS", "").strip(),
    }


def parse_extra_headers(raw: str) -> dict:
    if not raw:
        return {}
    raw = raw.strip()
    if raw.startswith("{"):
        try:
            return {str(k): str(v) for k, v in json.loads(raw).items()}
        except Exception:
            pass
    headers = {}
    for line in raw.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip()] = v.strip()
    return headers


# ==================== 格式探测 ====================
def detect_format(path: Path) -> str:
    """读文件头判断真实归档格式。"""
    try:
        with open(path, "rb") as f:
            head = f.read(8)
    except OSError:
        return "unknown"
    if head.startswith(b"PK\x03\x04") or head.startswith(b"PK\x05\x06") or head.startswith(b"PK\x07\x08"):
        return "zip"
    if head.startswith(b"Rar!\x1a\x07"):
        return "rar"
    if head.startswith(b"7z\xbc\xaf\x27\x1c"):
        return "7z"
    if head.startswith(b"\x1f\x8b"):
        return "gzip"
    if head.startswith(b"<") or head.startswith(b"\xef\xbb\xbf<"):
        return "html"
    return "unknown"


# ==================== 文件名解析（严格保留原包名） ====================
def sanitize_name(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", name)
    name = name.strip().strip(".")
    return name or "archive"


def derive_filename(url: str, index: int, used: set) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    name = None
    for key in ("f", "filename", "name", "file"):
        if key in qs and qs[key]:
            name = unquote(qs[key][0])
            break
    if not name:
        name = parsed.path.rstrip("/").split("/")[-1] or f"archive_{index}.zip"
    name = Path(name).name  # 防止路径穿越

    # 剥掉原有压缩扩展名，避免出现 a.rar.zip
    lower = name.lower()
    for ext in (".rar", ".7z", ".tar", ".gz", ".bz2", ".xz", ".zip"):
        if lower.endswith(ext):
            name = name[: -len(ext)]
            break

    name = sanitize_name(name) + ".zip"
    base, ext = name[:-4], name[-4:]
    candidate, n = name, 1
    while candidate in used:
        candidate = f"{base}_{n}{ext}"
        n += 1
    used.add(candidate)
    return candidate


# ==================== 下载逻辑 ====================
def _tmp_dir_for(url: str, base: Path) -> Path:
    h = hashlib.md5(url.encode("utf-8")).hexdigest()[:10]
    return base / f"_gdl_{h}"


def _download_once(url: str, dest: Path, timeout: int, downloader: str, min_interval: float, env: dict, ua_index: int = 0) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    ua = env["ua"] if ua_index == 0 else FALLBACK_UAS[ua_index % len(FALLBACK_UAS)]
    extra_headers = parse_extra_headers(env["headers_raw"])
    cookie = env["cookie"]
    use_gdl = downloader in ("auto", "gallery-dl") and shutil.which("gallery-dl")
    use_req = downloader in ("auto", "requests")

    if use_gdl:
        try:
            tmp_dir = _tmp_dir_for(url, dest.parent)
            tmp_dir.mkdir(exist_ok=True)
            interval = round(random.uniform(min_interval, min_interval * 1.8), 2)
            cmd = ["gallery-dl", "--dest", str(tmp_dir), "--no-mtime", "--retries", "3",
                   "--sleep-request", str(interval), "--user-agent", ua]
            if cookie:
                cmd += ["--header", f"Cookie: {cookie}"]
            for k, v in extra_headers.items():
                cmd += ["--header", f"{k}: {v}"]
            cmd.append(url)
            subprocess.run(cmd, check=True, timeout=timeout, capture_output=True, text=True)
            files = [f for f in tmp_dir.rglob("*") if f.is_file()]
            if not files:
                shutil.rmtree(tmp_dir, ignore_errors=True)
                raise RuntimeError("gallery-dl 未下载到文件")
            downloaded = max(files, key=lambda f: f.stat().st_size)
            shutil.move(str(downloaded), str(dest))
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return dest
        except Exception:
            shutil.rmtree(_tmp_dir_for(url, dest.parent), ignore_errors=True)
            if downloader == "gallery-dl":
                raise

    if not use_req:
        raise RuntimeError(f"下载器 {downloader} 不可用")
    headers = {"User-Agent": ua}
    if "pawchive" in url:
        headers["Referer"] = "https://pawchive.pw/"
    headers.update(extra_headers)
    if cookie:
        headers["Cookie"] = cookie
    with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 16):
                if chunk:
                    f.write(chunk)
    return dest


def download(url: str, dest: Path, timeout: int = 300, downloader: str = "auto", min_interval: float = 1.0, retries: int = 3) -> Path:
    env = get_download_env()
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            print(f"[下载] {dest.name} (尝试 {attempt}/{retries})", flush=True)
            _download_once(url, dest, timeout, downloader, min_interval, env, ua_index=attempt - 1)

            # 下载后立刻校验真实格式
            fmt = detect_format(dest)
            if fmt == "unknown" or fmt == "html":
                size = dest.stat().st_size if dest.exists() else 0
                dest.unlink(missing_ok=True)
                raise RuntimeError(f"下载内容非法（格式={fmt}, {size} bytes），URL: {url}")

            print(f"[下载完成] {dest.name} ({dest.stat().st_size} bytes, 格式={fmt})", flush=True)
            return dest
        except Exception as e:
            last_err = e
            print(f"[下载失败 {attempt}/{retries}] {dest.name}: {e}", file=sys.stderr, flush=True)
            if dest.exists():
                dest.unlink(missing_ok=True)
            if attempt < retries:
                time.sleep(2 * attempt)
    raise RuntimeError(f"重试失败: {last_err}")


# ==================== 统一归档读取 ====================
def _extract_rar(path: Path, tmp: Path):
    """用 unrar 解压 RAR，失败时回退 7z。"""
    if UNRAR:
        # unrar: x 保留路径, -o+ 覆盖, -idq 静默
        # 目标目录必须以 / 结尾
        cmd = [UNRAR, "x", "-o+", "-idq", str(path), str(tmp) + os.sep]
        r = subprocess.run(cmd, capture_output=True)
        if r.returncode == 0:
            return
        # 某些 unrar 版本对空密码用 -p-，再试一次
        cmd = [UNRAR, "x", "-o+", "-idq", "-p-", str(path), str(tmp) + os.sep]
        r = subprocess.run(cmd, capture_output=True)
        if r.returncode == 0:
            return
        err = r.stderr.decode("utf-8", errors="replace")[:300]
        print(f"  [unrar 失败] rc={r.returncode}: {err}", file=sys.stderr, flush=True)

    if SEVEN_ZIP:
        cmd = [SEVEN_ZIP, "x", "-y", "-bd", f"-o{tmp}", str(path)]
        r = subprocess.run(cmd, capture_output=True)
        if r.returncode == 0:
            return
        err = r.stderr.decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"7z 解 rar 失败 rc={r.returncode}: {err}")

    raise RuntimeError("系统里没有 unrar 也没有可用的 7z，无法解 RAR")


def list_archive_members(path: Path) -> list:
    """返回 [(成员名, 内容bytes)]，支持 zip / rar / 7z。"""
    fmt = detect_format(path)
    if fmt == "zip":
        with zipfile.ZipFile(path, "r") as z:
            return [(i.filename, z.read(i)) for i in z.infolist() if not i.is_dir()]

    if fmt in ("rar", "7z"):
        tmp = Path(tempfile.mkdtemp(prefix="_arc_", dir=path.parent))
        try:
            if fmt == "rar":
                _extract_rar(path, tmp)
            else:  # 7z
                if not SEVEN_ZIP:
                    raise RuntimeError("检测到 7z，但环境中没有 7z")
                cmd = [SEVEN_ZIP, "x", "-y", "-bd", f"-o{tmp}", str(path)]
                r = subprocess.run(cmd, capture_output=True)
                if r.returncode != 0:
                    err = r.stderr.decode("utf-8", errors="replace")[:300]
                    raise RuntimeError(f"7z 解压失败 rc={r.returncode}: {err}")

            out = []
            for f in tmp.rglob("*"):
                if f.is_file():
                    out.append((str(f.relative_to(tmp)), f.read_bytes()))
            if not out:
                raise RuntimeError("解包后没有任何文件")
            return out
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    raise RuntimeError(f"不支持的归档格式: {fmt}")


# ==================== 图片压缩核心逻辑 ====================
def compress_worker(args):
    orig_name, src_path, dst_path, default_quality, max_size = args
    ext = Path(orig_name).suffix.lower()
    size = src_path.stat().st_size

    info = {
        "orig_name": orig_name,
        "ext": ext,
        "orig_size": size,
        "new_name": orig_name,
        "new_size": size,
        "converted": False,
        "quality": None,
        "reason": None,
        "error": None,
    }

    if ext == ".webp":
        info["reason"] = "已是 webp"
        return (orig_name, False, None, info, None)

    if ext == ".png" and size <= 800 * 1024:
        info["reason"] = "png 且 <=800KB"
        return (orig_name, False, None, info, None)

    if ext in (".jpg", ".jpeg") and size <= 700 * 1024:
        info["reason"] = "jpg 且 <=700KB"
        return (orig_name, False, None, info, None)

    final_q = 90 if ext in (".jpg", ".jpeg") else default_quality
    info["quality"] = final_q

    try:
        with Image.open(src_path) as im:
            im.load()
            info["width"], info["height"] = im.size
            info["mode"] = im.mode

            if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
                im = im.convert("RGBA")
            else:
                im = im.convert("RGB")

            if max_size:
                im.thumbnail((max_size, max_size), Image.LANCZOS)

            tmp_png = str(dst_path) + ".png"
            im.save(tmp_png, "PNG")

        cmd = ["cwebp", "-q", str(final_q), "-m", "2", "-sharp_yuv", tmp_png, "-o", str(dst_path)]
        subprocess.run(cmd, check=True, capture_output=True)
        os.unlink(tmp_png)

        new_size = dst_path.stat().st_size
        new_name = str(Path(orig_name).with_suffix(".webp"))

        info.update({
            "new_name": new_name,
            "new_size": new_size,
            "converted": True,
            "saved": size - new_size,
            "saved_pct": round((size - new_size) / size * 100, 2) if size else 0.0,
        })
        return (orig_name, True, None, info, (new_name, dst_path))
    except Exception as e:
        if os.path.exists(str(dst_path) + ".png"):
            os.unlink(str(dst_path) + ".png")
        info["error"] = str(e)
        return (orig_name, False, str(e), info, None)


# ==================== 处理单个压缩包并重打包 ====================
def process_one_zip(zip_path: Path, out_root: Path, default_quality, max_size, convert_workers, compress_level):
    out_zip = out_root / zip_path.name
    tmp_dir = out_root / f"_{hashlib.md5(zip_path.name.encode()).hexdigest()[:8]}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    print(f"[处理] {zip_path.name} (格式={detect_format(zip_path)})", flush=True)
    t0 = time.time()

    members = list_archive_members(zip_path)

    tasks, others = [], []
    for arcname, data in members:
        ext = Path(arcname).suffix.lower()
        if ext in IMG_EXTS:
            safe_name = hashlib.md5(arcname.encode("utf-8")).hexdigest() + ext
            src_temp = tmp_dir / safe_name
            src_temp.write_bytes(data)
            dst_temp = tmp_dir / (safe_name + ".webp")
            tasks.append((arcname, src_temp, dst_temp, default_quality, max_size))
        else:
            others.append((arcname, data))

    results = {}
    image_stats = []
    if tasks:
        with ProcessPoolExecutor(max_workers=convert_workers) as pool:
            for orig_name, converted, err, info, result_data in pool.map(compress_worker, tasks):
                if err:
                    print(f"  [转换失败] {orig_name}: {err}", file=sys.stderr)
                if converted and result_data:
                    results[orig_name] = result_data
                else:
                    src_temp = next(t[1] for t in tasks if t[0] == orig_name)
                    results[orig_name] = (orig_name, src_temp)
                if info:
                    image_stats.append(info)

    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED, compresslevel=compress_level) as zout:
        for arcname, data in others:
            zout.writestr(arcname, data)
        for orig_name, (new_name, file_path) in results.items():
            if orig_name != new_name:
                parts = list(Path(orig_name).parts)
                parts[-1] = Path(new_name).name
                arcname = str(Path(*parts))
            else:
                arcname = orig_name
            zout.write(file_path, arcname)

    shutil.rmtree(tmp_dir, ignore_errors=True)

    elapsed = round(time.time() - t0, 2)
    converted = [s for s in image_stats if s.get("converted")]
    skipped = [s for s in image_stats if not s.get("converted") and not s.get("error")]
    failed = [s for s in image_stats if s.get("error")]
    orig_total = sum(s["orig_size"] for s in image_stats)
    new_total = sum(s["new_size"] for s in image_stats if s.get("converted")) + \
        sum(s["orig_size"] for s in image_stats if not s.get("converted"))

    stat = {
        "zip_name": zip_path.name,
        "url": None,
        "src_zip_size": zip_path.stat().st_size,
        "out_zip_size": out_zip.stat().st_size,
        "image_count": len(image_stats),
        "other_count": len(others),
        "converted_count": len(converted),
        "skipped_count": len(skipped),
        "failed_count": len(failed),
        "image_orig_bytes": orig_total,
        "image_new_bytes": new_total,
        "image_saved_bytes": orig_total - new_total,
        "image_saved_pct": round((orig_total - new_total) / orig_total * 100, 2) if orig_total else 0.0,
        "elapsed_sec": elapsed,
        "images": image_stats,
    }
    return out_zip, stat


# ==================== 流式打包器 ====================
class StreamingBundler:
    def __init__(self, out_root: Path, max_bytes: int, compress_level: int = 1, keep_originals: bool = False):
        self.out_root = out_root
        self.max_bytes = max_bytes
        self.compress_level = compress_level
        self.keep_originals = keep_originals
        self.queue = []
        self.current_size = 0
        self.bundle_index = 0
        self.bundles = []
        self.lock = threading.Lock()
        self.bundle_records = []

    def add(self, zip_path: Path):
        if not zip_path.exists():
            return
        size = zip_path.stat().st_size
        with self.lock:
            if size > self.max_bytes:
                self._flush_locked()
                self._make_bundle_locked([zip_path])
                if not self.keep_originals:
                    zip_path.unlink(missing_ok=True)
                return
            if self.current_size + size > self.max_bytes and self.queue:
                self._flush_locked()
            self.queue.append(zip_path)
            self.current_size += size
            if self.current_size >= self.max_bytes:
                self._flush_locked()

    def _flush_locked(self):
        if not self.queue:
            return
        self._make_bundle_locked(self.queue)
        if not self.keep_originals:
            for p in self.queue:
                p.unlink(missing_ok=True)
        self.queue = []
        self.current_size = 0

    def _make_bundle_locked(self, files):
        self.bundle_index += 1
        bundle_path = self.out_root / f"bundle-{self.bundle_index:02d}.zip"
        with zipfile.ZipFile(bundle_path, "w", zipfile.ZIP_DEFLATED, compresslevel=self.compress_level) as zf:
            for f in files:
                zf.write(f, f.name)
        total = sum(f.stat().st_size for f in files)
        print(f"[打包] {bundle_path.name}: {len(files)} 个文件, 原始 {total/1024**2:.1f} MB -> 压缩后 {bundle_path.stat().st_size/1024**2:.1f} MB", flush=True)
        self.bundles.append(bundle_path)
        self.bundle_records.append({
            "bundle": bundle_path.name,
            "files": [f.name for f in files],
            "files_count": len(files),
            "src_bytes": total,
            "out_bytes": bundle_path.stat().st_size,
        })

    def finish(self):
        with self.lock:
            self._flush_locked()
        return self.bundles


# ==================== 统计输出 ====================
def fmt_bytes(n):
    if n is None:
        return "-"
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024


def html_escape(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;").replace("'", "&#39;"))


def write_summary(out_root: Path, stats, bundle_records, done_counter, errors, total_urls):
    json_path = out_root / "summary.json"
    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_urls": total_urls,
        "success": done_counter["done"],
        "failed": done_counter["failed"],
        "errors": errors,
        "bundles": bundle_records,
        "zips": stats,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    total_img = sum(s["image_count"] for s in stats)
    total_conv = sum(s["converted_count"] for s in stats)
    total_skip = sum(s["skipped_count"] for s in stats)
    total_fail = sum(s["failed_count"] for s in stats)
    total_orig = sum(s["image_orig_bytes"] for s in stats)
    total_new = sum(s["image_new_bytes"] for s in stats)
    saved = total_orig - total_new
    saved_pct = (saved / total_orig * 100) if total_orig else 0.0
    total_src_zip = sum(s["src_zip_size"] for s in stats)
    total_out_zip = sum(s["out_zip_size"] for s in stats)
    total_elapsed = sum(s["elapsed_sec"] for s in stats)

    def pct_bar(p):
        p = max(0.0, min(100.0, p))
        return (
            f'<div class="bar"><div class="bar-fill" style="width:{p:.1f}%"></div>'
            f'<span class="bar-label">{p:.1f}%</span></div>'
        )

    zip_rows = []
    for i, s in enumerate(stats, 1):
        zip_rows.append(f"""
        <tr>
          <td class="num">{i}</td>
          <td class="name" title="{html_escape(s['zip_name'])}">{html_escape(s['zip_name'])}</td>
          <td class="num">{s['image_count']}</td>
          <td class="num ok">{s['converted_count']}</td>
          <td class="num muted">{s['skipped_count']}</td>
          <td class="num {'err' if s['failed_count'] else ''}">{s['failed_count']}</td>
          <td class="num">{fmt_bytes(s['image_orig_bytes'])}</td>
          <td class="num">{fmt_bytes(s['image_new_bytes'])}</td>
          <td class="num">{s['image_saved_pct']:.1f}%</td>
          <td class="num">{s['elapsed_sec']:.1f}s</td>
        </tr>""")

    detail_blocks = []
    for i, s in enumerate(stats, 1):
        rows = []
        for img in s["images"]:
            if img.get("converted"):
                status, cls = "转换", "ok"
                note = f"节省 {img.get('saved_pct', 0)}%"
            elif img.get("error"):
                status, cls = "失败", "err"
                note = html_escape(img.get("error") or "")
            else:
                status, cls = "跳过", "muted"
                note = html_escape(img.get("reason") or "")
            rows.append(f"""
            <tr>
              <td class="name" title="{html_escape(img['orig_name'])}">{html_escape(img['orig_name'])}</td>
              <td>{html_escape(img.get('ext',''))}</td>
              <td class="num">{fmt_bytes(img.get('orig_size'))}</td>
              <td class="num">{fmt_bytes(img.get('new_size'))}</td>
              <td class="num">{img.get('quality') or '-'}</td>
              <td class="{cls}">{status}</td>
              <td class="note">{note}</td>
            </tr>""")
        url_line = f'<div class="url">{html_escape(s["url"])}</div>' if s.get("url") else ""
        detail_blocks.append(f"""
        <details class="zip-detail">
          <summary>
            <span class="idx">[{i}]</span>
            <span class="name">{html_escape(s['zip_name'])}</span>
            <span class="badge">{s['image_count']} 图</span>
            <span class="badge ok">{s['converted_count']} 转换</span>
            <span class="badge muted">{s['skipped_count']} 跳过</span>
            <span class="badge {'err' if s['failed_count'] else 'muted'}">{s['failed_count']} 失败</span>
            <span class="badge">{fmt_bytes(s['src_zip_size'])} → {fmt_bytes(s['out_zip_size'])}</span>
          </summary>
          {url_line}
          <table class="tbl images">
            <thead><tr>
              <th>原文件</th><th>扩展名</th><th>原始</th><th>处理后</th><th>质量</th><th>状态</th><th>说明</th>
            </tr></thead>
            <tbody>{''.join(rows)}</tbody>
          </table>
        </details>""")

    bundle_rows = []
    for b in bundle_records:
        bundle_rows.append(f"""
        <tr>
          <td class="name">{html_escape(b['bundle'])}</td>
          <td class="num">{b['files_count']}</td>
          <td class="num">{fmt_bytes(b['src_bytes'])}</td>
          <td class="num">{fmt_bytes(b['out_bytes'])}</td>
        </tr>""")

    error_rows = []
    for url, err in errors:
        error_rows.append(f"""
        <tr>
          <td class="name" title="{html_escape(url)}">{html_escape(url)}</td>
          <td class="err">{html_escape(err)}</td>
        </tr>""")

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Batch Archive → WebP 统计报告</title>
<style>
  :root {{
    --bg:#0f1419; --panel:#161b22; --panel2:#1c232b; --border:#2d333b;
    --fg:#e6edf3; --muted:#8b949e; --ok:#3fb950; --err:#f85149; --accent:#58a6ff;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin:0; padding:24px; background:var(--bg); color:var(--fg);
    font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans CJK SC","Microsoft YaHei",sans-serif;
  }}
  h1 {{ font-size:22px; margin:0 0 4px; }}
  h2 {{ font-size:16px; margin:28px 0 10px; color:var(--accent); border-left:3px solid var(--accent); padding-left:8px; }}
  .sub {{ color:var(--muted); font-size:13px; margin-bottom:20px; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; }}
  .card {{ background:var(--panel); border:1px solid var(--border); border-radius:8px; padding:14px 16px; }}
  .card .label {{ color:var(--muted); font-size:12px; margin-bottom:6px; }}
  .card .value {{ font-size:20px; font-weight:600; }}
  .card .value.ok {{ color:var(--ok); }}
  .card .value.err {{ color:var(--err); }}
  .card .hint {{ color:var(--muted); font-size:12px; margin-top:4px; }}
  .tbl {{ width:100%; border-collapse:collapse; background:var(--panel); border:1px solid var(--border); border-radius:8px; overflow:hidden; }}
  .tbl th, .tbl td {{ padding:8px 10px; text-align:left; border-bottom:1px solid var(--border); }}
  .tbl th {{ background:var(--panel2); color:var(--muted); font-weight:600; font-size:12px; text-transform:uppercase; letter-spacing:.03em; }}
  .tbl tr:last-child td {{ border-bottom:none; }}
  .tbl tbody tr:hover {{ background:var(--panel2); }}
  .num {{ text-align:right; font-variant-numeric:tabular-nums; white-space:nowrap; }}
  .name {{ max-width:420px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}
  .note {{ color:var(--muted); font-size:12px; }}
  .ok {{ color:var(--ok); }} .err {{ color:var(--err); }} .muted {{ color:var(--muted); }}
  .bar {{ position:relative; height:18px; background:var(--panel2); border-radius:4px; overflow:hidden; min-width:120px; }}
  .bar-fill {{ height:100%; background:linear-gradient(90deg,#238636,#3fb950); }}
  .bar-label {{ position:absolute; inset:0; display:flex; align-items:center; justify-content:center; font-size:11px; font-variant-numeric:tabular-nums; }}
  details.zip-detail {{ background:var(--panel); border:1px solid var(--border); border-radius:8px; margin-bottom:10px; }}
  details.zip-detail > summary {{ cursor:pointer; padding:10px 14px; display:flex; align-items:center; gap:10px; flex-wrap:wrap; list-style:none; }}
  details.zip-detail > summary::-webkit-details-marker {{ display:none; }}
  details.zip-detail[open] > summary {{ border-bottom:1px solid var(--border); }}
  details.zip-detail .url {{ padding:8px 14px; color:var(--muted); font-size:12px; word-break:break-all; }}
  details.zip-detail table {{ border:none; border-radius:0; }}
  .idx {{ color:var(--muted); font-variant-numeric:tabular-nums; }}
  .badge {{ font-size:11px; padding:2px 8px; border-radius:999px; background:var(--panel2); border:1px solid var(--border); color:var(--muted); white-space:nowrap; }}
  .badge.ok {{ color:var(--ok); border-color:#238636; }}
  .badge.err {{ color:var(--err); border-color:#8e1519; }}
  .empty {{ color:var(--muted); padding:14px; background:var(--panel); border:1px dashed var(--border); border-radius:8px; }}
  footer {{ margin-top:32px; color:var(--muted); font-size:12px; text-align:center; }}
</style>
</head>
<body>
  <h1>Batch Archive → WebP 统计报告</h1>
  <div class="sub">生成时间：{html_escape(payload['generated_at'])} ｜ URL 总数 {total_urls} ｜ 成功 {done_counter['done']} ｜ 失败 {done_counter['failed']}</div>

  <h2>总览</h2>
  <div class="cards">
    <div class="card"><div class="label">归档文件数</div><div class="value">{len(stats)}</div></div>
    <div class="card"><div class="label">归档原始总大小</div><div class="value">{fmt_bytes(total_src_zip)}</div></div>
    <div class="card"><div class="label">归档处理后总大小</div><div class="value">{fmt_bytes(total_out_zip)}</div></div>
    <div class="card"><div class="label">图片总数</div><div class="value">{total_img}</div>
      <div class="hint">转换 {total_conv} ｜ 跳过 {total_skip} ｜ 失败 <span class="{'err' if total_fail else 'muted'}">{total_fail}</span></div></div>
    <div class="card"><div class="label">图片原始总大小</div><div class="value">{fmt_bytes(total_orig)}</div></div>
    <div class="card"><div class="label">图片处理后总大小</div><div class="value">{fmt_bytes(total_new)}</div></div>
    <div class="card"><div class="label">节省空间</div><div class="value ok">{fmt_bytes(saved)}</div>
      <div class="hint">{saved_pct:.2f}%</div></div>
    <div class="card"><div class="label">处理总耗时</div><div class="value">{total_elapsed:.2f}s</div></div>
  </div>

  <h2>节省比例</h2>
  {pct_bar(saved_pct)}

  <h2>各归档汇总</h2>
  <table class="tbl">
    <thead><tr>
      <th>#</th><th>归档文件名</th><th>图片</th><th>转换</th><th>跳过</th><th>失败</th>
      <th>原始</th><th>处理后</th><th>节省</th><th>耗时</th>
    </tr></thead>
    <tbody>{''.join(zip_rows) if zip_rows else '<tr><td colspan="10" class="empty">无数据</td></tr>'}</tbody>
  </table>

  <h2>图片明细（点击展开）</h2>
  {''.join(detail_blocks) if detail_blocks else '<div class="empty">无数据</div>'}

  <h2>打包结果</h2>
  <table class="tbl">
    <thead><tr><th>Bundle</th><th>文件数</th><th>原始大小</th><th>打包后</th></tr></thead>
    <tbody>{''.join(bundle_rows) if bundle_rows else '<tr><td colspan="4" class="empty">无数据</td></tr>'}</tbody>
  </table>

  <h2>错误列表</h2>
  <table class="tbl">
    <thead><tr><th>URL</th><th>错误</th></tr></thead>
    <tbody>{''.join(error_rows) if error_rows else '<tr><td colspan="2" class="empty">无错误</td></tr>'}</tbody>
  </table>

  <footer>Batch Archive → WebP · 自动生成</footer>
</body>
</html>"""
    (out_root / "summary.html").write_text(html, encoding="utf-8")


# ==================== 流水线 ====================
def run_pipeline(urls, downloads_dir: Path, out_root: Path, bundler: StreamingBundler, args):
    q = Queue(maxsize=max(2, args.download_workers))
    errors = []
    stats = []
    results_lock = threading.Lock()
    done_counter = {"done": 0, "failed": 0}
    total = len(urls)

    def producer():
        used = set()
        tasks = [(url, derive_filename(url, i, used)) for i, url in enumerate(urls, 1)]
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=args.download_workers) as pool:
            future_map = {
                pool.submit(download, url, downloads_dir / name,
                            downloader=args.downloader,
                            min_interval=args.min_request_interval): url
                for url, name in tasks
            }
            for fut in as_completed(future_map):
                url = future_map[fut]
                try:
                    q.put((url, fut.result()))
                except Exception as e:
                    with results_lock:
                        errors.append((url, str(e)))
                        done_counter["failed"] += 1
                    print(f"::error::下载失败 {url}: {e}", flush=True)
        for _ in range(args.convert_workers):
            q.put(None)

    def consumer():
        while True:
            item = q.get()
            if item is None:
                q.put(None)
                return
            url, zip_path = item
            try:
                out_zip, stat = process_one_zip(
                    zip_path, out_root, args.quality, args.max_size,
                    args.convert_workers, args.compress_level)
                stat["url"] = url
                zip_path.unlink(missing_ok=True)
                bundler.add(out_zip)
                with results_lock:
                    stats.append(stat)
                    done_counter["done"] += 1
                    print(f"[进度] {done_counter['done'] + done_counter['failed']}/{total} 完成, 已打 {bundler.bundle_index} 个 bundle", flush=True)
            except Exception as e:
                with results_lock:
                    errors.append((url, str(e)))
                    done_counter["failed"] += 1
                print(f"::error::处理失败 {zip_path.name}: {e}", flush=True)

    p = threading.Thread(target=producer, daemon=True)
    consumers = [threading.Thread(target=consumer, daemon=True) for _ in range(args.convert_workers)]
    p.start()
    for c in consumers:
        c.start()
    p.join()
    for c in consumers:
        c.join()
    return done_counter, errors, stats


# ==================== 主流程 ====================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--urls", required=True)
    parser.add_argument("--output", default="output")
    parser.add_argument("--quality", type=int, default=90)
    parser.add_argument("--max-size", type=int, default=None)
    parser.add_argument("--compress-level", type=int, default=6)
    parser.add_argument("--download-workers", type=int, default=DEFAULT_DOWNLOAD_WORKERS)
    parser.add_argument("--convert-workers", type=int, default=max(1, os.cpu_count() or 2))
    parser.add_argument("--downloader", default="auto")
    parser.add_argument("--min-request-interval", type=float, default=1.0)
    parser.add_argument("--bundle-size-mb", type=int, default=1024)
    parser.add_argument("--keep-originals", action="store_true")
    args = parser.parse_args()

    urls = [ln.strip() for ln in Path(args.urls).read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.strip().startswith("#")]
    if not urls:
        return

    out_root = Path(args.output)
    out_root.mkdir(parents=True, exist_ok=True)
    downloads = out_root / "_downloads"
    downloads.mkdir(parents=True, exist_ok=True)

    bundler = StreamingBundler(out_root, args.bundle_size_mb * 1024**2, args.compress_level, args.keep_originals)
    done_counter, errors, stats = run_pipeline(urls, downloads, out_root, bundler, args)
    bundles = bundler.finish()

    if not args.keep_originals and downloads.exists() and not any(downloads.iterdir()):
        shutil.rmtree(downloads, ignore_errors=True)

    write_summary(out_root, stats, bundler.bundle_records, done_counter, errors, len(urls))

    print(f"\n[全部完成] 成功 {done_counter['done']}, 失败 {done_counter['failed']}, 独立包 {len(bundles)} 个", flush=True)
    if done_counter["done"] == 0 and done_counter["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    mp.freeze_support()
    main()
