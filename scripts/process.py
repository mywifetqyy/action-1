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
import multiprocessing as mp
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote
from queue import Queue
from concurrent.futures import ProcessPoolExecutor

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

# ==================== 环境配置 ====================
def get_download_env():
    return {
        "ua": os.environ.get("DOWNLOAD_USER_AGENT", "").strip() or DEFAULT_UA,
        "cookie": os.environ.get("DOWNLOAD_COOKIE", "").strip(),
        "headers_raw": os.environ.get("DOWNLOAD_HEADERS", "").strip(),
    }

def parse_extra_headers(raw: str) -> dict:
    if not raw: return {}
    raw = raw.strip()
    if raw.startswith("{"):
        try: return {str(k): str(v) for k, v in json.loads(raw).items()}
        except: pass
    headers = {}
    for line in raw.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip()] = v.strip()
    return headers

# ==================== 文件名解析（保包名用） ====================
def sanitize_name(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", name)
    name = name.strip().strip(".")
    return name or "archive.zip"

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
    name = Path(name).name
    if not name.lower().endswith(".zip"):
        name = f"{name}.zip"
    name = sanitize_name(name)
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
            cmd = ["gallery-dl", "--dest", str(tmp_dir), "--no-mtime", "--retries", "3", "--sleep-request", str(interval), "--user-agent", ua]
            if cookie: cmd += ["--header", f"Cookie: {cookie}"]
            for k, v in extra_headers.items(): cmd += ["--header", f"{k}: {v}"]
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
        except Exception as e:
            shutil.rmtree(_tmp_dir_for(url, dest.parent), ignore_errors=True)
            if downloader == "gallery-dl": raise

    if not use_req: raise RuntimeError(f"下载器 {downloader} 不可用")
    headers = {"User-Agent": ua}
    if "pawchive" in url: headers["Referer"] = "https://pawchive.pw/"
    headers.update(extra_headers)
    if cookie: headers["Cookie"] = cookie
    with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 16):
                if chunk: f.write(chunk)
    return dest

def download(url: str, dest: Path, timeout: int = 300, downloader: str = "auto", min_interval: float = 1.0, retries: int = 3) -> Path:
    env = get_download_env()
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            print(f"[下载] {dest.name} (尝试 {attempt}/{retries})", flush=True)
            _download_once(url, dest, timeout, downloader, min_interval, env, ua_index=attempt - 1)
            print(f"[下载完成] {dest.name} ({dest.stat().st_size} bytes)", flush=True)
            return dest
        except Exception as e:
            last_err = e
            print(f"[下载失败 {attempt}/{retries}] {dest.name}: {e}", file=sys.stderr, flush=True)
            if dest.exists(): dest.unlink(missing_ok=True)
            if attempt < retries: time.sleep(2 * attempt)
    raise RuntimeError(f"重试失败: {last_err}")

# ==================== 图片压缩核心逻辑 ====================
def compress_worker(args):
    orig_name, src_path, dst_path, target_quality, max_size = args
    ext = Path(orig_name).suffix.lower()
    size = src_path.stat().st_size

    # 规则 1 & 2: 不转换
    if ext == ".webp": return (orig_name, False, None, None)
    if ext == ".png" and size <= 500 * 1024: return (orig_name, False, None, None)

    # 规则 3: 动态质量
    final_q = target_quality
    if ext in (".jpg", ".jpeg"):
        final_q = 95 if size <= 700 * 1024 else 90

    try:
        with Image.open(src_path) as im:
            im.load()
            if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
                im = im.convert("RGBA")
            else:
                im = im.convert("RGB")
            if max_size: im.thumbnail((max_size, max_size), Image.LANCZOS)
            
            tmp_png = str(dst_path) + ".png"
            im.save(tmp_png, "PNG")

        cmd = ["cwebp", "-q", str(final_q), "-m", "2", "-sharp_yuv", tmp_png, "-o", str(dst_path)]
        subprocess.run(cmd, check=True, capture_output=True)
        os.unlink(tmp_png)

        new_name = str(Path(orig_name).with_suffix(".webp"))
        return (orig_name, True, None, (new_name, dst_path))
    except Exception as e:
        if os.path.exists(str(dst_path) + ".png"): os.unlink(str(dst_path) + ".png")
        return (orig_name, False, str(e), None)

# ==================== 原样保留 ZIP 结构与重打包 ====================
def process_one_zip(zip_path: Path, out_root: Path, quality, max_size, convert_workers, compress_level):
    out_zip = out_root / zip_path.name
    tmp_dir = out_root / f"_{hashlib.md5(zip_path.name.encode()).hexdigest()[:8]}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    tasks, others = [], []
    print(f"[处理] {zip_path.name}", flush=True)
    with zipfile.ZipFile(zip_path, 'r') as zin:
        for item in zin.infolist():
            if item.is_dir(): continue
            ext = Path(item.filename).suffix.lower()
            if ext in IMG_EXTS:
                safe_name = hashlib.md5(item.filename.encode('utf-8')).hexdigest() + ext
                src_temp = tmp_dir / safe_name
                with zin.open(item) as f_in, open(src_temp, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                dst_temp = tmp_dir / (safe_name + ".webp")
                tasks.append((item.filename, src_temp, dst_temp, quality, max_size))
            else:
                others.append(item)

        results = {}
        if tasks:
            with ProcessPoolExecutor(max_workers=convert_workers) as pool:
                for orig_name, converted, err, result_data in pool.map(compress_worker, tasks):
                    if err: print(f"  [转换失败] {orig_name}: {err}", file=sys.stderr)
                    if converted and result_data:
                        results[orig_name] = result_data
                    else:
                        src_temp = next(t[1] for t in tasks if t[0] == orig_name)
                        results[orig_name] = (orig_name, src_temp)

        with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED, compresslevel=compress_level) as zout:
            for item in others:
                zout.writestr(item, zin.read(item))
            for orig_name, (new_name, file_path) in results.items():
                if orig_name != new_name:
                    parts = list(Path(orig_name).parts)
                    parts[-1] = Path(new_name).name
                    arcname = str(Path(*parts))
                else:
                    arcname = orig_name
                zout.write(file_path, arcname)

    shutil.rmtree(tmp_dir, ignore_errors=True)
    return out_zip

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

    def add(self, zip_path: Path):
        if not zip_path.exists(): return
        size = zip_path.stat().st_size
        with self.lock:
            if size > self.max_bytes:
                self._flush_locked()
                self._make_bundle_locked([zip_path])
                if not self.keep_originals: zip_path.unlink(missing_ok=True)
                return
            if self.current_size + size > self.max_bytes and self.queue:
                self._flush_locked()
            self.queue.append(zip_path)
            self.current_size += size
            if self.current_size >= self.max_bytes:
                self._flush_locked()

    def _flush_locked(self):
        if not self.queue: return
        self._make_bundle_locked(self.queue)
        if not self.keep_originals:
            for p in self.queue: p.unlink(missing_ok=True)
        self.queue = []
        self.current_size = 0

    def _make_bundle_locked(self, files):
        self.bundle_index += 1
        bundle_path = self.out_root / f"bundle-{self.bundle_index:02d}.zip"
        with zipfile.ZipFile(bundle_path, "w", zipfile.ZIP_DEFLATED, compresslevel=self.compress_level) as zf:
            for f in files: zf.write(f, f.name)
        total = sum(f.stat().st_size for f in files)
        print(f"[打包] {bundle_path.name}: {len(files)} 个文件, 原始 {total/1024**2:.1f} MB -> 压缩后 {bundle_path.stat().st_size/1024**2:.1f} MB", flush=True)
        self.bundles.append(bundle_path)

    def finish(self):
        with self.lock: self._flush_locked()
        return self.bundles

# ==================== 流水线 ====================
def run_pipeline(urls, downloads_dir: Path, out_root: Path, bundler: StreamingBundler, args):
    q = Queue(maxsize=max(2, args.download_workers))
    errors = []
    results_lock = threading.Lock()
    done_counter = {"done": 0, "failed": 0}
    total = len(urls)

    def producer():
        used = set()
        tasks = [(url, derive_filename(url, i, used)) for i, url in enumerate(urls, 1)]
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=args.download_workers) as pool:
            future_map = {pool.submit(download, url, downloads_dir / name, downloader=args.downloader, min_interval=args.min_request_interval): url for url, name in tasks}
            for fut in as_completed(future_map):
                url = future_map[fut]
                try: q.put((url, fut.result()))
                except Exception as e:
                    with results_lock:
                        errors.append((url, str(e)))
                        done_counter["failed"] += 1
                    print(f"::error::下载失败 {url}: {e}", flush=True)
        for _ in range(args.convert_workers): q.put(None)

    def consumer():
        while True:
            item = q.get()
            if item is None:
                q.put(None)
                return
            url, zip_path = item
            try:
                out_zip = process_one_zip(zip_path, out_root, args.quality, args.max_size, args.convert_workers, args.compress_level)
                zip_path.unlink(missing_ok=True)
                bundler.add(out_zip)
                with results_lock:
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
    for c in consumers: c.start()
    p.join()
    for c in consumers: c.join()
    return done_counter, errors

# ==================== 主流程 ====================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--urls", required=True)
    parser.add_argument("--output", default="output")
    parser.add_argument("--quality", type=int, default=90)
    parser.add_argument("--max-size", type=int, default=None)
    parser.add_argument("--compress-level", type=int, default=6)  # 补上这个参数
    parser.add_argument("--download-workers", type=int, default=DEFAULT_DOWNLOAD_WORKERS)
    parser.add_argument("--convert-workers", type=int, default=max(1, os.cpu_count() or 2))
    parser.add_argument("--downloader", default="auto")
    parser.add_argument("--min-request-interval", type=float, default=1.0)
    parser.add_argument("--bundle-size-mb", type=int, default=1024)
    parser.add_argument("--keep-originals", action="store_true")
    args = parser.parse_args()

    urls = [ln.strip() for ln in Path(args.urls).read_text(encoding="utf-8").splitlines() if ln.strip() and not ln.strip().startswith("#")]
    if not urls: return

    out_root = Path(args.output)
    out_root.mkdir(parents=True, exist_ok=True)
    downloads = out_root / "_downloads"
    downloads.mkdir(parents=True, exist_ok=True)

    bundler = StreamingBundler(out_root, args.bundle_size_mb * 1024**2, args.compress_level, args.keep_originals)
    done_counter, errors = run_pipeline(urls, downloads, out_root, bundler, args)
    bundles = bundler.finish()

    if not args.keep_originals and downloads.exists() and not any(downloads.iterdir()):
        shutil.rmtree(downloads, ignore_errors=True)

    print(f"\n[全部完成] 成功 {done_counter['done']}, 失败 {done_counter['failed']}, 独立包 {len(bundles)} 个", flush=True)
    if done_counter["done"] == 0 and done_counter["failed"] > 0: sys.exit(1)

if __name__ == "__main__":
    mp.freeze_support()
    main()