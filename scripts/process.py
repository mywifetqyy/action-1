#!/usr/bin/env python3
import os
import sys
import time
import shutil
import zipfile
import tempfile
import argparse
import subprocess
import threading
import multiprocessing as mp
from pathlib import Path
from urllib.parse import unquote, urlsplit
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

import requests
from PIL import Image

IMG_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp"}
DEFAULT_DOWNLOAD_WORKERS = 3          # 站点上限:5;降到 3 降低峰值与风控
DEFAULT_DOWNLOAD_TIMEOUT = 180
DEFAULT_GDL_SLEEP = 1.0               # 站点上限:1 请求/秒
DEFAULT_MIN_REQUEST_INTERVAL = 1.0

DEFAULT_USER_AGENT = "970656489@qq.com"

_RATE_LOCK = threading.Lock()
_LAST_REQUEST_AT = 0.0


def user_agent() -> str:
    return os.environ.get("DOWNLOAD_USER_AGENT", "").strip() or DEFAULT_USER_AGENT


def throttle(min_interval: float) -> None:
    global _LAST_REQUEST_AT
    if min_interval <= 0:
        return
    with _RATE_LOCK:
        wait = min_interval - (time.monotonic() - _LAST_REQUEST_AT)
        if wait > 0:
            time.sleep(wait)
        _LAST_REQUEST_AT = time.monotonic()


def custom_headers() -> dict:
    result = {}
    raw = os.environ.get("DOWNLOAD_HEADERS", "")
    for item in raw.split(";"):
        if ":" in item:
            key, value = item.split(":", 1)
            if key.strip() and value.strip():
                result[key.strip()] = value.strip()
    cookie = os.environ.get("DOWNLOAD_COOKIE", "").strip()
    if cookie:
        result["Cookie"] = cookie
    return result


def request_headers() -> dict:
    headers = {
        "User-Agent": user_agent(),
        "Accept": "application/zip,application/octet-stream,*/*;q=0.8",
    }
    headers.update(custom_headers())
    return headers


def download_with_gallery_dl(url: str, dest: Path,
                             timeout: int = DEFAULT_DOWNLOAD_TIMEOUT,
                             sleep_request: float = DEFAULT_GDL_SLEEP,
                             min_interval: float = DEFAULT_MIN_REQUEST_INTERVAL) -> Path:
    print(f"[gallery-dl 下载] {url}", flush=True)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = dest.parent / f"_gdl_{dest.stem}"
    shutil.rmtree(tmp_dir, ignore_errors=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "-m", "gallery_dl",
        "--config-ignore",
        "--destination", str(tmp_dir),
        "--no-mtime",
        "--retries", "3",
        "--sleep-request", str(sleep_request),
        "--user-agent", user_agent(),
    ]
    for key, value in custom_headers().items():
        cmd += ["-o", f"headers.{key}={value}"]
    cmd.append(url)

    try:
        throttle(min_interval)
        proc = subprocess.run(
            cmd, check=True, timeout=timeout,
            capture_output=True, text=True,
            encoding="utf-8", errors="replace",
        )
        files = [p for p in tmp_dir.rglob("*") if p.is_file() and p.stat().st_size > 0]
        if not files:
            detail = (proc.stdout or proc.stderr or "").strip()[-500:]
            raise RuntimeError(f"gallery-dl 未下载到文件: {detail}")
        downloaded = max(files, key=lambda p: p.stat().st_size)
        dest.unlink(missing_ok=True)
        shutil.move(str(downloaded), str(dest))
        print(f"[下载完成] {dest.name} ({dest.stat().st_size} bytes) [gallery-dl]", flush=True)
        return dest
    except FileNotFoundError as exc:
        raise RuntimeError("gallery-dl 未安装(pip install gallery-dl)") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"gallery-dl 超时({timeout}s): {url}") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip()[-800:]
        raise RuntimeError(f"gallery-dl 退出码 {exc.returncode}: {detail}") from exc
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def download_with_requests(url: str, dest: Path,
                           timeout: int = DEFAULT_DOWNLOAD_TIMEOUT,
                           min_interval: float = DEFAULT_MIN_REQUEST_INTERVAL) -> Path:
    print(f"[requests 下载] {url}", flush=True)
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    headers = request_headers()
    last_error = None
    for attempt in range(1, 4):
        try:
            throttle(min_interval)
            with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
                r.raise_for_status()
                with open(part, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 16):
                        if chunk:
                            f.write(chunk)
            part.replace(dest)
            print(f"[下载完成] {dest.name} ({dest.stat().st_size} bytes) [requests]", flush=True)
            return dest
        except Exception as exc:
            last_error = exc
            part.unlink(missing_ok=True)
            print(f"[重试 {attempt}/3] {url}: {exc}", file=sys.stderr, flush=True)
            if attempt < 3:
                time.sleep(2 * attempt)
    raise RuntimeError(f"requests 下载失败(3 次): {url}: {last_error}")


def download(url: str, dest: Path, timeout: int = DEFAULT_DOWNLOAD_TIMEOUT,
             downloader: str = "auto",
             gdl_sleep: float = DEFAULT_GDL_SLEEP,
             min_interval: float = DEFAULT_MIN_REQUEST_INTERVAL) -> Path:
    if downloader in ("auto", "gallery-dl"):
        try:
            return download_with_gallery_dl(url, dest, timeout, gdl_sleep, min_interval)
        except Exception as exc:
            print(f"[gallery-dl 失败] {url}: {exc}", file=sys.stderr, flush=True)
            if downloader == "gallery-dl":
                raise
            print(f"[回退 requests] {url}", file=sys.stderr, flush=True)
    return download_with_requests(url, dest, timeout, min_interval)


def convert_one(args):
    src_str, dst_str, quality, max_size = args
    src, dst = Path(src_str), Path(dst_str)
    try:
        with Image.open(src) as im:
            im.load()
            if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
                im = im.convert("RGBA")
            else:
                im = im.convert("RGB")
            if max_size:
                im.thumbnail((max_size, max_size), Image.LANCZOS)
            dst.parent.mkdir(parents=True, exist_ok=True)
            im.save(dst, "WEBP", quality=quality, method=4)
        if dst != src:
            src.unlink(missing_ok=True)
        return (str(src), True, None)
    except Exception as e:
        return (str(src), False, str(e))


def convert_images_in_dir(root_dir: Path, quality: int, max_size, workers: int):
    tasks = []
    for r, _, files in os.walk(root_dir):
        for name in files:
            src = Path(r) / name
            if src.suffix.lower() in IMG_EXTS:
                tasks.append((str(src), str(src.with_suffix(".webp")), quality, max_size))
    if not tasks:
        print("  (该包内没有可转换的图片)")
        return
    print(f"  开始转换 {len(tasks)} 张图片,进程数={workers}")
    ok = fail = 0
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for src, success, err in pool.map(convert_one, tasks, chunksize=8):
            if success:
                ok += 1
            else:
                fail += 1
                print(f"  [跳过] {src}: {err}", file=sys.stderr)
    print(f"  转换完成:成功 {ok},失败 {fail}")


def safe_extract(zip_path: Path, out_dir: Path):
    with zipfile.ZipFile(zip_path) as zf:
        out_resolved = out_dir.resolve()
        for member in zf.namelist():
            member_path = (out_dir / member).resolve()
            if not str(member_path).startswith(str(out_resolved)):
                raise RuntimeError(f"非法路径: {member}")
        zf.extractall(out_dir)


def rezip(src_dir: Path, zip_out: Path, compress_level: int):
    zip_out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_out, "w", zipfile.ZIP_DEFLATED,
                         compresslevel=compress_level) as zf:
        for r, _, files in os.walk(src_dir):
            for name in files:
                full = Path(r) / name
                zf.write(full, full.relative_to(src_dir))


def process_one_zip(zip_path: Path, out_root: Path, quality, max_size,
                    convert_workers, compress_level):
    print(f"[处理] {zip_path.name}")
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        safe_extract(zip_path, tmp_dir)
        convert_images_in_dir(tmp_dir, quality, max_size, convert_workers)
        out_zip = out_root / zip_path.name
        rezip(tmp_dir, out_zip, compress_level)
    print(f"[完成] {out_zip.name} ({out_zip.stat().st_size} bytes)")
    return out_zip


def make_tasks(urls, downloads_dir: Path):
    tasks = []
    used = set()
    for i, url in enumerate(urls, 1):
        # 取路径部分(不含查询串),再取最后一段并完全解码
        path_part = urlsplit(url).path.rstrip("/")
        raw_name = path_part.split("/")[-1] if path_part else ""
        name = unquote(raw_name) or f"archive_{i}.zip"
        # 解码后可能含路径分隔符或非法字符,只取 basename 兜底
        name = Path(name).name
        if not name.lower().endswith(".zip"):
            name = f"archive_{i}.zip"
        stem, seq = name[:-4], 2
        while name in used:
            name = f"{stem}_{seq}.zip"
            seq += 1
        used.add(name)
        tasks.append((url, downloads_dir / name))
    return tasks


def run_pipeline(urls, downloads: Path, out_root: Path, args):
    """边下边处理:每下完一个包立刻处理,处理完按需删原包。"""
    tasks = make_tasks(urls, downloads)
    ok = fail = 0
    with ThreadPoolExecutor(max_workers=args.download_workers) as pool:
        future_map = {
            pool.submit(download, url, path, args.download_timeout,
                        args.downloader, args.gdl_sleep, args.min_request_interval):
                (url, path)
            for url, path in tasks
        }
        for fut in as_completed(future_map):
            url, path = future_map[fut]
            try:
                fut.result()
            except Exception as e:
                fail += 1
                print(f"[下载失败] {url}: {e}", file=sys.stderr)
                continue
            try:
                process_one_zip(
                    path, out_root,
                    quality=args.quality,
                    max_size=args.max_size,
                    convert_workers=args.convert_workers,
                    compress_level=args.compress_level,
                )
                ok += 1
            except Exception as e:
                fail += 1
                print(f"[错误] 处理 {path.name} 失败: {e}", file=sys.stderr)
            finally:
                # 无论成败都清理原包,避免失败批次占满磁盘
                if not args.keep_originals:
                    path.unlink(missing_ok=True)
                    print(f"[删除原包] {path.name}")
    return ok, fail


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--urls", required=True)
    parser.add_argument("--output", default="output")
    parser.add_argument("--quality", type=int, default=80)
    parser.add_argument("--max-size", type=int, default=None)
    parser.add_argument("--compress-level", type=int, default=6)
    parser.add_argument("--download-workers", type=int, default=DEFAULT_DOWNLOAD_WORKERS)
    parser.add_argument("--convert-workers", type=int,
                        default=min(2, os.cpu_count() or 2))
    parser.add_argument("--downloader", choices=["auto", "gallery-dl", "requests"],
                        default="auto")
    parser.add_argument("--download-timeout", type=int, default=DEFAULT_DOWNLOAD_TIMEOUT)
    parser.add_argument("--gdl-sleep", type=float, default=DEFAULT_GDL_SLEEP)
    parser.add_argument("--min-request-interval", type=float,
                        default=DEFAULT_MIN_REQUEST_INTERVAL)
    parser.add_argument("--keep-originals", action="store_true")
    args = parser.parse_args()

    urls = [ln.strip() for ln in Path(args.urls).read_text(encoding="utf-8-sig").splitlines()
            if ln.strip() and not ln.strip().startswith("#")]
    if not urls:
        print("没有 URL 可处理")
        return

    out_root = Path(args.output)
    out_root.mkdir(parents=True, exist_ok=True)
    downloads = out_root / "_downloads"
    downloads.mkdir(parents=True, exist_ok=True)

    cpu = os.cpu_count() or 2
    print(f"CPU 核数={cpu},下载并发={args.download_workers},"
          f"转换进程={args.convert_workers},zip 级别={args.compress_level},"
          f"下载器={args.downloader},UA={user_agent()}")
    if args.download_workers > 5 or args.min_request_interval < 1.0:
        print("[警告] 已超出站点建议的限速(1 请求/秒、5 个并发下载),可能被 403",
              file=sys.stderr)

    print(f"\n=== 流水线:下载 + 处理 {len(urls)} 个压缩包(边下边处理) ===")
    ok, fail = run_pipeline(urls, downloads, out_root, args)

    if not args.keep_originals and downloads.exists() and not any(downloads.iterdir()):
        shutil.rmtree(downloads, ignore_errors=True)

    print(f"\n[全部完成] 成功 {ok},失败 {fail}")


if __name__ == "__main__":
    mp.freeze_support()
    main()