#!/usr/bin/env python3
"""
批量下载 zip -> 转 WebP -> 重新打包 -> 输出原始文件名
- 用 URL 的 ?f= 参数还原原始文件名(不再用路径哈希名)
- 支持 gallery-dl / requests 双下载器
- 多线程下载 + 多进程转图
"""
import os
import re
import sys
import shutil
import zipfile
import tempfile
import argparse
import subprocess
import multiprocessing as mp
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

import requests
from PIL import Image

IMG_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp"}
DEFAULT_DOWNLOAD_WORKERS = 3
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)


# ==================== 文件名解析(核心) ====================

def sanitize_name(name: str) -> str:
    """最小化清洗:只替换文件系统非法字符,保留中文/空格/括号"""
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", name)
    name = name.strip().strip(".")
    return name or "archive.zip"


def derive_filename(url: str, index: int, used: set) -> str:
    """
    从 URL 推导原始文件名:
    1) 优先取 ?f= / ?filename= / ?name= / ?file= 参数
    2) 退化到路径最后一段
    3) 清洗非法字符 + 重名去重
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)

    name = None
    for key in ("f", "filename", "name", "file"):
        if key in qs and qs[key]:
            name = unquote(qs[key][0])
            break

    if not name:
        name = parsed.path.rstrip("/").split("/")[-1] or f"archive_{index}.zip"

    # 只取文件名部分,防路径穿越
    name = Path(name).name

    if not name.lower().endswith(".zip"):
        name = f"{name}.zip"

    name = sanitize_name(name)

    # 重名去重:base_1.zip, base_2.zip ...
    base, ext = name[:-4], name[-4:]
    candidate, n = name, 1
    while candidate in used:
        candidate = f"{base}_{n}{ext}"
        n += 1
    used.add(candidate)
    return candidate


# ==================== 下载 ====================

def download(url: str, dest: Path, timeout: int = 300,
             downloader: str = "auto", min_interval: float = 1.0) -> Path:
    """
    下载单个 URL:
    - downloader="gallery-dl": 只用 gallery-dl
    - downloader="requests":   只用 requests
    - downloader="auto":       先用 gallery-dl,失败回退 requests
    """
    print(f"[下载开始] {url}", flush=True)
    dest.parent.mkdir(parents=True, exist_ok=True)

    use_gdl = downloader in ("auto", "gallery-dl") and shutil.which("gallery-dl")
    use_req = downloader in ("auto", "requests")

    # ---------- gallery-dl ----------
    if use_gdl:
        try:
            tmp_dir = dest.parent / f"_gdl_{abs(hash(url)) % 10**10}"
            tmp_dir.mkdir(exist_ok=True)
            subprocess.run([
                "gallery-dl",
                "--dest", str(tmp_dir),
                "--no-mtime",
                "--retries", "3",
                "--sleep-request", str(min_interval),
                "--user-agent", DEFAULT_UA,
                url,
            ], check=True, timeout=timeout, capture_output=True, text=True)

            files = [f for f in tmp_dir.rglob("*") if f.is_file()]
            if files:
                downloaded = max(files, key=lambda f: f.stat().st_size)
                shutil.move(str(downloaded), str(dest))
                shutil.rmtree(tmp_dir, ignore_errors=True)
                print(f"[下载完成] {dest.name} ({dest.stat().st_size} bytes) [gallery-dl]",
                      flush=True)
                return dest
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise RuntimeError("gallery-dl 未下载到文件")
        except Exception as e:
            print(f"[gallery-dl 失败] {e}", file=sys.stderr, flush=True)
            if downloader == "gallery-dl":
                raise  # 指定只用 gdl 时直接失败

    # ---------- requests 回退 ----------
    if not use_req:
        raise RuntimeError(f"下载器 {downloader} 不可用")

    headers = {"User-Agent": DEFAULT_UA}
    # pawchive 系站点需要 Referer
    if "pawchive" in url:
        headers["Referer"] = "https://pawchive.pw/"

    with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 16):
                if chunk:
                    f.write(chunk)
    print(f"[下载完成] {dest.name} ({dest.stat().st_size} bytes) [requests]",
          flush=True)
    return dest


def download_all(urls, downloads_dir: Path,
                 workers: int = DEFAULT_DOWNLOAD_WORKERS,
                 downloader: str = "auto",
                 min_interval: float = 1.0):
    """并发下载,文件名用原始名"""
    used = set()
    tasks = []
    for i, url in enumerate(urls, 1):
        name = derive_filename(url, i, used)
        tasks.append((url, downloads_dir / name))
        print(f"[命名] {name}", flush=True)

    results = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {
            pool.submit(download, url, path,
                        downloader=downloader, min_interval=min_interval): (url, path)
            for url, path in tasks
        }
        for fut in as_completed(future_map):
            url, path = future_map[fut]
            try:
                fut.result()
                results[url] = (path, None)
            except Exception as e:
                print(f"[下载失败] {url}: {e}", file=sys.stderr, flush=True)
                results[url] = (None, e)
    return [(u, *results[u]) for u in urls]


# ==================== 图片转换(多进程) ====================

def convert_one(args):
    """单图转换(顶层函数,供子进程调用)"""
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
        print("  (无图片可转换)", flush=True)
        return
    print(f"  转换 {len(tasks)} 张图片, 进程数={workers}", flush=True)
    ok = fail = 0
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for src, success, err in pool.map(convert_one, tasks, chunksize=8):
            if success:
                ok += 1
            else:
                fail += 1
                print(f"  [跳过] {src}: {err}", file=sys.stderr, flush=True)
    print(f"  转换完成: 成功 {ok}, 失败 {fail}", flush=True)


# ==================== 解压 / 打包 ====================

def safe_extract(zip_path: Path, out_dir: Path):
    """安全解压(防 zip slip)"""
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
    """处理单个 zip:解压 -> 转 webp -> 用【原文件名】重新打包"""
    print(f"[处理] {zip_path.name}", flush=True)
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        safe_extract(zip_path, tmp_dir)
        convert_images_in_dir(tmp_dir, quality, max_size, convert_workers)
        out_zip = out_root / zip_path.name  # 输出名 = 原文件名
        rezip(tmp_dir, out_zip, compress_level)
    print(f"[完成] {out_zip.name} ({out_zip.stat().st_size} bytes)", flush=True)
    return out_zip


# ==================== 主流程 ====================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--urls", required=True, help="URL 列表文件")
    parser.add_argument("--output", default="output", help="输出目录")
    parser.add_argument("--quality", type=int, default=80, help="WebP 质量 1-100")
    parser.add_argument("--max-size", type=int, default=None, help="最大边长(像素)")
    parser.add_argument("--compress-level", type=int, default=6, help="zip 压缩级别 1-9")
    parser.add_argument("--download-workers", type=int, default=DEFAULT_DOWNLOAD_WORKERS,
                        help="并发下载线程数")
    parser.add_argument("--convert-workers", type=int, default=max(1, os.cpu_count() or 2),
                        help="图片转换进程数")
    parser.add_argument("--downloader", default="auto",
                        choices=["auto", "gallery-dl", "requests"],
                        help="下载器选择")
    parser.add_argument("--min-request-interval", type=float, default=1.0,
                        help="请求最小间隔(秒),传给 gallery-dl 的 --sleep-request")
    parser.add_argument("--keep-originals", action="store_true", help="保留原始下载的 zip")
    args = parser.parse_args()

    urls = [ln.strip() for ln in Path(args.urls).read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.strip().startswith("#")]
    if not urls:
        print("没有 URL 可处理")
        return

    out_root = Path(args.output)
    out_root.mkdir(parents=True, exist_ok=True)
    downloads = out_root / "_downloads"
    downloads.mkdir(parents=True, exist_ok=True)

    print(f"CPU={os.cpu_count()}, 下载并发={args.download_workers}, "
          f"转换进程={args.convert_workers}, zip级别={args.compress_level}, "
          f"下载器={args.downloader}, 最小间隔={args.min_request_interval}s", flush=True)

    print(f"\n=== 阶段 1:并发下载 {len(urls)} 个压缩包(用原始文件名) ===", flush=True)
    results = download_all(
        urls, downloads,
        workers=args.download_workers,
        downloader=args.downloader,
        min_interval=args.min_request_interval,
    )

    print(f"\n=== 阶段 2:解压 + 转 WebP + 重新打包 ===", flush=True)
    ok = fail = 0
    for url, zip_path, err in results:
        if err or zip_path is None:
            fail += 1
            print(f"[跳过] {url} 下载失败", file=sys.stderr, flush=True)
            continue
        try:
            process_one_zip(
                zip_path, out_root,
                quality=args.quality,
                max_size=args.max_size,
                convert_workers=args.convert_workers,
                compress_level=args.compress_level,
            )
            if not args.keep_originals:
                zip_path.unlink(missing_ok=True)
            ok += 1
        except Exception as e:
            fail += 1
            print(f"[错误] 处理 {zip_path.name} 失败: {e}", file=sys.stderr, flush=True)

    if not args.keep_originals and downloads.exists() and not any(downloads.iterdir()):
        shutil.rmtree(downloads, ignore_errors=True)

    print(f"\n[全部完成] 成功 {ok}, 失败 {fail}", flush=True)
    if fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    mp.freeze_support()
    main()