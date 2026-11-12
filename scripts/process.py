#!/usr/bin/env python3
"""
批量下载 zip -> 转 WebP -> 重新打包 -> 流式按 1GB 打独立包 -> 发布
- 用 URL 的 ?f= 参数还原原始文件名(不再用路径哈希名)
- 支持 gallery-dl / requests 双下载器
- 多线程下载 + 多进程转图
- 流式打包:每累计 >= N MB 立刻打一个 bundle-NN.zip,并删除已打包的单包
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

    name = Path(name).name  # 防路径穿越

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


# ==================== 下载 ====================

def download(url: str, dest: Path, timeout: int = 300,
             downloader: str = "auto", min_interval: float = 1.0) -> Path:
    """
    - downloader="gallery-dl": 只用 gallery-dl
    - downloader="requests":   只用 requests
    - downloader="auto":       先用 gallery-dl,失败回退 requests
    """
    print(f"[下载开始] {url}", flush=True)
    dest.parent.mkdir(parents=True, exist_ok=True)

    use_gdl = downloader in ("auto", "gallery-dl") and shutil.which("gallery-dl")
    use_req = downloader in ("auto", "requests")

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
                raise

    if not use_req:
        raise RuntimeError(f"下载器 {downloader} 不可用")

    headers = {"User-Agent": DEFAULT_UA}
    if "pawchive" in url:
        headers["Referer"] = "https://pawchive.pw/"

    with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 16):
                if chunk:
                    f.write(chunk)
    print(f"[下载完成] {dest.name} ({dest.stat().st_size} bytes) [requests]", flush=True)
    return dest


def download_all(urls, downloads_dir: Path,
                 workers: int = DEFAULT_DOWNLOAD_WORKERS,
                 downloader: str = "auto",
                 min_interval: float = 1.0):
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
    print(f"[处理] {zip_path.name}", flush=True)
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        safe_extract(zip_path, tmp_dir)
        convert_images_in_dir(tmp_dir, quality, max_size, convert_workers)
        out_zip = out_root / zip_path.name
        rezip(tmp_dir, out_zip, compress_level)
    print(f"[完成] {out_zip.name} ({out_zip.stat().st_size} bytes)", flush=True)
    return out_zip


# ==================== 流式打包器(核心) ====================

class StreamingBundler:
    """
    流式打包:每累计 >= max_bytes 就打一个 bundle-NN.zip,然后删掉已打包的单包。
    磁盘峰值 = 队列里最多 max_bytes 单包 + 正在写的 1 个 bundle。
    """

    def __init__(self, out_root: Path, max_bytes: int,
                 compress_level: int = 1, keep_originals: bool = False):
        self.out_root = out_root
        self.max_bytes = max_bytes
        self.compress_level = compress_level
        self.keep_originals = keep_originals
        self.queue = []          # [Path, ...]
        self.current_size = 0
        self.bundle_index = 0
        self.bundles = []

    def add(self, zip_path: Path):
        """加入一个单包；累计达上限立刻打包并删除这批单包"""
        if not zip_path.exists():
            return
        size = zip_path.stat().st_size

        # 单文件本身超限:先 flush 当前队列,再单独成 bundle
        if size > self.max_bytes:
            self.flush()
            self._make_bundle([zip_path])
            if not self.keep_originals:
                zip_path.unlink(missing_ok=True)
            return

        if self.current_size + size > self.max_bytes and self.queue:
            self.flush()

        self.queue.append(zip_path)
        self.current_size += size

        # 正好达到/超过上限,立刻打包
        if self.current_size >= self.max_bytes:
            self.flush()

    def flush(self):
        """把当前队列打成一个 bundle,并删除已打包的单包"""
        if not self.queue:
            return
        self._make_bundle(self.queue)
        if not self.keep_originals:
            for p in self.queue:
                p.unlink(missing_ok=True)
        self.queue = []
        self.current_size = 0

    def _make_bundle(self, files):
        self.bundle_index += 1
        bundle_path = self.out_root / f"bundle-{self.bundle_index:02d}.zip"
        with zipfile.ZipFile(bundle_path, "w", zipfile.ZIP_DEFLATED,
                             compresslevel=self.compress_level) as zf:
            for f in files:
                zf.write(f, f.name)
        total = sum(f.stat().st_size for f in files)
        print(f"[打包] {bundle_path.name}: {len(files)} 个文件, "
              f"原始 {total / 1024**2:.1f} MB -> "
              f"压缩后 {bundle_path.stat().st_size / 1024**2:.1f} MB", flush=True)
        self.bundles.append(bundle_path)

    def finish(self):
        """全部处理完后调用:把剩余不足 1GB 的队列打包"""
        self.flush()
        print(f"[打包] 共生成 {len(self.bundles)} 个独立包", flush=True)
        return self.bundles


# ==================== 主流程 ====================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--urls", required=True)
    parser.add_argument("--output", default="output")
    parser.add_argument("--quality", type=int, default=80)
    parser.add_argument("--max-size", type=int, default=None)
    parser.add_argument("--compress-level", type=int, default=6)
    parser.add_argument("--download-workers", type=int, default=DEFAULT_DOWNLOAD_WORKERS)
    parser.add_argument("--convert-workers", type=int, default=max(1, os.cpu_count() or 2))
    parser.add_argument("--downloader", default="auto",
                        choices=["auto", "gallery-dl", "requests"])
    parser.add_argument("--min-request-interval", type=float, default=1.0)
    parser.add_argument("--bundle-size-mb", type=int, default=1024,
                        help="每个独立包大小上限(MB),默认 1024 = 1GB")
    parser.add_argument("--keep-originals", action="store_true",
                        help="保留原始单包(默认打包后删除)")
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
          f"下载器={args.downloader}, 最小间隔={args.min_request_interval}s, "
          f"打包上限={args.bundle_size_mb}MB", flush=True)

    # ===== 阶段 1:下载 =====
    print(f"\n=== 阶段 1:并发下载 {len(urls)} 个压缩包(用原始文件名) ===", flush=True)
    results = download_all(
        urls, downloads,
        workers=args.download_workers,
        downloader=args.downloader,
        min_interval=args.min_request_interval,
    )

    # ===== 阶段 2+3:流式处理 + 每 1GB 打包并删除 =====
    print(f"\n=== 阶段 2+3:解压 + 转 WebP + 重新打包, 每 {args.bundle_size_mb}MB 打一个独立包 ===",
          flush=True)

    bundler = StreamingBundler(
        out_root=out_root,
        max_bytes=args.bundle_size_mb * 1024**2,
        compress_level=args.compress_level,
        keep_originals=args.keep_originals,
    )

    ok = fail = 0
    for url, zip_path, err in results:
        if err or zip_path is None:
            fail += 1
            print(f"[跳过] {url} 下载失败", file=sys.stderr, flush=True)
            continue
        try:
            out_zip = process_one_zip(
                zip_path, out_root,
                quality=args.quality,
                max_size=args.max_size,
                convert_workers=args.convert_workers,
                compress_level=args.compress_level,
            )
            if not args.keep_originals:
                zip_path.unlink(missing_ok=True)
            # 交给打包器;满 1GB 会立刻打包并删单包
            bundler.add(out_zip)
            ok += 1
        except Exception as e:
            fail += 1
            print(f"[错误] 处理 {zip_path.name} 失败: {e}", file=sys.stderr, flush=True)

    # 处理剩余不足 1GB 的队列
    bundles = bundler.finish()

    if not args.keep_originals and downloads.exists() and not any(downloads.iterdir()):
        shutil.rmtree(downloads, ignore_errors=True)

    print(f"\n[全部完成] 成功 {ok}, 失败 {fail}, 独立包 {len(bundles)} 个", flush=True)
    if fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    mp.freeze_support()
    main()