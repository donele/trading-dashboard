#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


DEFAULT_REPO_DIR = str(Path.home() / "repos" / "sgt")
DEFAULT_SITE_DIR = str(Path.home() / "repos" / "sgt" / "site")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build MkDocs site and serve rendered docs")
    parser.add_argument("--repo-dir", default=DEFAULT_REPO_DIR, help="Repository containing mkdocs.yml")
    parser.add_argument("--site-dir", default=DEFAULT_SITE_DIR, help="Output site directory")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=8010, help="Bind port")
    parser.add_argument(
        "--internet",
        action="store_true",
        help="Bind to 0.0.0.0 so docs are reachable over the network/internet",
    )
    return parser.parse_args()


def _run(cmd: list[str], *, cwd: Path | None = None) -> None:
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True)


def main() -> int:
    args = parse_args()
    repo_dir = Path(args.repo_dir).expanduser().resolve()
    site_dir = Path(args.site_dir).expanduser().resolve()
    host = "0.0.0.0" if args.internet else args.host

    if not repo_dir.is_dir():
        raise SystemExit(f"repo directory does not exist or is not a directory: {repo_dir}")
    if not (repo_dir / "mkdocs.yml").is_file():
        raise SystemExit(f"mkdocs.yml not found in repo directory: {repo_dir}")

    os.makedirs(site_dir, exist_ok=True)

    try:
        _run([sys.executable, "-c", "import mkdocs"])
    except subprocess.CalledProcessError:
        req = repo_dir / "docs" / "requirements.txt"
        if not req.is_file():
            raise SystemExit(f"mkdocs is missing and requirements file not found: {req}")
        _run([sys.executable, "-m", "pip", "install", "-r", str(req)])

    print(f"Building docs into: {site_dir}", file=sys.stderr)
    _run(["mkdocs", "build", "--clean", "--site-dir", str(site_dir)], cwd=repo_dir)

    handler = partial(SimpleHTTPRequestHandler, directory=str(site_dir))
    server = ThreadingHTTPServer((host, args.port), handler)
    print(f"Serving rendered docs from: {site_dir}", file=sys.stderr)
    print(f"Listening on: http://{host}:{args.port}/", file=sys.stderr)
    if args.internet:
        print(f"Internet mode enabled (--internet). Ensure firewall/NAT allows port {args.port}.", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
