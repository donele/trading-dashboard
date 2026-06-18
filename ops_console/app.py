from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import time
from urllib.parse import quote
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
DEFAULT_STATE_DIR = Path.home() / ".local" / "state" / "trading-dashboard" / "ops-console"


@dataclass
class EndpointConfig:
    name: str
    url: str
    note: str = ""


@dataclass
class ServiceConfig:
    name: str
    manager: str
    display_name: str
    note: str = ""
    unit: str = ""
    scope: str = "system"
    start_command: list[str] = field(default_factory=list)
    cwd: str = ""
    log_file: str = ""
    pid_file: str = ""
    env: dict[str, str] = field(default_factory=dict)


@dataclass
class ServiceStatus:
    name: str
    display_name: str
    manager: str
    active_state: str
    sub_state: str
    unit_file_state: str
    status_text: str
    detail: str = ""
    note: str = ""
    log_source: str = ""


@dataclass
class DashboardConfig:
    title: str
    refresh_seconds: int
    log_lines: int
    endpoints: list[EndpointConfig]
    services: list[ServiceConfig]


def expand_path(path: str) -> Path:
    return Path(path).expanduser().resolve()


def default_log_file(name: str) -> Path:
    return DEFAULT_STATE_DIR / "logs" / f"{name}.log"


def default_pid_file(name: str) -> Path:
    return DEFAULT_STATE_DIR / "run" / f"{name}.pid"


def tail_lines(path: Path, limit: int) -> str:
    if limit <= 0:
        return ""
    if not path.exists():
        return f"log file not found: {path}"
    with path.open("r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    return "".join(lines[-limit:])


def parse_systemctl_show(output: str) -> dict[str, str]:
    data: dict[str, str] = {}
    for line in output.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key] = value
    return data


class ServiceController:
    def __init__(self, config: DashboardConfig) -> None:
        self.config = config
        self.services = {service.name: service for service in config.services}

    def _require_service(self, name: str) -> ServiceConfig:
        service = self.services.get(name)
        if service is None:
            raise HTTPException(status_code=404, detail=f"unknown service: {name}")
        return service

    def _run(self, cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            env=env,
            capture_output=True,
            text=True,
        )

    def _systemctl_prefix(self, service: ServiceConfig) -> list[str]:
        if service.scope == "user":
            return ["systemctl", "--user"]
        return ["systemctl"]

    def _journalctl_prefix(self, service: ServiceConfig) -> list[str]:
        if service.scope == "user":
            return ["journalctl", "--user"]
        return ["journalctl"]

    def _process_pid_path(self, service: ServiceConfig) -> Path:
        pid_file = service.pid_file or str(default_pid_file(service.name))
        return expand_path(pid_file)

    def _process_log_path(self, service: ServiceConfig) -> Path:
        log_file = service.log_file or str(default_log_file(service.name))
        return expand_path(log_file)

    def _has_explicit_log_file(self, service: ServiceConfig) -> bool:
        return bool(service.log_file.strip())

    def _service_log_source(self, service: ServiceConfig) -> str:
        if self._has_explicit_log_file(service):
            return str(self._process_log_path(service))
        if service.manager == "systemd":
            return f"journalctl -u {service.unit}"
        if service.manager == "process":
            return str(self._process_log_path(service))
        return ""

    def _read_pid(self, path: Path) -> int | None:
        if not path.exists():
            return None
        try:
            return int(path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            return None

    def _pid_running(self, pid: int | None) -> bool:
        if pid is None or pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def _process_status(self, service: ServiceConfig) -> ServiceStatus:
        pid_path = self._process_pid_path(service)
        log_path = self._process_log_path(service)
        pid = self._read_pid(pid_path)
        if self._pid_running(pid):
            return ServiceStatus(
                name=service.name,
                display_name=service.display_name,
                manager=service.manager,
                active_state="active",
                sub_state="running",
                unit_file_state="n/a",
                status_text=f"pid {pid}",
                detail=str(pid_path),
                note=service.note,
                log_source=self._service_log_source(service),
            )
        if pid_path.exists():
            try:
                pid_path.unlink()
            except OSError:
                pass
        return ServiceStatus(
            name=service.name,
            display_name=service.display_name,
            manager=service.manager,
            active_state="inactive",
            sub_state="dead",
            unit_file_state="n/a",
            status_text="not running",
            detail=str(pid_path),
            note=service.note,
            log_source=self._service_log_source(service),
        )

    def _systemd_status(self, service: ServiceConfig) -> ServiceStatus:
        cmd = self._systemctl_prefix(service) + [
            "show",
            service.unit,
            "--property=ActiveState,SubState,UnitFileState,Description,LoadState",
            "--no-pager",
        ]
        proc = self._run(cmd)
        if proc.returncode != 0:
            stderr = proc.stderr.strip() or proc.stdout.strip() or "systemctl show failed"
            return ServiceStatus(
                name=service.name,
                display_name=service.display_name,
                manager=service.manager,
                active_state="unknown",
                sub_state="unknown",
                unit_file_state="unknown",
                status_text=stderr,
                detail=service.unit,
                note=service.note,
                log_source=self._service_log_source(service),
            )
        data = parse_systemctl_show(proc.stdout)
        return ServiceStatus(
            name=service.name,
            display_name=service.display_name,
            manager=service.manager,
            active_state=data.get("ActiveState", "unknown"),
            sub_state=data.get("SubState", "unknown"),
            unit_file_state=data.get("UnitFileState", "unknown"),
            status_text=data.get("Description", service.unit),
            detail=service.unit,
            note=service.note,
            log_source=self._service_log_source(service),
        )

    def status(self, service: ServiceConfig) -> ServiceStatus:
        if service.manager == "systemd":
            return self._systemd_status(service)
        if service.manager == "process":
            return self._process_status(service)
        return ServiceStatus(
            name=service.name,
            display_name=service.display_name,
            manager=service.manager,
            active_state="unknown",
            sub_state="unknown",
            unit_file_state="unknown",
            status_text=f"unsupported manager: {service.manager}",
            note=service.note,
        )

    def all_statuses(self) -> list[ServiceStatus]:
        return [self.status(service) for service in self.config.services]

    def logs(self, service: ServiceConfig, lines: int) -> str:
        if self._has_explicit_log_file(service):
            return tail_lines(self._process_log_path(service), lines)
        if service.manager == "systemd":
            cmd = self._journalctl_prefix(service) + [
                "-u",
                service.unit,
                "-n",
                str(lines),
                "--no-pager",
                "-o",
                "short-iso",
            ]
            proc = self._run(cmd)
            if proc.returncode != 0:
                return proc.stderr.strip() or proc.stdout.strip() or f"failed to read logs for {service.unit}"
            return proc.stdout
        if service.manager == "process":
            return tail_lines(self._process_log_path(service), lines)
        return f"unsupported manager: {service.manager}"

    def _process_action(self, service: ServiceConfig, action: str) -> str:
        pid_path = self._process_pid_path(service)
        log_path = self._process_log_path(service)
        cwd = expand_path(service.cwd) if service.cwd else Path.cwd()
        if action == "start":
            status = self._process_status(service)
            if status.active_state == "active":
                return f"{service.display_name} already running"
            if not service.start_command:
                raise HTTPException(status_code=400, detail=f"missing start_command for {service.name}")
            pid_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            env = os.environ.copy()
            env.update(service.env)
            with log_path.open("ab") as log_handle:
                proc = subprocess.Popen(
                    service.start_command,
                    cwd=str(cwd),
                    env=env,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            pid_path.write_text(f"{proc.pid}\n", encoding="utf-8")
            return f"started {service.display_name} (pid {proc.pid})"
        if action in {"stop", "restart"}:
            pid = self._read_pid(pid_path)
            if self._pid_running(pid):
                try:
                    os.killpg(pid, signal.SIGTERM)
                except OSError:
                    pass
                deadline = time.time() + 5.0
                while time.time() < deadline and self._pid_running(pid):
                    time.sleep(0.1)
                if self._pid_running(pid):
                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except OSError:
                        pass
                try:
                    pid_path.unlink()
                except OSError:
                    pass
            elif pid_path.exists():
                try:
                    pid_path.unlink()
                except OSError:
                    pass
            if action == "restart":
                return self._process_action(service, "start")
            return f"stopped {service.display_name}"
        raise HTTPException(status_code=400, detail=f"unsupported action: {action}")

    def action(self, service: ServiceConfig, action: str) -> str:
        if action not in {"start", "stop", "restart"}:
            raise HTTPException(status_code=400, detail=f"unsupported action: {action}")
        if service.manager == "systemd":
            cmd = self._systemctl_prefix(service) + [action, service.unit]
            proc = self._run(cmd)
            if proc.returncode != 0:
                stderr = proc.stderr.strip() or proc.stdout.strip() or f"systemctl {action} failed"
                raise HTTPException(status_code=500, detail=stderr)
            return f"{action} requested for {service.unit}"
        if service.manager == "process":
            return self._process_action(service, action)
        raise HTTPException(status_code=400, detail=f"unsupported manager: {service.manager}")


def load_dashboard_config(config_path: str | None) -> DashboardConfig:
    package_dir = Path(__file__).resolve().parent
    candidate_paths = []
    if config_path:
        candidate_paths.append(expand_path(config_path))
    env_path = os.getenv("OPS_CONSOLE_CONFIG")
    if env_path:
        candidate_paths.append(expand_path(env_path))
    candidate_paths.append(package_dir / "config.json")
    candidate_paths.append(package_dir / "config.example.json")

    for path in candidate_paths:
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8"))
            break
    else:
        raise FileNotFoundError("could not find ops_console config")

    endpoints = [
        EndpointConfig(
            name=str(item["name"]),
            url=str(item["url"]),
            note=str(item.get("note", "")),
        )
        for item in raw.get("endpoints", [])
    ]
    services = [
        ServiceConfig(
            name=str(item["name"]),
            manager=str(item["manager"]),
            display_name=str(item.get("display_name", item["name"])),
            note=str(item.get("note", "")),
            unit=str(item.get("unit", "")),
            scope=str(item.get("scope", "system")),
            start_command=[str(part) for part in item.get("start_command", [])],
            cwd=str(item.get("cwd", "")),
            log_file=str(item.get("log_file", "")),
            pid_file=str(item.get("pid_file", "")),
            env={str(k): str(v) for k, v in item.get("env", {}).items()},
        )
        for item in raw.get("services", [])
    ]
    return DashboardConfig(
        title=str(raw.get("title", "Trading Ops Console")),
        refresh_seconds=int(raw.get("refresh_seconds", 15)),
        log_lines=int(raw.get("log_lines", 80)),
        endpoints=endpoints,
        services=services,
    )


def create_app(config_path: str | None = None) -> FastAPI:
    config = load_dashboard_config(config_path)
    controller = ServiceController(config)
    templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))
    app = FastAPI(title=config.title)
    app.state.dashboard_config = config
    app.state.controller = controller

    @app.get("/")
    async def index(request: Request, lines: int | None = None) -> Any:
        line_count = lines if lines is not None else config.log_lines
        services = []
        for status in controller.all_statuses():
            service_cfg = controller._require_service(status.name)
            services.append(
                {
                    "status": status,
                    "logs": controller.logs(service_cfg, line_count),
                }
            )
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "title": config.title,
                "request": request,
                "endpoints": config.endpoints,
                "services": services,
                "log_lines": line_count,
                "refresh_seconds": config.refresh_seconds,
                "message": request.query_params.get("message", ""),
                "error": request.query_params.get("error", ""),
            },
        )

    @app.post("/services/{service_name}/{action}")
    async def service_action(service_name: str, action: str) -> RedirectResponse:
        service = controller._require_service(service_name)
        try:
            message = controller.action(service, action)
            return RedirectResponse(url=f"/?message={quote(message)}", status_code=303)
        except HTTPException as exc:
            return RedirectResponse(url=f"/?error={quote(str(exc.detail))}", status_code=303)

    @app.get("/logs/{service_name}", response_class=PlainTextResponse)
    async def service_logs(service_name: str, lines: int | None = None) -> str:
        service = controller._require_service(service_name)
        line_count = lines if lines is not None else config.log_lines
        return controller.logs(service, line_count)

    @app.get("/api/services")
    async def api_services() -> dict[str, Any]:
        return {
            "services": [status.__dict__ for status in controller.all_statuses()],
            "endpoints": [endpoint.__dict__ for endpoint in config.endpoints],
        }

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Operations console for market-data and dashboard services")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Bind host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Bind port")
    parser.add_argument("--config", default="", help="Path to ops_console JSON config")
    parser.add_argument("--reload", action="store_true", help="Enable uvicorn auto-reload")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.config:
        os.environ["OPS_CONSOLE_CONFIG"] = args.config
    if args.reload:
        uvicorn.run(
            "ops_console.app:create_app",
            host=args.host,
            port=args.port,
            reload=True,
            factory=True,
        )
    else:
        app = create_app(args.config or None)
        uvicorn.run(app, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
