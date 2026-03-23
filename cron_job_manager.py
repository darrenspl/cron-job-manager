#!/usr/bin/env python3
"""
Cron Job Manager — monitors scheduled task health and alerts on failures.

Cross-platform (Windows + Linux), stdlib-only (no pip dependencies).
Checks log files for recency and errors, optionally checks OS scheduler.
Emails via Mailgun SMTP on failure. Self-checks on boot.

Usage:
    python cron-job-manager.py              # Check all tasks, email on failure
    python cron-job-manager.py --verbose    # Print full report to stdout
    python cron-job-manager.py --dry-run    # Check but don't email
    python cron-job-manager.py --json       # Output JSON for machine consumption
"""

import csv
import json
import logging
import os
import platform
import re
import smtplib
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import StringIO
from pathlib import Path
from typing import Optional

# ─── Configuration ──────────────────────────────────────────────────────────

HOME = Path.home()
LOG_DIR = HOME / ".claude" / "logs"
LOG_FILE = LOG_DIR / "cron-job-manager.log"
BOOT_STAMP = HOME / ".local" / "bin" / ".cron-manager-booted"
MAX_LOG_LINES = 1000

TASKS = {
    "VaultKeeper": {
        "log": LOG_DIR / "vault-keeper.log",
        "script": HOME / ".claude" / "scripts" / "vault-keeper-loop.sh",
        "expected_interval_min": 15,
        "description": "Note processing & maturation",
    },
    "VaultLoop": {
        "log": LOG_DIR / "vault-loop.log",
        "script": HOME / ".claude" / "scripts" / "vault-loop.sh",
        "expected_interval_min": 1440,
        "description": "TaskMaster task execution",
    },
    "VaultSnapshot": {
        "log": None,  # Inline command, no log file produced
        "script": None,  # Inline command in Task Scheduler, no script file
        "expected_interval_min": 360,
        "description": "Git commit + push snapshots",
        "scheduler_only": True,  # Only check via Task Scheduler, not log files
    },
    "VaultCI": {
        "log": LOG_DIR / "vault-ci.log",
        "script": None,  # Inline command: uv run vault-ops ci
        "expected_interval_min": 1440,
        "description": "Full vault-ops CI pipeline",
    },
    "CronJobManager": {
        "log": LOG_FILE,
        "script": Path(__file__).resolve(),  # Use __file__ so it works from any location
        "expected_interval_min": 30,
        "description": "Cron job health monitor & alerting",
        "is_self": True,
    },
}

MAILGUN_SECRETS = HOME / ".claude" / "secrets" / "mailgun.json"
ALERT_TO = "darren@superpowerlabs.co"
STALE_MULTIPLIER = 3

ERROR_PATTERNS = [
    r"FATAL:",
    r"exit code:\s*(?!0\b)\d+",
    r"Exit code:\s*(?!0\b)\d+",
    r"exited with code (?!0\b)\d+",
    r"timed out after",
    r"WARN: Claude timed out",
    r"Last Result:\s*(?!0\b)\d+",
]

SUCCESS_PATTERNS = [
    r"exited successfully",
    r"exit code:\s*0\b",
    r"Exit code:\s*0\b",
    r"SESSION SUMMARY",
    r"All \d+ tasks healthy",
    r"SELF-CHECK PASSED",
]


# ─── File Logging ──────────────────────────────────────────────────────────

def setup_logging():
    """Set up file + console logging."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("cron-manager")
    logger.setLevel(logging.INFO)

    # File handler
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(fh)

    # Console handler (only for verbose/errors)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(message)s"))
    ch.setLevel(logging.WARNING)
    logger.addHandler(ch)

    return logger


def rotate_log():
    """Keep log file under MAX_LOG_LINES."""
    if not LOG_FILE.exists():
        return
    try:
        lines = LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
        if len(lines) > MAX_LOG_LINES:
            LOG_FILE.write_text("\n".join(lines[-MAX_LOG_LINES:]) + "\n", encoding="utf-8")
    except OSError:
        pass


# ─── Self-Health Check ─────────────────────────────────────────────────────

def check_self(log: logging.Logger) -> list[tuple[str, bool, str]]:
    """Run self-health checks before checking other tasks."""
    checks = []

    # 1. Script exists
    script = Path(__file__).resolve()
    checks.append(("self_script", script.exists(), str(script)))

    # 2. Mailgun secrets readable
    if MAILGUN_SECRETS.exists():
        try:
            with open(MAILGUN_SECRETS) as f:
                data = json.load(f)
            has_keys = all(k in data for k in ["MAILGUN_SMTP_SERVER", "MAILGUN_SMTP_USERNAME", "MAILGUN_SMTP_PASSWORD"])
            checks.append(("mailgun_secrets", has_keys, "All required keys present" if has_keys else "Missing required keys"))
        except Exception as e:
            checks.append(("mailgun_secrets", False, f"Cannot parse: {e}"))
    else:
        checks.append(("mailgun_secrets", False, f"File not found: {MAILGUN_SECRETS}"))

    # 3. Log directory exists
    checks.append(("log_dir", LOG_DIR.exists(), str(LOG_DIR)))

    # 4. Python stdlib imports
    try:
        import smtplib as _s, pathlib as _p, subprocess as _sp, csv as _c
        checks.append(("stdlib_imports", True, "All modules importable"))
    except ImportError as e:
        checks.append(("stdlib_imports", False, str(e)))

    all_passed = all(passed for _, passed, _ in checks)

    if all_passed:
        log.info("SELF-CHECK PASSED — all systems nominal")
    else:
        failures = [f"{n}: {d}" for n, passed, d in checks if not passed]
        log.warning(f"SELF-CHECK FAILED — {'; '.join(failures)}")

    # 5. First-boot detection
    if not BOOT_STAMP.exists():
        log.info("FIRST BOOT detected — sending alive notification")
        _send_boot_email()
        try:
            BOOT_STAMP.parent.mkdir(parents=True, exist_ok=True)
            BOOT_STAMP.write_text(f"Booted: {datetime.now().isoformat()}\n")
        except OSError:
            pass

    return checks


def _send_boot_email():
    """Send a 'cron-manager is alive' email on first boot."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    hostname = platform.node()
    task_list = ", ".join(TASKS.keys())

    plain = (
        f"Cron Job Manager started on {hostname} at {now}.\n\n"
        f"Platform: {platform.system()} {platform.release()}\n"
        f"Python: {sys.version.split()[0]}\n"
        f"Monitoring {len(TASKS)} tasks every 30 minutes.\n\n"
        f"Tasks: {task_list}\n"
    )

    task_pills = "".join(
        f'<span style="display:inline-block;padding:4px 12px;margin:3px;border-radius:16px;'
        f'background:#1e293b;color:#94a3b8;font-size:12px;font-weight:500;">{name}</span>'
        for name in TASKS.keys()
    )

    html = f'''<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#000;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<div style="max-width:520px;margin:0 auto;background:#0a0a0a;">
    <div style="background:#0d9488;padding:32px 24px;text-align:center;">
        <div style="font-size:42px;line-height:1;">&#128737;</div>
        <div style="font-size:20px;font-weight:700;color:#fff;margin-top:8px;">Cron Job Manager Online</div>
        <div style="font-size:13px;color:rgba(255,255,255,0.75);margin-top:4px;">{hostname} &mdash; {now}</div>
    </div>
    <div style="padding:24px;text-align:center;">
        <div style="font-size:13px;color:#9ca3af;margin-bottom:16px;">Monitoring {len(TASKS)} tasks every 30 minutes</div>
        <div>{task_pills}</div>
    </div>
    <div style="padding:0 24px 16px;">
        <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #262626;border-radius:8px;overflow:hidden;border-collapse:collapse;">
            <tr style="border-bottom:1px solid #1f1f1f;">
                <td style="padding:8px 12px;color:#6b7280;font-size:12px;">Platform</td>
                <td style="padding:8px 12px;color:#e5e7eb;font-size:12px;text-align:right;">{platform.system()} {platform.release()}</td>
            </tr>
            <tr style="border-bottom:1px solid #1f1f1f;">
                <td style="padding:8px 12px;color:#6b7280;font-size:12px;">Python</td>
                <td style="padding:8px 12px;color:#e5e7eb;font-size:12px;text-align:right;">{sys.version.split()[0]}</td>
            </tr>
            <tr>
                <td style="padding:8px 12px;color:#6b7280;font-size:12px;">Script</td>
                <td style="padding:8px 12px;color:#e5e7eb;font-size:12px;text-align:right;">{Path(__file__).resolve()}</td>
            </tr>
        </table>
    </div>
    <div style="padding:12px 24px;background:#111;border-top:1px solid #262626;text-align:center;">
        <div style="font-size:11px;color:#4b5563;">First boot notification &mdash; you will not receive this again unless the stamp file is deleted.</div>
    </div>
</div>
</body></html>'''

    _send_email(f"Cron Job Manager ONLINE — {hostname}", plain, html)


# ─── Health Check Logic ────────────────────────────────────────────────────

class TaskHealth:
    def __init__(self, name: str):
        self.name = name
        self.checks: list[tuple[str, bool, str]] = []

    def add(self, check: str, passed: bool, detail: str = ""):
        self.checks.append((check, passed, detail))

    @property
    def healthy(self) -> bool:
        return all(passed for _, passed, _ in self.checks)

    @property
    def failures(self) -> list[tuple[str, str]]:
        return [(name, detail) for name, passed, detail in self.checks if not passed]


def check_script_exists(config: dict) -> tuple[bool, str]:
    script = config["script"]
    if script is None:
        return True, "Inline command (no script file)"
    if script.exists():
        return True, str(script)
    return False, f"Script missing: {script}"


def check_log_exists(config: dict) -> tuple[bool, str]:
    log = config.get("log")
    if log is None:
        return True, "No log file expected (inline command)"
    if log.exists() and log.stat().st_size > 0:
        return True, f"{log.stat().st_size:,} bytes"
    if log.exists():
        return False, "Log file exists but is empty"
    return False, f"No log file: {log}"


def check_log_recency(config: dict) -> tuple[bool, str]:
    log = config.get("log")
    if log is None:
        return True, "No log file expected"
    if not log.exists():
        return False, "No log file"

    mtime = datetime.fromtimestamp(log.stat().st_mtime, tz=timezone.utc)
    now = datetime.now(tz=timezone.utc)
    age = now - mtime
    max_age = timedelta(minutes=config["expected_interval_min"] * STALE_MULTIPLIER)

    age_str = _format_timedelta(age)
    if age <= max_age:
        return True, f"Last modified {age_str} ago"
    return False, f"Stale: last modified {age_str} ago (expected within {_format_timedelta(max_age)})"


def check_last_session_health(config: dict) -> tuple[bool, str]:
    log = config.get("log")
    if log is None:
        return True, "No log file expected (scheduler-only check)"
    if not log.exists():
        return False, "No log file"

    try:
        size = log.stat().st_size
        read_bytes = min(size, 5120)
        with open(log, "r", encoding="utf-8", errors="replace") as f:
            if size > read_bytes:
                f.seek(size - read_bytes)
            tail = f.read()
    except (OSError, IOError) as e:
        return False, f"Cannot read log: {e}"

    lines = tail.strip().splitlines()
    if not lines:
        return False, "Log file is empty"

    last_lines = "\n".join(lines[-50:])

    for pattern in ERROR_PATTERNS:
        match = re.search(pattern, last_lines, re.IGNORECASE)
        if match:
            error_pos = match.start()
            after_error = last_lines[error_pos:]
            has_success_after = any(
                re.search(sp, after_error, re.IGNORECASE) for sp in SUCCESS_PATTERNS
            )
            if not has_success_after:
                for line in lines[-50:]:
                    if re.search(pattern, line, re.IGNORECASE):
                        return False, f"Error in last session: {line.strip()[:120]}"

    has_success = any(re.search(sp, last_lines, re.IGNORECASE) for sp in SUCCESS_PATTERNS)
    if has_success:
        return True, "Last session completed successfully"

    return True, "No errors detected (but no explicit success signal)"


def check_scheduler_status(task_name: str) -> Optional[tuple[bool, str]]:
    if platform.system() != "Windows":
        return None

    try:
        result = subprocess.run(
            ["schtasks", "/query", "/tn", task_name, "/fo", "CSV", "/v"],
            capture_output=True, text=True, timeout=10,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if result.returncode != 0:
            return False, "Task not registered in Task Scheduler"

        reader = csv.DictReader(StringIO(result.stdout))
        for row in reader:
            status = row.get("Status", "Unknown")
            last_result = row.get("Last Result", "Unknown")
            last_run = row.get("Last Run Time", "Unknown")

            if status == "Disabled":
                return False, "Task is DISABLED in Task Scheduler"

            if last_result not in ("0", "Unknown", "N/A", ""):
                return False, f"Last exit code: {last_result} (run: {last_run})"

            return True, f"Status: {status}, Last result: {last_result}"

    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None


def run_health_checks() -> list[TaskHealth]:
    results = []

    for task_name, config in TASKS.items():
        health = TaskHealth(task_name)
        is_self = config.get("is_self", False)

        # Check 1: Script exists
        passed, detail = check_script_exists(config)
        health.add("script_exists", passed, detail)

        # Check 2: Log exists (skip for self on first run — log hasn't been written yet)
        passed, detail = check_log_exists(config)
        health.add("log_exists", passed, detail)

        # Check 3: Log is recent (skip for self — we're writing it right now)
        if not is_self:
            passed, detail = check_log_recency(config)
            health.add("log_recent", passed, detail)

        # Check 4: Last session healthy (skip for self)
        if not is_self:
            passed, detail = check_last_session_health(config)
            health.add("last_session", passed, detail)

        # Check 5: OS scheduler (skip for self — circular)
        if not is_self:
            scheduler_result = check_scheduler_status(task_name)
            if scheduler_result is not None:
                passed, detail = scheduler_result
                health.add("scheduler", passed, detail)

        results.append(health)

    return results


# ─── Reporting ──────────────────────────────────────────────────────────────

def format_report(results: list[TaskHealth], self_checks: list) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"Cron Job Manager Health Report — {now}",
        "=" * 50,
        "",
    ]

    # Self-check section
    self_ok = all(passed for _, passed, _ in self_checks)
    lines.append(f"[{'OK' if self_ok else 'FAIL'}] Self-Check")
    for name, passed, detail in self_checks:
        mark = "  +" if passed else "  !"
        lines.append(f"{mark} {name}: {detail}")
    lines.append("")

    # Task checks
    all_healthy = all(r.healthy for r in results)
    lines.append(f"Overall: {'ALL HEALTHY' if all_healthy else 'FAILURES DETECTED'}")
    lines.append("")

    for result in results:
        desc = TASKS[result.name]["description"]
        status = "OK" if result.healthy else "FAIL"
        lines.append(f"[{status}] {result.name} — {desc}")
        for check_name, passed, detail in result.checks:
            mark = "  +" if passed else "  !"
            lines.append(f"{mark} {check_name}: {detail}")
        lines.append("")

    if not all_healthy:
        lines.append("FAILURES REQUIRING ATTENTION:")
        lines.append("-" * 30)
        for result in results:
            for check_name, detail in result.failures:
                lines.append(f"  {result.name}.{check_name}: {detail}")
        lines.append("")

    return "\n".join(lines)


def format_json(results: list[TaskHealth], self_checks: list) -> str:
    data = {
        "timestamp": datetime.now().isoformat(),
        "overall_healthy": all(r.healthy for r in results),
        "self_check": {name: {"passed": passed, "detail": detail} for name, passed, detail in self_checks},
        "tasks": {},
    }
    for result in results:
        data["tasks"][result.name] = {
            "healthy": result.healthy,
            "checks": {
                name: {"passed": passed, "detail": detail}
                for name, passed, detail in result.checks
            },
        }
    return json.dumps(data, indent=2)


def format_html_report(results: list[TaskHealth], self_checks: list) -> str:
    """Format health check results as a polished HTML email."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    hostname = platform.node()
    all_healthy = all(r.healthy for r in results)
    self_ok = all(passed for _, passed, _ in self_checks)
    healthy_count = sum(1 for r in results if r.healthy)
    total_count = len(results)

    # Status colors
    if all_healthy:
        banner_bg = "#0d9488"  # teal
        banner_icon = "&#10003;"  # checkmark
        banner_text = "ALL SYSTEMS HEALTHY"
    else:
        banner_bg = "#dc2626"  # red
        banner_icon = "&#9888;"  # warning
        banner_text = "FAILURES DETECTED"

    # Build task rows
    task_rows = ""
    for result in results:
        config = TASKS[result.name]
        desc = config["description"]
        interval = f'{config["expected_interval_min"]}m'
        if config["expected_interval_min"] >= 1440:
            interval = "Daily"
        elif config["expected_interval_min"] >= 60:
            interval = f'{config["expected_interval_min"] // 60}h'

        if result.healthy:
            status_badge = '<span style="display:inline-block;padding:2px 10px;border-radius:12px;font-size:12px;font-weight:600;background:#065f46;color:#6ee7b7;">PASS</span>'
            row_bg = "#0a0a0a"
        else:
            status_badge = '<span style="display:inline-block;padding:2px 10px;border-radius:12px;font-size:12px;font-weight:600;background:#991b1b;color:#fca5a5;">FAIL</span>'
            row_bg = "#1a0505"

        # Build check detail rows
        check_details = ""
        for check_name, passed, detail in result.checks:
            icon = '<span style="color:#34d399;">&#10003;</span>' if passed else '<span style="color:#f87171;">&#10007;</span>'
            detail_color = "#9ca3af" if passed else "#fca5a5"
            check_details += f'''
                <tr style="border-bottom:1px solid #1f1f1f;">
                    <td style="padding:4px 12px 4px 32px;color:#6b7280;font-size:12px;">{icon} {check_name}</td>
                    <td style="padding:4px 12px;color:{detail_color};font-size:12px;">{detail}</td>
                </tr>'''

        task_rows += f'''
            <tr style="background:{row_bg};border-bottom:1px solid #262626;">
                <td style="padding:12px;font-weight:600;color:#f3f4f6;">{result.name}</td>
                <td style="padding:12px;color:#9ca3af;font-size:13px;">{desc}</td>
                <td style="padding:12px;text-align:center;">{interval}</td>
                <td style="padding:12px;text-align:center;">{status_badge}</td>
            </tr>
            {check_details}'''

    # Build self-check rows
    self_rows = ""
    for name, passed, detail in self_checks:
        icon = '<span style="color:#34d399;">&#10003;</span>' if passed else '<span style="color:#f87171;">&#10007;</span>'
        detail_color = "#9ca3af" if passed else "#fca5a5"
        self_rows += f'''
            <tr style="border-bottom:1px solid #1f1f1f;">
                <td style="padding:4px 12px;color:#6b7280;font-size:12px;">{icon} {name}</td>
                <td style="padding:4px 12px;color:{detail_color};font-size:12px;">{detail}</td>
            </tr>'''

    # Build failure summary (only if failures exist)
    failure_section = ""
    if not all_healthy:
        failure_rows = ""
        for result in results:
            for check_name, detail in result.failures:
                failure_rows += f'''
                    <tr style="border-bottom:1px solid #2d1515;">
                        <td style="padding:8px 12px;color:#fca5a5;font-weight:500;">{result.name}</td>
                        <td style="padding:8px 12px;color:#9ca3af;">{check_name}</td>
                        <td style="padding:8px 12px;color:#f87171;font-size:13px;">{detail}</td>
                    </tr>'''
        failure_section = f'''
            <div style="margin-top:24px;border:1px solid #991b1b;border-radius:8px;overflow:hidden;">
                <div style="background:#450a0a;padding:12px 16px;font-weight:600;color:#fca5a5;font-size:14px;">
                    &#9888; Failures Requiring Attention
                </div>
                <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                    <tr style="background:#1a0505;border-bottom:1px solid #2d1515;">
                        <th style="padding:8px 12px;text-align:left;color:#6b7280;font-size:11px;text-transform:uppercase;font-weight:500;">Task</th>
                        <th style="padding:8px 12px;text-align:left;color:#6b7280;font-size:11px;text-transform:uppercase;font-weight:500;">Check</th>
                        <th style="padding:8px 12px;text-align:left;color:#6b7280;font-size:11px;text-transform:uppercase;font-weight:500;">Detail</th>
                    </tr>
                    {failure_rows}
                </table>
            </div>'''

    html = f'''<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#000000;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
    <div style="max-width:680px;margin:0 auto;background:#0a0a0a;">

        <!-- Banner -->
        <div style="background:{banner_bg};padding:24px 24px;text-align:center;">
            <div style="font-size:36px;line-height:1;">{banner_icon}</div>
            <div style="font-size:18px;font-weight:700;color:#ffffff;letter-spacing:1px;margin-top:8px;">{banner_text}</div>
            <div style="font-size:12px;color:rgba(255,255,255,0.7);margin-top:4px;">{hostname} &mdash; {now}</div>
        </div>

        <!-- Score bar -->
        <div style="padding:16px 24px;background:#111111;display:flex;border-bottom:1px solid #262626;">
            <table width="100%" cellpadding="0" cellspacing="0"><tr>
                <td style="text-align:center;padding:8px;">
                    <div style="font-size:28px;font-weight:700;color:{'#34d399' if all_healthy else '#f87171'};">{healthy_count}/{total_count}</div>
                    <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:1px;">Tasks Healthy</div>
                </td>
                <td style="text-align:center;padding:8px;">
                    <div style="font-size:28px;font-weight:700;color:{'#34d399' if self_ok else '#f87171'};">{'OK' if self_ok else 'FAIL'}</div>
                    <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:1px;">Self-Check</div>
                </td>
                <td style="text-align:center;padding:8px;">
                    <div style="font-size:28px;font-weight:700;color:#818cf8;">{platform.system()}</div>
                    <div style="font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:1px;">Platform</div>
                </td>
            </tr></table>
        </div>

        <!-- Task table -->
        <div style="padding:24px;">
            <div style="font-size:14px;font-weight:600;color:#e5e7eb;margin-bottom:12px;text-transform:uppercase;letter-spacing:1px;">Task Status</div>
            <div style="border:1px solid #262626;border-radius:8px;overflow:hidden;">
                <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                    <tr style="background:#111111;">
                        <th style="padding:10px 12px;text-align:left;color:#6b7280;font-size:11px;text-transform:uppercase;font-weight:500;letter-spacing:0.5px;">Task</th>
                        <th style="padding:10px 12px;text-align:left;color:#6b7280;font-size:11px;text-transform:uppercase;font-weight:500;letter-spacing:0.5px;">Description</th>
                        <th style="padding:10px 12px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;font-weight:500;letter-spacing:0.5px;">Interval</th>
                        <th style="padding:10px 12px;text-align:center;color:#6b7280;font-size:11px;text-transform:uppercase;font-weight:500;letter-spacing:0.5px;">Status</th>
                    </tr>
                    {task_rows}
                </table>
            </div>
        </div>

        {failure_section}

        <!-- Self-check details -->
        <div style="padding:0 24px 24px;">
            <div style="font-size:14px;font-weight:600;color:#e5e7eb;margin-bottom:12px;text-transform:uppercase;letter-spacing:1px;">Self-Check</div>
            <div style="border:1px solid #262626;border-radius:8px;overflow:hidden;">
                <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                    {self_rows}
                </table>
            </div>
        </div>

        <!-- Footer -->
        <div style="padding:16px 24px;background:#111111;border-top:1px solid #262626;text-align:center;">
            <div style="font-size:11px;color:#4b5563;">Cron Job Manager &mdash; {hostname} &mdash; Python {sys.version.split()[0]}</div>
            <div style="font-size:11px;color:#374151;margin-top:2px;">Runs every 30 min + on boot &mdash; {Path(__file__).resolve()}</div>
        </div>

    </div>
</body>
</html>'''
    return html


# ─── Email Alerting ────────────────────────────────────────────────────────

def load_mailgun_credentials() -> Optional[dict]:
    if not MAILGUN_SECRETS.exists():
        return None
    try:
        with open(MAILGUN_SECRETS) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _send_email(subject: str, plain_body: str, html_body: Optional[str] = None) -> bool:
    """Send an email via Mailgun SMTP. Supports HTML + plain text fallback."""
    creds = load_mailgun_credentials()
    if not creds:
        print("WARNING: Cannot send email — Mailgun secrets not found", file=sys.stderr)
        return False

    if html_body:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(plain_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))
    else:
        msg = MIMEText(plain_body, "plain", "utf-8")

    msg["Subject"] = subject
    msg["From"] = f"{creds.get('MAILGUN_FROM_NAME', 'Cron Job Manager')} <{creds['MAILGUN_FROM']}>"
    msg["To"] = ALERT_TO

    try:
        server = smtplib.SMTP(creds["MAILGUN_SMTP_SERVER"], int(creds["MAILGUN_SMTP_PORT"]))
        server.starttls()
        server.login(creds["MAILGUN_SMTP_USERNAME"], creds["MAILGUN_SMTP_PASSWORD"])
        server.sendmail(creds["MAILGUN_FROM"], [ALERT_TO], msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print(f"WARNING: Email failed: {e}", file=sys.stderr)
        return False


def send_report_email(report: str, all_healthy: bool, task_count: int,
                      results: list[TaskHealth] = None, self_checks: list = None) -> bool:
    """Send the full health report email every run with HTML formatting."""
    hostname = platform.node()
    status = "ALL HEALTHY" if all_healthy else "FAILURES DETECTED"
    subject = f"Cron Job Manager [{status}] — {hostname}"

    html_body = None
    if results is not None and self_checks is not None:
        html_body = format_html_report(results, self_checks)

    return _send_email(subject, report, html_body)


# ─── Helpers ────────────────────────────────────────────────────────────────

def _format_timedelta(td: timedelta) -> str:
    total_seconds = int(td.total_seconds())
    if total_seconds < 0:
        return "0s"
    if total_seconds < 60:
        return f"{total_seconds}s"
    minutes = total_seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    remaining_min = minutes % 60
    if hours < 24:
        return f"{hours}h {remaining_min}m" if remaining_min else f"{hours}h"
    days = hours // 24
    remaining_hours = hours % 24
    return f"{days}d {remaining_hours}h" if remaining_hours else f"{days}d"


# ─── Main ───────────────────────────────────────────────────────────────────

def main():
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    dry_run = "--dry-run" in sys.argv
    as_json = "--json" in sys.argv

    # Set up file logging
    log = setup_logging()
    rotate_log()

    log.info("=" * 50)
    log.info("CRON MANAGER RUN START")

    # Phase 1: Self-check (always runs first)
    self_checks = check_self(log)

    # Phase 2: Check all tasks
    results = run_health_checks()
    all_healthy = all(r.healthy for r in results)

    # Log summary
    healthy_count = sum(1 for r in results if r.healthy)
    total_count = len(results)
    if all_healthy:
        log.info(f"All {total_count} tasks healthy.")
    else:
        failing = [r.name for r in results if not r.healthy]
        log.warning(f"FAILURES: {', '.join(failing)} ({healthy_count}/{total_count} healthy)")

    # Output
    if as_json:
        print(format_json(results, self_checks))
        log.info("CRON MANAGER RUN END (json output)")
        sys.exit(0 if all_healthy else 1)

    report = format_report(results, self_checks)

    if verbose or not all_healthy:
        print(report)

    # Log full report on failure
    if not all_healthy:
        for line in report.splitlines():
            log.info(line)

    if all_healthy and not verbose:
        print(f"[{datetime.now().strftime('%H:%M')}] All {total_count} tasks healthy.")

    # Always email the full report (unless --dry-run)
    if not dry_run:
        sent = send_report_email(report, all_healthy, total_count, results, self_checks)
        if sent:
            log.info(f"Report email sent to {ALERT_TO}")
            print(f"Report email sent to {ALERT_TO}")
        else:
            log.warning("Failed to send report email")

    log.info("CRON MANAGER RUN END")
    sys.exit(0 if all_healthy else 1)


if __name__ == "__main__":
    main()
