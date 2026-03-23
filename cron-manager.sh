#!/usr/bin/env bash
# cron-manager.sh — Manage Windows Scheduled Tasks for vault automation
# Wraps PowerShell ScheduledTask commands for Git Bash / WSL use.
# Cross-platform: Git Bash (Windows) primary, WSL/Linux stub-compatible.

set -euo pipefail

VAULT_DIR="A:/obsidian"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ─── Managed Tasks ─────────────────────────────────────────────────────────
# Maps task names to their script files
declare -A TASK_SCRIPTS=(
    [VaultKeeper]="vault-keeper-loop.sh"
    [VaultLoop]="vault-loop.sh"
    [VaultSnapshot]="vault-snapshot.sh"
    [VaultCI]="vault-ci.sh"
    [CronJobManager]="cron_job_manager.py"
)

# Tasks that use python instead of bash
declare -A TASK_EXECUTORS=(
    [CronJobManager]="python"
)

# ─── Helpers ───────────────────────────────────────────────────────────────
die() { echo "ERROR: $1" >&2; exit 1; }

usage() {
    cat <<'EOF'
cron-manager.sh — Manage vault scheduled tasks

Usage:
  cron-manager.sh status                    Show all vault tasks + intervals
  cron-manager.sh set <task> <interval>     Set task interval
  cron-manager.sh downshift <task>          Switch to daily at midnight
  cron-manager.sh upshift <task>            Restore saved active interval
  cron-manager.sh list                      List manageable task names

Intervals: 5m, 10m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, daily

Managed tasks: VaultKeeper, VaultLoop, VaultSnapshot, VaultCI
EOF
    exit 0
}

# Parse interval string to minutes (or "daily")
parse_interval() {
    local input="$1"
    case "$input" in
        *m) echo "${input%m}" ;;
        *h) echo $(( ${input%h} * 60 )) ;;
        daily) echo "daily" ;;
        *) die "Invalid interval: $input (use 5m, 15m, 1h, 6h, daily, etc.)" ;;
    esac
}

# Get the script path for a task
get_script_path() {
    local task="$1"
    local script="${TASK_SCRIPTS[$task]:-}"
    [[ -z "$script" ]] && die "Unknown task: $task. Use: ${!TASK_SCRIPTS[*]}"
    local executor="${TASK_EXECUTORS[$task]:-bash}"
    if [[ "$executor" == "python" ]]; then
        echo "A:/src/cron-job-manager/${script}"
    else
        echo "${SCRIPT_DIR}/${script}"
    fi
}

# Get the executable for a task (bash.exe or python)
get_task_executor() {
    local task="$1"
    local executor="${TASK_EXECUTORS[$task]:-bash}"
    if [[ "$executor" == "python" ]]; then
        echo "python"
    else
        echo "C:\\Program Files\\Git\\bin\\bash.exe"
    fi
}

# Get the argument format for a task
get_task_argument() {
    local task="$1"
    local script_path="$2"
    local executor="${TASK_EXECUTORS[$task]:-bash}"
    if [[ "$executor" == "python" ]]; then
        echo "\"${script_path}\" --verbose"
    else
        echo "-l -c \"\$HOME/.claude/scripts/${TASK_SCRIPTS[$task]}\""
    fi
}

# ─── Commands ──────────────────────────────────────────────────────────────

cmd_status() {
    echo "Vault Scheduled Tasks"
    echo "====================="
    echo ""
    powershell.exe -NoProfile -Command "
        \$tasks = @('VaultKeeper', 'VaultLoop', 'VaultSnapshot', 'VaultCI', 'CronJobManager')
        foreach (\$name in \$tasks) {
            \$task = Get-ScheduledTask -TaskName \$name -ErrorAction SilentlyContinue
            if (\$task) {
                \$info = Get-ScheduledTaskInfo -TaskName \$name -ErrorAction SilentlyContinue
                \$state = \$task.State
                \$desc = \$task.Description
                \$trigger = \$task.Triggers[0]
                \$interval = ''
                if (\$trigger.Repetition.Interval) {
                    \$interval = \$trigger.Repetition.Interval
                } elseif (\$trigger.CimClass.CimClassName -eq 'MSFT_TaskDailyTrigger') {
                    \$interval = 'Daily'
                }
                \$lastRun = if (\$info.LastRunTime -and \$info.LastRunTime -ne [DateTime]::MinValue) { \$info.LastRunTime.ToString('yyyy-MM-dd HH:mm') } else { 'Never' }
                \$nextRun = if (\$info.NextRunTime -and \$info.NextRunTime -ne [DateTime]::MinValue) { \$info.NextRunTime.ToString('yyyy-MM-dd HH:mm') } else { 'N/A' }
                Write-Host \"  \$name\"
                Write-Host \"    State:    \$state\"
                Write-Host \"    Interval: \$interval\"
                Write-Host \"    Last run: \$lastRun\"
                Write-Host \"    Next run: \$nextRun\"
                Write-Host \"    Desc:     \$desc\"
                Write-Host ''
            } else {
                Write-Host \"  \$name — NOT REGISTERED\"
                Write-Host ''
            }
        }
    " 2>/dev/null

    # Show signal file status
    echo "Signal Files"
    echo "============"
    if [[ -f "${VAULT_DIR}/.vault-keeper-idle" ]]; then
        echo "  .vault-keeper-idle: EXISTS ($(cat "${VAULT_DIR}/.vault-keeper-idle"))"
    else
        echo "  .vault-keeper-idle: not present (keeper is active)"
    fi
    if [[ -f "${VAULT_DIR}/.vault-keeper-active-interval" ]]; then
        echo "  .vault-keeper-active-interval: $(cat "${VAULT_DIR}/.vault-keeper-active-interval") min (saved for upshift)"
    else
        echo "  .vault-keeper-active-interval: not present"
    fi
}

cmd_set() {
    local task="${1:-}"
    local interval_raw="${2:-}"
    [[ -z "$task" ]] && die "Usage: cron-manager.sh set <task> <interval>"
    [[ -z "$interval_raw" ]] && die "Usage: cron-manager.sh set <task> <interval>"

    local script_path executor task_arg
    script_path=$(get_script_path "$task")
    executor=$(get_task_executor "$task")
    task_arg=$(get_task_argument "$task" "$script_path")
    local interval
    interval=$(parse_interval "$interval_raw")

    echo "Setting $task to $interval_raw..."

    if [[ "$interval" == "daily" ]]; then
        powershell.exe -NoProfile -Command "
            \$action = New-ScheduledTaskAction -Execute '${executor}' -Argument '${task_arg}'
            \$trigger = New-ScheduledTaskTrigger -Daily -At '00:00'
            \$settings = New-ScheduledTaskSettingsSet -DontStopOnIdleEnd -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Minutes 20) -MultipleInstances IgnoreNew -StartWhenAvailable
            Register-ScheduledTask -TaskName '${task}' -Action \$action -Trigger \$trigger -Settings \$settings -Description '${task} — daily at midnight' -Force | Out-Null
            Write-Host 'Done: ${task} set to daily at midnight'
        " 2>/dev/null
    else
        powershell.exe -NoProfile -Command "
            \$action = New-ScheduledTaskAction -Execute '${executor}' -Argument '${task_arg}'
            \$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes ${interval}) -RepetitionDuration (New-TimeSpan -Days 9999)
            \$settings = New-ScheduledTaskSettingsSet -DontStopOnIdleEnd -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Minutes 20) -MultipleInstances IgnoreNew -StartWhenAvailable
            Register-ScheduledTask -TaskName '${task}' -Action \$action -Trigger \$trigger -Settings \$settings -Description '${task} — every ${interval_raw}' -Force | Out-Null
            Write-Host 'Done: ${task} set to every ${interval_raw}'
        " 2>/dev/null
    fi

    # Clear signal files when manually setting
    rm -f "${VAULT_DIR}/.vault-keeper-idle" "${VAULT_DIR}/.vault-keeper-active-interval" 2>/dev/null || true
    echo "Signal files cleared."
}

cmd_downshift() {
    local task="${1:-}"
    [[ -z "$task" ]] && die "Usage: cron-manager.sh downshift <task>"

    local script_path
    script_path=$(get_script_path "$task")

    # Save current interval for later restoration (default 15 min)
    local current_interval="15"
    if [[ -f "${VAULT_DIR}/.vault-keeper-active-interval" ]]; then
        current_interval=$(cat "${VAULT_DIR}/.vault-keeper-active-interval")
    fi
    echo "$current_interval" > "${VAULT_DIR}/.vault-keeper-active-interval"

    echo "Downshifting $task to daily at midnight (saved interval: ${current_interval}m)..."

    powershell.exe -NoProfile -Command "
        \$action = New-ScheduledTaskAction -Execute 'C:\Program Files\Git\bin\bash.exe' -Argument '-l -c \"\$HOME/.claude/scripts/${TASK_SCRIPTS[$task]}\"'
        \$trigger = New-ScheduledTaskTrigger -Daily -At '00:00'
        \$settings = New-ScheduledTaskSettingsSet -DontStopOnIdleEnd -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Minutes 20) -MultipleInstances IgnoreNew -StartWhenAvailable
        Register-ScheduledTask -TaskName '${task}' -Action \$action -Trigger \$trigger -Settings \$settings -Description '${task} — daily at midnight (downshifted from every ${current_interval}m)' -Force | Out-Null
        Write-Host 'Done: ${task} downshifted to daily'
    " 2>/dev/null
}

cmd_upshift() {
    local task="${1:-}"
    [[ -z "$task" ]] && die "Usage: cron-manager.sh upshift <task>"

    local script_path
    script_path=$(get_script_path "$task")

    # Read saved interval
    local interval="15"
    if [[ -f "${VAULT_DIR}/.vault-keeper-active-interval" ]]; then
        interval=$(cat "${VAULT_DIR}/.vault-keeper-active-interval")
        rm -f "${VAULT_DIR}/.vault-keeper-active-interval"
    fi

    # Clear idle signal
    rm -f "${VAULT_DIR}/.vault-keeper-idle" 2>/dev/null || true

    echo "Upshifting $task back to every ${interval} minutes..."

    powershell.exe -NoProfile -Command "
        \$action = New-ScheduledTaskAction -Execute 'C:\Program Files\Git\bin\bash.exe' -Argument '-l -c \"\$HOME/.claude/scripts/${TASK_SCRIPTS[$task]}\"'
        \$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes ${interval}) -RepetitionDuration ([TimeSpan]::MaxValue)
        \$settings = New-ScheduledTaskSettingsSet -DontStopOnIdleEnd -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Minutes 20) -MultipleInstances IgnoreNew -StartWhenAvailable
        Register-ScheduledTask -TaskName '${task}' -Action \$action -Trigger \$trigger -Settings \$settings -Description '${task} — every ${interval}m (upshifted)' -Force | Out-Null
        Write-Host 'Done: ${task} upshifted to every ${interval}m'
    " 2>/dev/null
}

cmd_list() {
    echo "Manageable tasks:"
    for task in "${!TASK_SCRIPTS[@]}"; do
        echo "  $task → ${TASK_SCRIPTS[$task]}"
    done
}

# ─── Main ──────────────────────────────────────────────────────────────────
command="${1:-status}"
shift 2>/dev/null || true

case "$command" in
    status)    cmd_status ;;
    set)       cmd_set "$@" ;;
    downshift) cmd_downshift "$@" ;;
    upshift)   cmd_upshift "$@" ;;
    list)      cmd_list ;;
    help|-h|--help) usage ;;
    *)         die "Unknown command: $command. Try: status, set, downshift, upshift, list" ;;
esac
