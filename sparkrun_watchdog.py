#!/usr/bin/env python3

"""Supervise configured sparkrun recipes and restart failed runs."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


LOG = logging.getLogger("sparkrun-watchdog")
PLACEHOLDER_RE = re.compile(r"\{([a-z_]+)\}")


def parse_command_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be a JSON array of strings")
    return [str(item) for item in value]


def outcome_text(value: Optional[bool]) -> str:
    if value is None:
        return "skipped"
    return "true" if value else "false"


def expand_placeholders(command: list[str], values: dict[str, str]) -> list[str]:
    """Replace {field} tokens once, without expanding values recursively."""

    def replacer(match: re.Match[str]) -> str:
        key = match.group(1)
        return values[key] if key in values else match.group(0)

    return [PLACEHOLDER_RE.sub(replacer, part) for part in command]


@dataclass(frozen=True)
class RestartEvent:
    recipe: str
    run_id: Optional[str]
    cluster_id: Optional[str]
    reason: str
    trigger: str
    fatal_pattern: Optional[str]
    launch_fatal_pattern: Optional[str]
    runtime_seconds: Optional[float]
    stop_succeeded: Optional[bool]
    launch_succeeded: Optional[bool]
    timestamp: str

    def placeholder_values(self) -> dict[str, str]:
        runtime = (
            str(int(self.runtime_seconds))
            if self.runtime_seconds is not None
            else ""
        )
        return {
            "recipe": self.recipe,
            "run_id": self.run_id or "",
            "cluster_id": self.cluster_id or "",
            "reason": self.reason,
            "trigger": self.trigger,
            "fatal_pattern": self.fatal_pattern or "",
            "launch_fatal_pattern": self.launch_fatal_pattern or "",
            "runtime_seconds": runtime,
            "stop_succeeded": outcome_text(self.stop_succeeded),
            "launch_succeeded": outcome_text(self.launch_succeeded),
            "timestamp": self.timestamp,
        }

    def payload(self) -> dict[str, Any]:
        return {
            "event": "restart",
            "recipe": self.recipe,
            "run_id": self.run_id,
            "cluster_id": self.cluster_id,
            "reason": self.reason,
            "trigger": self.trigger,
            "fatal_pattern": self.fatal_pattern,
            "launch_fatal_pattern": self.launch_fatal_pattern,
            "runtime_seconds": (
                int(self.runtime_seconds)
                if self.runtime_seconds is not None
                else None
            ),
            "stop_succeeded": self.stop_succeeded,
            "launch_succeeded": self.launch_succeeded,
            "timestamp": self.timestamp,
        }

    def environ(self) -> dict[str, str]:
        values = self.placeholder_values()
        env = {
            f"SPARKRUN_WATCHDOG_{key.upper()}": value
            for key, value in values.items()
        }
        env["SPARKRUN_WATCHDOG_EVENT"] = "restart"
        env["SPARKRUN_WATCHDOG_EVENT_JSON"] = json.dumps(
            self.payload(),
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return env


@dataclass(frozen=True)
class ModelConfig:
    recipe: str
    launch_args: list[str]
    post_restart_command: Optional[list[str]] = None


@dataclass
class ModelState:
    config: ModelConfig
    run_id: Optional[str] = None
    cluster_id: Optional[str] = None
    log_process: Optional[subprocess.Popen[str]] = None
    state_lock: threading.Lock = field(default_factory=threading.Lock)
    restart_lock: threading.Lock = field(default_factory=threading.Lock)
    restart_pending: bool = False
    last_restart: Optional[float] = None
    launched_at: Optional[float] = None
    run_observed_at: Optional[float] = None
    launch_fatal_pattern: Optional[str] = None


class SparkRunWatchdog:
    """Reconcile configured recipes with the sparkrun control plane."""

    def __init__(self, config: dict[str, Any]):
        self.status_interval = float(config.get("status_interval_seconds", 10))
        self.restart_cooldown = float(config.get("restart_cooldown_seconds", 20))
        self.startup_grace = float(config.get("startup_grace_seconds", 60))
        self.log_tail_lines = int(config.get("log_tail_lines", 200))
        self.fatal_patterns = [str(pattern) for pattern in config.get("fatal_patterns", [])]
        self.post_restart_command = parse_command_list(
            config.get("post_restart_command", []),
            "post_restart_command",
        )
        self.post_restart_timeout = float(
            config.get("post_restart_timeout_seconds", 30)
        )
        if self.post_restart_timeout <= 0:
            raise ValueError("post_restart_timeout_seconds must be positive")

        self.models: dict[str, ModelState] = {}
        for model in config.get("models", []):
            post_restart_command = None
            if "post_restart_command" in model:
                post_restart_command = parse_command_list(
                    model["post_restart_command"],
                    f"models[{model.get('recipe', '?')}].post_restart_command",
                )
            model_config = ModelConfig(
                recipe=model["recipe"],
                launch_args=[str(arg) for arg in model.get("launch_args", [])],
                post_restart_command=post_restart_command,
            )
            self.models[model_config.recipe] = ModelState(config=model_config)

        self.shutdown_event = threading.Event()

    def command(
        self,
        args: list[str],
        *,
        capture_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        """Run an external command without invoking a shell."""
        LOG.debug("running command: %s", " ".join(args))
        return subprocess.run(
            args,
            text=True,
            capture_output=capture_output,
            check=False,
        )

    def get_status(self) -> Optional[dict[str, Any]]:
        """Return sparkrun status, or None when the status is unavailable."""
        try:
            result = self.command(
                ["sparkrun", "status", "--json"],
                capture_output=True,
            )
            if result.returncode != 0:
                LOG.error(
                    "sparkrun status failed with rc=%d: %s",
                    result.returncode,
                    result.stderr.strip(),
                )
                return None
            parsed = json.loads(result.stdout)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError as exc:
            LOG.error("invalid JSON from sparkrun status: %s", exc)
        except OSError:
            LOG.exception("failed to run sparkrun status")
        return None

    @staticmethod
    def run_id_from_cluster_id(cluster_id: Optional[str]) -> Optional[str]:
        if not cluster_id:
            return None
        prefix = "sparkrun_"
        return cluster_id[len(prefix) :] if cluster_id.startswith(prefix) else cluster_id

    @classmethod
    def _recipe_and_cluster_id(
        cls,
        entry: dict[str, Any],
        fallback_cluster_id: Optional[str] = None,
    ) -> tuple[Optional[str], Optional[str]]:
        meta = entry.get("meta", {})
        if not isinstance(meta, dict):
            meta = {}
        runtime_info = meta.get("runtime_info", {})
        if not isinstance(runtime_info, dict):
            runtime_info = {}

        recipe = meta.get("recipe") or runtime_info.get("container_sparkrun_recipe")
        cluster_id = entry.get("cluster_id") or meta.get("cluster_id") or fallback_cluster_id
        return recipe, cluster_id

    def discover_runs(self, status: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Extract recipe/run mappings from grouped and solo status entries."""
        found: dict[str, dict[str, Any]] = {}

        groups = status.get("groups", {})
        if isinstance(groups, dict):
            for group_key, entry in groups.items():
                if not isinstance(entry, dict):
                    continue
                recipe, cluster_id = self._recipe_and_cluster_id(entry, group_key)
                if recipe:
                    found[recipe] = {
                        "recipe": recipe,
                        "cluster_id": cluster_id,
                        "run_id": self.run_id_from_cluster_id(cluster_id),
                        "entry": entry,
                    }

        solo_entries = status.get("solo_entries", [])
        if isinstance(solo_entries, list):
            for entry in solo_entries:
                if not isinstance(entry, dict):
                    continue
                recipe, cluster_id = self._recipe_and_cluster_id(entry)
                if recipe:
                    found[recipe] = {
                        "recipe": recipe,
                        "cluster_id": cluster_id,
                        "run_id": self.run_id_from_cluster_id(cluster_id),
                        "entry": entry,
                    }

        return found

    def launch_model(self, state: ModelState) -> bool:
        recipe = state.config.recipe
        if self.shutdown_event.is_set():
            LOG.info("[%s] shutdown requested; not launching", recipe)
            return False
        with state.state_lock:
            state.launch_fatal_pattern = None
            state.launched_at = None

        # sparkrun follows workload logs by default. Disable that follow so
        # this watchdog can attach its own follower after status reports the
        # new run, otherwise launch output can race with the old follower.
        command = [
            "sparkrun",
            "run",
            recipe,
            *state.config.launch_args,
            "--no-follow",
        ]
        LOG.warning("[%s] launching: %s", recipe, " ".join(command))
        try:
            result = self.command(command, capture_output=True)
        except OSError:
            LOG.exception("[%s] launch command failed", recipe)
            return False

        matched = self._inspect_output(
            recipe,
            result.stdout or "",
            result.stderr or "",
        )
        with state.state_lock:
            state.launch_fatal_pattern = matched
        if matched:
            LOG.error(
                "[%s] fatal log signature detected during launch: %r",
                recipe,
                matched,
            )
            return False

        if result.returncode != 0:
            LOG.error("[%s] launch failed with rc=%d", recipe, result.returncode)
            return False
        with state.state_lock:
            now = time.monotonic()
            state.launched_at = now
            state.run_observed_at = now
        return True

    def stop_run(self, state: ModelState, run_id: str) -> bool:
        recipe = state.config.recipe
        LOG.warning("[%s] stopping run %s", recipe, run_id)
        try:
            result = self.command(["sparkrun", "stop", run_id])
        except OSError:
            LOG.exception("[%s] failed stopping run %s", recipe, run_id)
            return False
        if result.returncode != 0:
            LOG.error("[%s] sparkrun stop returned rc=%d", recipe, result.returncode)
            return False
        return True

    def _terminate_process(
        self,
        process: subprocess.Popen[str],
        recipe: str,
    ) -> None:
        try:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            LOG.exception("[%s] failed stopping log follower", recipe)
        finally:
            if process.stdout is not None:
                process.stdout.close()

    def stop_log_follower(self, state: ModelState) -> None:
        with state.state_lock:
            process = state.log_process
            state.log_process = None
        if process is None:
            return

        self._terminate_process(process, state.config.recipe)

    def start_log_follower(self, state: ModelState) -> None:
        with state.state_lock:
            if state.restart_pending or not state.run_id:
                return
            run_id = state.run_id
            existing_process = state.log_process
            if existing_process is not None and existing_process.poll() is None:
                return
            state.log_process = None

        if existing_process is not None:
            self._terminate_process(existing_process, state.config.recipe)

        recipe = state.config.recipe
        LOG.info("[%s] following logs for run %s", recipe, run_id)
        try:
            process = subprocess.Popen(
                [
                    "sparkrun",
                    "logs",
                    run_id,
                    "-n",
                    str(self.log_tail_lines),
                    "-f",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except OSError:
            LOG.exception("[%s] unable to start sparkrun logs", recipe)
            return

        with state.state_lock:
            can_attach = (
                not state.restart_pending
                and state.run_id == run_id
                and state.log_process is None
            )
            if can_attach:
                state.log_process = process

        if not can_attach:
            self._terminate_process(process, recipe)
            return

        thread = threading.Thread(
            target=self.log_reader,
            args=(state, process, run_id),
            daemon=True,
            name=f"log-{recipe}",
        )
        thread.start()

    def log_reader(
        self,
        state: ModelState,
        process: subprocess.Popen[str],
        run_id: str,
    ) -> None:
        recipe = state.config.recipe
        try:
            assert process.stdout is not None
            for raw_line in process.stdout:
                if self.shutdown_event.is_set():
                    break
                line = raw_line.rstrip()
                LOG.info("[%s] %s", recipe, line)
                matched = self._match_fatal_pattern(line)
                if matched:
                    LOG.error("[%s] fatal log signature detected: %r", recipe, matched)
                    self.request_restart(
                        state,
                        reason=f"log signature: {matched}",
                        trigger="fatal_log",
                        fatal_pattern=matched,
                        expected_run_id=run_id,
                        expected_log_process=process,
                    )
                    break
        except (OSError, ValueError):
            if not self.shutdown_event.is_set():
                LOG.exception("[%s] log follower failed", recipe)
        finally:
            try:
                return_code = process.wait(timeout=1)
            except (subprocess.TimeoutExpired, OSError):
                return_code = None
            LOG.info(
                "[%s] log follower ended for %s, rc=%s",
                recipe,
                run_id,
                return_code,
            )

    def _match_fatal_pattern(self, line: str) -> Optional[str]:
        return next(
            (pattern for pattern in self.fatal_patterns if pattern in line),
            None,
        )

    def _inspect_output(
        self,
        recipe: str,
        stdout: str,
        stderr: str,
    ) -> Optional[str]:
        """Forward and inspect captured stdout and stderr from a command."""
        for output in (stdout, stderr):
            for line in output.splitlines():
                LOG.info("[%s] %s", recipe, line)
                matched = self._match_fatal_pattern(line)
                if matched:
                    return matched
        return None

    def request_restart(
        self,
        state: ModelState,
        *,
        reason: str,
        trigger: str = "unspecified",
        fatal_pattern: Optional[str] = None,
        expected_run_id: Optional[str] = None,
        expected_log_process: Optional[subprocess.Popen[str]] = None,
    ) -> None:
        """Schedule a restart so a log-reader thread never waits on itself."""
        with state.state_lock:
            if self.shutdown_event.is_set():
                return
            if expected_run_id is not None and state.run_id != expected_run_id:
                LOG.info("[%s] ignoring stale restart request", state.config.recipe)
                return
            if (
                expected_log_process is not None
                and state.log_process is not expected_log_process
            ):
                LOG.info("[%s] ignoring stale log follower request", state.config.recipe)
                return
            if state.restart_pending:
                LOG.debug("[%s] restart already pending", state.config.recipe)
                return
            state.restart_pending = True

        threading.Thread(
            target=self.restart_model,
            kwargs={
                "state": state,
                "reason": reason,
                "trigger": trigger,
                "fatal_pattern": fatal_pattern,
                "expected_run_id": expected_run_id,
                "expected_log_process": expected_log_process,
            },
            daemon=True,
            name=f"restart-{state.config.recipe}",
        ).start()

    def post_restart_command_for(self, state: ModelState) -> list[str]:
        if state.config.post_restart_command is not None:
            return list(state.config.post_restart_command)
        return list(self.post_restart_command)

    def run_post_restart_action(
        self,
        state: ModelState,
        event: RestartEvent,
    ) -> None:
        template = self.post_restart_command_for(state)
        if not template:
            return

        command = expand_placeholders(template, event.placeholder_values())
        env = os.environ.copy()
        env.update(event.environ())
        LOG.info("[%s] post-restart action: %s", event.recipe, " ".join(command))
        try:
            result = subprocess.run(
                command,
                text=True,
                capture_output=True,
                check=False,
                env=env,
                timeout=self.post_restart_timeout,
            )
        except subprocess.TimeoutExpired:
            LOG.error(
                "[%s] post-restart action timed out after %.1fs",
                event.recipe,
                self.post_restart_timeout,
            )
            return
        except OSError:
            LOG.exception("[%s] post-restart action failed to start", event.recipe)
            return

        if result.stdout:
            LOG.info(
                "[%s] post-restart action stdout: %s",
                event.recipe,
                result.stdout.strip(),
            )
        if result.stderr:
            LOG.warning(
                "[%s] post-restart action stderr: %s",
                event.recipe,
                result.stderr.strip(),
            )
        if result.returncode != 0:
            LOG.error(
                "[%s] post-restart action exited rc=%d",
                event.recipe,
                result.returncode,
            )

    def restart_model(
        self,
        state: ModelState,
        *,
        reason: str,
        trigger: str = "unspecified",
        fatal_pattern: Optional[str] = None,
        expected_run_id: Optional[str] = None,
        expected_log_process: Optional[subprocess.Popen[str]] = None,
    ) -> None:
        recipe = state.config.recipe
        if not state.restart_lock.acquire(blocking=False):
            LOG.info("[%s] restart already in progress", recipe)
            return

        retry_reason: Optional[str] = None
        retry_pattern: Optional[str] = None
        try:
            now = time.monotonic()
            with state.state_lock:
                if (
                    state.last_restart is not None
                    and now - state.last_restart < self.restart_cooldown
                ):
                    LOG.warning("[%s] ignoring restart during cooldown: %s", recipe, reason)
                    return

                if expected_run_id is not None and state.run_id != expected_run_id:
                    LOG.warning("[%s] ignoring stale restart from old run %s", recipe, expected_run_id)
                    return

                if (
                    expected_log_process is not None
                    and state.log_process is not expected_log_process
                ):
                    LOG.warning("[%s] ignoring stale restart from an old log follower", recipe)
                    return

                state.last_restart = now
                old_run_id = state.run_id
                old_cluster_id = state.cluster_id
                observed_at = state.run_observed_at
                state.run_id = None
                state.cluster_id = None
                state.launched_at = None
                state.run_observed_at = None

            runtime_seconds = (
                now - observed_at if observed_at is not None else None
            )
            timestamp = datetime.now(timezone.utc).isoformat()
            stop_succeeded: Optional[bool] = None
            launch_succeeded: Optional[bool] = None
            launch_fatal_pattern: Optional[str] = None

            def emit_post_restart() -> None:
                self.run_post_restart_action(
                    state,
                    RestartEvent(
                        recipe=recipe,
                        run_id=old_run_id,
                        cluster_id=old_cluster_id,
                        reason=reason,
                        trigger=trigger,
                        fatal_pattern=fatal_pattern,
                        launch_fatal_pattern=launch_fatal_pattern,
                        runtime_seconds=runtime_seconds,
                        stop_succeeded=stop_succeeded,
                        launch_succeeded=launch_succeeded,
                        timestamp=timestamp,
                    ),
                )

            LOG.error("[%s] restarting because: %s", recipe, reason)

            # Stop following before stopping the workload to avoid matching our
            # own shutdown messages as a second failure.
            self.stop_log_follower(state)
            if old_run_id:
                stop_succeeded = self.stop_run(state, old_run_id)
                if not stop_succeeded:
                    LOG.error(
                        "[%s] restart deferred because the old run did not stop",
                        recipe,
                    )
                    emit_post_restart()
                    return
            if self.shutdown_event.is_set():
                LOG.info("[%s] shutdown requested; not launching replacement", recipe)
                emit_post_restart()
                return
            launch_succeeded = self.launch_model(state)
            with state.state_lock:
                launch_fatal_pattern = state.launch_fatal_pattern
                if launch_fatal_pattern:
                    retry_reason = (
                        "fatal log signature during launch: "
                        f"{launch_fatal_pattern}"
                    )
                    retry_pattern = launch_fatal_pattern
            emit_post_restart()
        finally:
            with state.state_lock:
                state.restart_pending = False
            state.restart_lock.release()

        if retry_reason:
            self._schedule_restart_after_cooldown(
                state,
                retry_reason,
                trigger="fatal_log",
                fatal_pattern=retry_pattern,
            )

    def _schedule_restart_after_cooldown(
        self,
        state: ModelState,
        reason: str,
        *,
        trigger: str,
        fatal_pattern: Optional[str] = None,
    ) -> None:
        with state.state_lock:
            last_restart = state.last_restart
        elapsed = (
            time.monotonic() - last_restart
            if last_restart is not None
            else self.restart_cooldown
        )
        delay = max(0.0, self.restart_cooldown - elapsed)

        LOG.warning(
            "[%s] fatal launch output observed; retrying after %.1fs cooldown",
            state.config.recipe,
            delay,
        )

        def request_retry() -> None:
            self.request_restart(
                state,
                reason=reason,
                trigger=trigger,
                fatal_pattern=fatal_pattern,
            )

        retry = threading.Timer(delay, request_retry)
        retry.daemon = True
        retry.start()

    def reconcile(self) -> None:
        status = self.get_status()
        if status is None:
            return

        discovered = self.discover_runs(status)
        for recipe, state in self.models.items():
            with state.state_lock:
                if state.restart_pending:
                    continue
                launched_at = state.launched_at

            current = discovered.get(recipe)
            if current is None:
                age = (
                    time.monotonic() - launched_at
                    if launched_at is not None
                    else None
                )
                if age is not None and age < self.startup_grace:
                    LOG.info(
                        "[%s] not visible yet, within startup grace (%.1fs / %.1fs)",
                        recipe,
                        age,
                        self.startup_grace,
                    )
                    continue
                self.request_restart(
                    state,
                    reason="recipe missing from sparkrun status",
                    trigger="missing",
                )
                continue

            new_run_id = current["run_id"]
            new_cluster_id = current["cluster_id"]
            with state.state_lock:
                if state.restart_pending:
                    continue
                previous_run_id = state.run_id
                run_changed = previous_run_id != new_run_id
                if run_changed:
                    state.run_id = new_run_id
                    state.cluster_id = new_cluster_id
                    state.launched_at = None
                    keep_launch_observation = (
                        previous_run_id is None
                        and state.run_observed_at is not None
                    )
                    if not keep_launch_observation:
                        state.run_observed_at = time.monotonic()
                follower_missing = (
                    state.log_process is None or state.log_process.poll() is not None
                )

            if run_changed:
                LOG.info("[%s] discovered run %s", recipe, new_run_id)
                self.stop_log_follower(state)
                self.start_log_follower(state)
            elif follower_missing:
                LOG.warning("[%s] log follower absent; reattaching", recipe)
                self.start_log_follower(state)

    def run(self) -> None:
        LOG.info("watching %d recipe(s)", len(self.models))
        while not self.shutdown_event.is_set():
            try:
                self.reconcile()
            except Exception:
                LOG.exception("unexpected reconciliation error")
            self.shutdown_event.wait(self.status_interval)

        LOG.info("watchdog shutting down")
        for state in self.models.values():
            self.stop_log_follower(state)


def load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)
    if not isinstance(config, dict):
        raise ValueError("configuration root must be a JSON object")
    return config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Supervise sparkrun recipes and restart crashed jobs"
    )
    parser.add_argument(
        "-c",
        "--config",
        default="/etc/sparkrun-watchdog.json",
        help="path to a JSON configuration file",
    )
    parser.add_argument("--debug", action="store_true", help="enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
    )
    watchdog = SparkRunWatchdog(load_config(args.config))

    def shutdown(signum: int, _frame: Any) -> None:
        LOG.info("received signal %s", signum)
        watchdog.shutdown_event.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    watchdog.run()


if __name__ == "__main__":
    main()
