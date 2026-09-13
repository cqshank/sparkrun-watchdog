#!/usr/bin/env python3

"""Supervise configured sparkrun recipes and restart failed runs."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional


LOG = logging.getLogger("sparkrun-watchdog")


@dataclass(frozen=True)
class ModelConfig:
    recipe: str
    launch_args: list[str]


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


class SparkRunWatchdog:
    """Reconcile configured recipes with the sparkrun control plane."""

    def __init__(self, config: dict[str, Any]):
        self.status_interval = float(config.get("status_interval_seconds", 10))
        self.restart_cooldown = float(config.get("restart_cooldown_seconds", 20))
        self.startup_grace = float(config.get("startup_grace_seconds", 60))
        self.log_tail_lines = int(config.get("log_tail_lines", 200))
        self.fatal_patterns = [str(pattern) for pattern in config.get("fatal_patterns", [])]

        self.models: dict[str, ModelState] = {}
        for model in config.get("models", []):
            model_config = ModelConfig(
                recipe=model["recipe"],
                launch_args=[str(arg) for arg in model.get("launch_args", [])],
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

        command = ["sparkrun", "run", recipe, *state.config.launch_args]
        LOG.warning("[%s] launching: %s", recipe, " ".join(command))
        try:
            result = self.command(command)
        except OSError:
            LOG.exception("[%s] launch command failed", recipe)
            return False

        if result.returncode != 0:
            LOG.error("[%s] launch failed with rc=%d", recipe, result.returncode)
            return False
        with state.state_lock:
            state.launched_at = time.monotonic()
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
                matched = next(
                    (pattern for pattern in self.fatal_patterns if pattern in line),
                    None,
                )
                if matched:
                    LOG.error("[%s] fatal log signature detected: %r", recipe, matched)
                    self.request_restart(
                        state,
                        reason=f"log signature: {matched}",
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

    def request_restart(
        self,
        state: ModelState,
        *,
        reason: str,
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
                "expected_run_id": expected_run_id,
                "expected_log_process": expected_log_process,
            },
            daemon=True,
            name=f"restart-{state.config.recipe}",
        ).start()

    def restart_model(
        self,
        state: ModelState,
        *,
        reason: str,
        expected_run_id: Optional[str] = None,
        expected_log_process: Optional[subprocess.Popen[str]] = None,
    ) -> None:
        recipe = state.config.recipe
        if not state.restart_lock.acquire(blocking=False):
            LOG.info("[%s] restart already in progress", recipe)
            return

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
                state.run_id = None
                state.cluster_id = None
                state.launched_at = None

            LOG.error("[%s] restarting because: %s", recipe, reason)

            # Stop following before stopping the workload to avoid matching our
            # own shutdown messages as a second failure.
            self.stop_log_follower(state)
            if old_run_id and not self.stop_run(state, old_run_id):
                LOG.error("[%s] restart deferred because the old run did not stop", recipe)
                return
            if self.shutdown_event.is_set():
                LOG.info("[%s] shutdown requested; not launching replacement", recipe)
                return
            self.launch_model(state)
        finally:
            with state.state_lock:
                state.restart_pending = False
            state.restart_lock.release()

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
                self.request_restart(state, reason="recipe missing from sparkrun status")
                continue

            new_run_id = current["run_id"]
            new_cluster_id = current["cluster_id"]
            with state.state_lock:
                if state.restart_pending:
                    continue
                run_changed = state.run_id != new_run_id
                if run_changed:
                    state.run_id = new_run_id
                    state.cluster_id = new_cluster_id
                    state.launched_at = None
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
