import json
import subprocess
import time
import unittest
from unittest.mock import Mock, patch

from sparkrun_watchdog import (
    RestartEvent,
    SparkRunWatchdog,
    expand_placeholders,
    load_config,
)


class SparkRunWatchdogTests(unittest.TestCase):
    def make_watchdog(self):
        return SparkRunWatchdog(
            {
                "status_interval_seconds": 10,
                "restart_cooldown_seconds": 20,
                "startup_grace_seconds": 60,
                "fatal_patterns": [
                    "EngineCore encountered a fatal error.",
                    "EngineCore failed to start.",
                ],
                "models": [
                    {
                        "recipe": "@namespace/model",
                        "launch_args": ["--tp", "2"],
                    }
                ],
            }
        )

    def test_run_id_strips_sparkrun_prefix(self):
        self.assertEqual(
            SparkRunWatchdog.run_id_from_cluster_id("sparkrun_abc123"),
            "abc123",
        )
        self.assertEqual(
            SparkRunWatchdog.run_id_from_cluster_id("abc123"),
            "abc123",
        )
        self.assertIsNone(SparkRunWatchdog.run_id_from_cluster_id(None))

    def test_discover_runs_supports_grouped_and_solo_entries(self):
        watchdog = self.make_watchdog()
        status = {
            "groups": {
                "sparkrun_grouped": {
                    "meta": {
                        "recipe": "@namespace/grouped",
                    }
                }
            },
            "solo_entries": [
                {
                    "cluster_id": "sparkrun_solo",
                    "meta": {
                        "runtime_info": {
                            "container_sparkrun_recipe": "@namespace/solo"
                        }
                    },
                }
            ],
        }

        found = watchdog.discover_runs(status)

        self.assertEqual(found["@namespace/grouped"]["run_id"], "grouped")
        self.assertEqual(found["@namespace/solo"]["run_id"], "solo")

    def test_multiple_models_are_tracked_independently(self):
        watchdog = SparkRunWatchdog(
            {
                "models": [
                    {"recipe": "@namespace/first", "launch_args": []},
                    {"recipe": "@namespace/second", "launch_args": ["--tp", "2"]},
                ]
            }
        )

        self.assertEqual(
            set(watchdog.models),
            {"@namespace/first", "@namespace/second"},
        )
        self.assertIsNot(
            watchdog.models["@namespace/first"],
            watchdog.models["@namespace/second"],
        )

    def test_launch_model_preserves_recipe_arguments(self):
        watchdog = self.make_watchdog()
        state = watchdog.models["@namespace/model"]
        completed = subprocess.CompletedProcess([], 0)

        with patch.object(watchdog, "command", return_value=completed) as command:
            self.assertTrue(watchdog.launch_model(state))

        command.assert_called_once_with(
            [
                "sparkrun",
                "run",
                "@namespace/model",
                "--tp",
                "2",
                "--no-follow",
            ],
            capture_output=True,
        )
        self.assertIsNotNone(state.launched_at)
        self.assertEqual(state.run_observed_at, state.launched_at)

    def test_launch_model_matches_fatal_patterns_on_both_streams(self):
        watchdog = self.make_watchdog()
        state = watchdog.models["@namespace/model"]

        for stream, pattern in (
            ("stdout", "EngineCore encountered a fatal error."),
            ("stderr", "EngineCore failed to start."),
        ):
            with self.subTest(stream=stream), patch.object(
                watchdog,
                "command",
                return_value=subprocess.CompletedProcess(
                    ["sparkrun"],
                    0,
                    stdout=(
                        pattern + "\n"
                        if stream == "stdout"
                        else "normal output\n"
                    ),
                    stderr=pattern + "\n" if stream == "stderr" else "",
                ),
            ):
                self.assertFalse(watchdog.launch_model(state))

            self.assertEqual(state.launch_fatal_pattern, pattern)
            self.assertIsNone(state.launched_at)

    @patch("sparkrun_watchdog.threading.Timer")
    def test_fatal_launch_output_retries_after_cooldown(self, timer):
        watchdog = self.make_watchdog()
        state = watchdog.models["@namespace/model"]
        state.run_id = "old-run"

        with patch.object(
            watchdog,
            "command",
            side_effect=[
                subprocess.CompletedProcess(["sparkrun"], 0),
                subprocess.CompletedProcess(
                    ["sparkrun"],
                    0,
                    stdout="EngineCore encountered a fatal error.\n",
                    stderr="",
                ),
            ],
        ):
            watchdog.restart_model(
                state,
                reason="test failure",
                expected_run_id="old-run",
            )

        timer.assert_called_once()
        self.assertGreaterEqual(timer.call_args.args[0], 0)
        timer.return_value.start.assert_called_once_with()
        self.assertFalse(state.restart_pending)

    def test_shutdown_does_not_launch_new_work(self):
        watchdog = self.make_watchdog()
        watchdog.shutdown_event.set()
        state = watchdog.models["@namespace/model"]

        with patch.object(watchdog, "command") as command:
            self.assertFalse(watchdog.launch_model(state))

        command.assert_not_called()

    def test_failed_stop_does_not_launch_a_replacement(self):
        watchdog = self.make_watchdog()
        state = watchdog.models["@namespace/model"]
        state.run_id = "old-run"

        with patch.object(
            watchdog,
            "command",
            return_value=subprocess.CompletedProcess(["sparkrun"], 1),
        ), patch.object(watchdog, "launch_model") as launch:
            watchdog.restart_model(
                state,
                reason="test failure",
                expected_run_id="old-run",
            )

        launch.assert_not_called()
        self.assertFalse(state.restart_pending)

    def test_status_errors_are_fail_safe(self):
        watchdog = self.make_watchdog()
        watchdog.command = Mock(
            return_value=subprocess.CompletedProcess(
                ["sparkrun"],
                1,
                stdout="",
                stderr="temporarily unavailable",
            )
        )
        self.assertIsNone(watchdog.get_status())

    @patch("sparkrun_watchdog.threading.Thread")
    @patch("sparkrun_watchdog.subprocess.Popen")
    def test_log_follower_uses_tail_and_follow_mode(self, popen, thread):
        watchdog = self.make_watchdog()
        state = watchdog.models["@namespace/model"]
        state.run_id = "run-123"

        process = Mock()
        process.poll.return_value = None
        process.stdout = Mock()
        popen.return_value = process

        watchdog.start_log_follower(state)

        popen.assert_called_once_with(
            ["sparkrun", "logs", "run-123", "-n", "200", "-f"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        thread.return_value.start.assert_called_once_with()

    @patch("sparkrun_watchdog.threading.Thread")
    def test_restart_requests_are_coalesced(self, thread):
        watchdog = self.make_watchdog()
        state = watchdog.models["@namespace/model"]

        watchdog.request_restart(state, reason="first failure")
        watchdog.request_restart(state, reason="second failure")

        thread.assert_called_once()
        self.assertTrue(state.restart_pending)

    @patch("sparkrun_watchdog.threading.Thread")
    def test_stale_log_follower_cannot_request_restart(self, thread):
        watchdog = self.make_watchdog()
        state = watchdog.models["@namespace/model"]
        state.run_id = "new-run"
        state.log_process = Mock()

        watchdog.request_restart(
            state,
            reason="old follower failure",
            expected_run_id="old-run",
            expected_log_process=Mock(),
        )

        thread.assert_not_called()
        self.assertFalse(state.restart_pending)

    def test_invalid_status_json_is_fail_safe(self):
        watchdog = self.make_watchdog()
        watchdog.command = Mock(
            return_value=subprocess.CompletedProcess(
                ["sparkrun"],
                0,
                stdout="not json",
                stderr="",
            )
        )
        self.assertIsNone(watchdog.get_status())

    def test_config_load_requires_object_root(self):
        with patch("builtins.open", unittest.mock.mock_open(read_data=json.dumps([]))):
            with self.assertRaises(ValueError):
                load_config("unused.json")

    def test_placeholders_are_not_expanded_inside_values(self):
        rendered = expand_placeholders(
            ["{reason}", "{recipe}"],
            {
                "reason": "matched {recipe}",
                "recipe": "@namespace/model",
            },
        )
        self.assertEqual(rendered, ["matched {recipe}", "@namespace/model"])

    def make_restart_event(self, **overrides):
        values = {
            "recipe": "@namespace/model",
            "run_id": "old-run",
            "cluster_id": "sparkrun_old-run",
            "reason": "log signature: EngineDeadError",
            "trigger": "fatal_log",
            "fatal_pattern": "EngineDeadError",
            "launch_fatal_pattern": None,
            "runtime_seconds": 125.9,
            "stop_succeeded": True,
            "launch_succeeded": True,
            "timestamp": "2026-09-15T17:00:00+00:00",
        }
        values.update(overrides)
        return RestartEvent(**values)

    def test_restart_event_exports_env_and_json(self):
        event = self.make_restart_event()
        env = event.environ()
        payload = json.loads(env["SPARKRUN_WATCHDOG_EVENT_JSON"])

        self.assertEqual(env["SPARKRUN_WATCHDOG_EVENT"], "restart")
        self.assertEqual(env["SPARKRUN_WATCHDOG_RECIPE"], "@namespace/model")
        self.assertEqual(env["SPARKRUN_WATCHDOG_RUN_ID"], "old-run")
        self.assertEqual(env["SPARKRUN_WATCHDOG_TRIGGER"], "fatal_log")
        self.assertEqual(env["SPARKRUN_WATCHDOG_FATAL_PATTERN"], "EngineDeadError")
        self.assertEqual(env["SPARKRUN_WATCHDOG_RUNTIME_SECONDS"], "125")
        self.assertEqual(env["SPARKRUN_WATCHDOG_STOP_SUCCEEDED"], "true")
        self.assertEqual(payload["runtime_seconds"], 125)
        self.assertTrue(payload["launch_succeeded"])
        self.assertIsNone(payload["launch_fatal_pattern"])

    def test_post_restart_action_expands_args_and_sets_env(self):
        watchdog = SparkRunWatchdog(
            {
                "post_restart_command": [
                    "/notify",
                    "--recipe",
                    "{recipe}",
                    "--reason",
                    "{reason}",
                ],
                "models": [{"recipe": "@namespace/model", "launch_args": []}],
            }
        )
        event = self.make_restart_event()
        completed = subprocess.CompletedProcess(["/notify"], 0, stdout="", stderr="")

        with patch("sparkrun_watchdog.subprocess.run", return_value=completed) as run:
            watchdog.run_post_restart_action(
                watchdog.models["@namespace/model"],
                event,
            )

        run.assert_called_once()
        command = run.call_args.args[0]
        env = run.call_args.kwargs["env"]
        self.assertEqual(
            command,
            [
                "/notify",
                "--recipe",
                "@namespace/model",
                "--reason",
                "log signature: EngineDeadError",
            ],
        )
        self.assertEqual(env["SPARKRUN_WATCHDOG_CLUSTER_ID"], "sparkrun_old-run")
        self.assertEqual(run.call_args.kwargs["timeout"], 30)

    def test_per_model_post_restart_command_overrides_global(self):
        watchdog = SparkRunWatchdog(
            {
                "post_restart_command": ["/global-notify"],
                "models": [
                    {
                        "recipe": "@namespace/model",
                        "launch_args": [],
                        "post_restart_command": ["/local-notify"],
                    }
                ],
            }
        )
        self.assertEqual(
            watchdog.post_restart_command_for(watchdog.models["@namespace/model"]),
            ["/local-notify"],
        )

    def test_empty_per_model_post_restart_command_disables_hook(self):
        watchdog = SparkRunWatchdog(
            {
                "post_restart_command": ["/global-notify"],
                "models": [
                    {
                        "recipe": "@namespace/model",
                        "launch_args": [],
                        "post_restart_command": [],
                    }
                ],
            }
        )
        state = watchdog.models["@namespace/model"]
        self.assertEqual(watchdog.post_restart_command_for(state), [])

        with patch("sparkrun_watchdog.subprocess.run") as run:
            watchdog.run_post_restart_action(state, self.make_restart_event())

        run.assert_not_called()

    def test_invalid_post_restart_command_is_rejected(self):
        with self.assertRaises(ValueError):
            SparkRunWatchdog(
                {
                    "post_restart_command": "/bin/true",
                    "models": [],
                }
            )

    def test_post_restart_timeout_does_not_raise(self):
        watchdog = SparkRunWatchdog(
            {
                "post_restart_command": ["/notify"],
                "models": [{"recipe": "@namespace/model", "launch_args": []}],
            }
        )

        with patch(
            "sparkrun_watchdog.subprocess.run",
            side_effect=subprocess.TimeoutExpired("/notify", 30),
        ):
            watchdog.run_post_restart_action(
                watchdog.models["@namespace/model"],
                self.make_restart_event(),
            )

    def test_restart_model_runs_post_restart_action(self):
        watchdog = self.make_watchdog()
        state = watchdog.models["@namespace/model"]
        state.run_id = "old-run"
        state.cluster_id = "sparkrun_old-run"
        state.run_observed_at = time.monotonic() - 125

        with patch.object(
            watchdog,
            "command",
            return_value=subprocess.CompletedProcess(
                ["sparkrun"],
                0,
                stdout="",
                stderr="",
            ),
        ), patch.object(watchdog, "run_post_restart_action") as hook:
            watchdog.restart_model(
                state,
                reason="log signature: EngineDeadError",
                trigger="fatal_log",
                fatal_pattern="EngineDeadError",
                expected_run_id="old-run",
            )

        hook.assert_called_once()
        event = hook.call_args.args[1]
        self.assertEqual(event.run_id, "old-run")
        self.assertEqual(event.cluster_id, "sparkrun_old-run")
        self.assertEqual(event.trigger, "fatal_log")
        self.assertEqual(event.fatal_pattern, "EngineDeadError")
        self.assertTrue(event.stop_succeeded)
        self.assertTrue(event.launch_succeeded)
        self.assertGreaterEqual(event.runtime_seconds, 125)

    def test_cooldown_does_not_run_post_restart_action(self):
        watchdog = self.make_watchdog()
        state = watchdog.models["@namespace/model"]
        state.run_id = "old-run"
        state.last_restart = time.monotonic()

        with patch.object(watchdog, "run_post_restart_action") as hook:
            watchdog.restart_model(
                state,
                reason="recipe missing from sparkrun status",
                trigger="missing",
            )

        hook.assert_not_called()

    def test_failed_stop_still_runs_post_restart_action(self):
        watchdog = self.make_watchdog()
        state = watchdog.models["@namespace/model"]
        state.run_id = "old-run"

        with patch.object(
            watchdog,
            "command",
            return_value=subprocess.CompletedProcess(["sparkrun"], 1),
        ), patch.object(watchdog, "run_post_restart_action") as hook:
            watchdog.restart_model(
                state,
                reason="log signature: EngineDeadError",
                trigger="fatal_log",
                fatal_pattern="EngineDeadError",
                expected_run_id="old-run",
            )

        hook.assert_called_once()
        event = hook.call_args.args[1]
        self.assertFalse(event.stop_succeeded)
        self.assertIsNone(event.launch_succeeded)

    def test_discovered_run_records_observation_time(self):
        watchdog = self.make_watchdog()
        state = watchdog.models["@namespace/model"]
        watchdog.get_status = Mock(
            return_value={
                "solo_entries": [
                    {
                        "cluster_id": "sparkrun_abc",
                        "meta": {"recipe": "@namespace/model"},
                    }
                ]
            }
        )

        with patch.object(watchdog, "start_log_follower"), patch.object(
            watchdog, "stop_log_follower"
        ):
            watchdog.reconcile()

        self.assertEqual(state.run_id, "abc")
        self.assertIsNotNone(state.run_observed_at)

    def test_launch_observation_is_kept_when_run_appears(self):
        watchdog = self.make_watchdog()
        state = watchdog.models["@namespace/model"]
        launched = time.monotonic() - 5
        state.launched_at = launched
        state.run_observed_at = launched
        watchdog.get_status = Mock(
            return_value={
                "solo_entries": [
                    {
                        "cluster_id": "sparkrun_abc",
                        "meta": {"recipe": "@namespace/model"},
                    }
                ]
            }
        )

        with patch.object(watchdog, "start_log_follower"), patch.object(
            watchdog, "stop_log_follower"
        ):
            watchdog.reconcile()

        self.assertEqual(state.run_id, "abc")
        self.assertEqual(state.run_observed_at, launched)


if __name__ == "__main__":
    unittest.main()
