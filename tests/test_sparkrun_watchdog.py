import json
import subprocess
import unittest
from unittest.mock import Mock, patch

from sparkrun_watchdog import SparkRunWatchdog, load_config


class SparkRunWatchdogTests(unittest.TestCase):
    def make_watchdog(self):
        return SparkRunWatchdog(
            {
                "status_interval_seconds": 10,
                "restart_cooldown_seconds": 20,
                "startup_grace_seconds": 60,
                "fatal_patterns": ["EngineCore encountered a fatal error."],
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
            ["sparkrun", "run", "@namespace/model", "--tp", "2"]
        )
        self.assertIsNotNone(state.launched_at)

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


if __name__ == "__main__":
    unittest.main()
