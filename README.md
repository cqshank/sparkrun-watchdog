# sparkrun-watchdog

A small, dependency-free supervisor for `sparkrun` model recipes.

The watchdog periodically checks `sparkrun status --json`, tracks configured
recipes, follows their `sparkrun logs` output, and relaunches a recipe when it
is missing or a configured fatal log signature is observed. It is deliberately
conservative: an unavailable or invalid status response is treated as
unknown, not as proof that a workload has stopped.

## Requirements

- Python 3.9 or newer
- The `sparkrun` CLI installed and available on `PATH`
- Permissions to run `sparkrun status`, `sparkrun logs`, `sparkrun stop`, and
  `sparkrun run`

The watchdog has no runtime Python dependencies.

## Quick start

Copy the example configuration and edit the recipes and launch arguments for
your environment:

```bash
cp config/sparkrun-watchdog.example.json config/sparkrun-watchdog.local.json
${EDITOR:-vi} config/sparkrun-watchdog.local.json
```

Run directly from a checkout:

```bash
python3 sparkrun_watchdog.py \
  --config config/sparkrun-watchdog.local.json \
  --debug
```

Or install the small package and use its console command:

```bash
python3 -m pip install .
sparkrun-watchdog --config /etc/sparkrun-watchdog.json
```

The process handles `SIGINT` and `SIGTERM` and stops following logs before it
exits. A service manager such as systemd is recommended for keeping the
watchdog itself running.

## systemd

The included unit runs as `chris` by default. Change `User=` and the config
ownership when deploying elsewhere. It assumes the checkout is at
`/opt/sparkrun-watchdog` and Python is at `/usr/bin/python3`.

Install and configure it:

```bash
sudo install -o chris -g chris -m 0640 \
  config/sparkrun-watchdog.example.json /etc/sparkrun-watchdog.json
sudoedit /etc/sparkrun-watchdog.json
sudo install -m 0644 systemd/sparkrun-watchdog.service \
  /etc/systemd/system/sparkrun-watchdog.service
sudo systemctl daemon-reload
sudo systemctl enable --now sparkrun-watchdog.service
```

If `sparkrun` is not in `/usr/local/bin`, `/usr/bin`, or `/bin`, add its
directory to `/etc/sparkrun-watchdog.env`:

```text
PATH=/home/chris/.local/bin:/usr/local/bin:/usr/bin:/bin
```

Useful commands:

```bash
sudo systemctl status sparkrun-watchdog.service
sudo journalctl -u sparkrun-watchdog.service -f
sudo systemctl restart sparkrun-watchdog.service
sudo systemctl stop sparkrun-watchdog.service
```

After changing the unit, run `systemctl daemon-reload`. Stopping the watchdog
stops its log followers but does not stop supervised workloads. Check the
journal first if a recipe restarts repeatedly; status-query failures are
treated as unknown and do not trigger speculative restarts.

## Configuration

The configuration is JSON so no YAML package is required. The important
fields are:

| Field | Purpose |
| --- | --- |
| `status_interval_seconds` | Delay between status reconciliations. |
| `restart_cooldown_seconds` | Minimum delay between restart attempts for one recipe. |
| `startup_grace_seconds` | Time to wait for a newly launched recipe to appear in status. |
| `log_tail_lines` | Number of recent lines requested when attaching to logs. |
| `fatal_patterns` | Literal substrings that indicate a failed run. |
| `post_restart_command` | Argument list to run after a restart attempt. |
| `post_restart_timeout_seconds` | Maximum time to wait for that command. |
| `models` | Recipes to supervise and their arguments for `sparkrun run`. |

Each model entry has this shape:

```json
{
  "recipe": "@namespace/recipe-name",
  "launch_args": ["--example-flag", "value"]
}
```

An optional per-model `post_restart_command` overrides the global command.
An empty per-model list disables the hook for that recipe.

Add one entry per recipe. Each model is monitored independently, with its own
run ID, `sparkrun logs -f` follower, cooldown, and restart state. Launches use
`sparkrun run --no-follow`; the watchdog captures and scans both stdout and
stderr from that command before attaching to the new run's logs.

The status parser supports grouped runs under `groups` and single-node runs
under `solo_entries`. It prefers `meta.recipe` and falls back to
`meta.runtime_info.container_sparkrun_recipe`. A `cluster_id` beginning with
`sparkrun_` is normalized to the run ID expected by `sparkrun logs` and
`sparkrun stop`.

### Post-restart command

After a restart attempt proceeds past cooldown and stale-run checks, the
watchdog can run a command. A typical use is a notification script. The
command is an argument list, not a shell string:

```json
{
  "post_restart_timeout_seconds": 30,
  "post_restart_command": [
    "/usr/local/bin/notify-sparkrun-restart",
    "--recipe",
    "{recipe}",
    "--trigger",
    "{trigger}"
  ]
}
```

`{field}` tokens in those arguments are expanded once from the event. The
same fields are also exported as `SPARKRUN_WATCHDOG_*` environment
variables, plus `SPARKRUN_WATCHDOG_EVENT_JSON` for a structured copy.
Unset optional values are empty strings in the environment and `null` in
the JSON object.

| Field | Trigger / meaning |
| --- | --- |
| `recipe` | Configured recipe that was restarted. |
| `run_id` | Run that was replaced, if any. |
| `cluster_id` | Cluster ID for that run, if any. |
| `reason` | Human-readable restart reason. |
| `trigger` | `fatal_log` or `missing`. |
| `fatal_pattern` | Log substring that triggered the restart, if any. |
| `launch_fatal_pattern` | Log substring seen in `sparkrun run` output, if any. |
| `runtime_seconds` | Whole seconds the watchdog had observed this run. |
| `stop_succeeded` | `true`, `false`, or `skipped`. |
| `launch_succeeded` | `true`, `false`, or `skipped`. |
| `timestamp` | UTC ISO-8601 time of the restart attempt. |

`runtime_seconds` is how long this watchdog process had been watching the
run, not the workload's own start time. A run discovered after the
watchdog starts is measured from first observation.

The hook runs as the watchdog user, inherits the process environment, and
is subject to `post_restart_timeout_seconds` (default 30). A timeout,
nonzero exit, or failure to start the command is logged and does not
change restart behavior. Stale or cooldown-ignored restart requests do
not run the hook. If shutdown interrupts a restart after the old run is
stopped, the hook may still run with `launch_succeeded=skipped`.

## Safety notes

This program can stop and relaunch workloads. Review the configuration before
running it and start with a generous cooldown and startup grace period. Keep
machine-specific configuration outside version control; the repository's
`.gitignore` excludes `config/*.local.json`.

The watchdog does not attempt to restart anything when `sparkrun status
--json` fails or returns invalid JSON. It also checks the run ID before acting
on a log-follower event so an old log stream cannot restart a newer run.
Routine workload log lines and successful post-restart command output are not
copied into the watchdog log. A matching fatal line is recorded when detected;
operational command and restart lifecycle outcomes are logged separately.
At DEBUG level, command start/completion results and routine reconciliation
decisions are visible without copying command output into the log.
Matched fatal lines are retained in the watchdog log, so consider journal
access and retention if those lines may contain sensitive content.
Post-restart commands inherit the watchdog environment and can
receive recipe names, run IDs, and matched log snippets; keep secrets out
of hook arguments and treat hook output as potentially sensitive.

## Tests

The tests use only the Python standard library and mock the external
`sparkrun` command:

```bash
python3 -m unittest discover -s tests -v
```

## License

MIT. See [LICENSE](LICENSE).
