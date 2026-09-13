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
| `models` | Recipes to supervise and their arguments for `sparkrun run`. |

Each model entry has this shape:

```json
{
  "recipe": "@namespace/recipe-name",
  "launch_args": ["--example-flag", "value"]
}
```

Add one entry per recipe. Each model is monitored independently, with its own
run ID, `sparkrun logs -f` follower, cooldown, and restart state.

The status parser supports grouped runs under `groups` and single-node runs
under `solo_entries`. It prefers `meta.recipe` and falls back to
`meta.runtime_info.container_sparkrun_recipe`. A `cluster_id` beginning with
`sparkrun_` is normalized to the run ID expected by `sparkrun logs` and
`sparkrun stop`.

## Safety notes

This program can stop and relaunch workloads. Review the configuration before
running it and start with a generous cooldown and startup grace period. Keep
machine-specific configuration outside version control; the repository's
`.gitignore` excludes `config/*.local.json`.

The watchdog does not attempt to restart anything when `sparkrun status
--json` fails or returns invalid JSON. It also checks the run ID before acting
on a log-follower event so an old log stream cannot restart a newer run.
The process forwards followed `sparkrun` output to its logger, so review
journal retention and access policies if model logs may contain sensitive
content.

## Tests

The tests use only the Python standard library and mock the external
`sparkrun` command:

```bash
python3 -m unittest discover -s tests -v
```

## License

MIT. See [LICENSE](LICENSE).
