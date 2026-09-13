# AGENTS.md

## Project overview

`sparkrun-watchdog` is a small, dependency-free Python supervisor for
configured `sparkrun` recipes. It periodically reads `sparkrun status --json`,
follows the logs for each configured run, and restarts a recipe when a
configured fatal log signature is observed or the recipe disappears from
status.

## Development rules

- Keep runtime dependencies in the Python standard library unless there is a
  clear, documented reason to add one.
- Treat `sparkrun` as an external command. Do not shell out through a string;
  pass argument lists to `subprocess` so recipe arguments remain unambiguous.
- A failed or invalid status query must not cause a restart. This avoids
  duplicate workloads during a temporary control-plane failure.
- Preserve the startup grace period, restart cooldown, and stale-log race
  guard when changing restart behavior.
- Do not put real recipes, credentials, hostnames, or infrastructure details
  in tests, examples, or documentation.
- Update the tests and README when changing configuration keys or lifecycle
  behavior.
- Keep `systemd/sparkrun-watchdog.service` and the systemd section in the
  README synchronized when changing paths, signals, or shutdown semantics.
- The service is intentionally non-destructive on shutdown: terminating the
  watchdog must not stop the workloads it supervises.

## Validation

Run the standard-library test suite from the repository root:

```bash
python3 -m unittest discover -s tests -v
```

For packaging changes, also run:

```bash
python3 -m build
```

Do not assume that `sparkrun` is available in CI; tests should mock external
commands.

The systemd unit is a deployment template, not a CI test. Validate it on a
target Linux host with `systemd-analyze verify` after substituting deployment
paths and installing the unit.
