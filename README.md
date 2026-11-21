# rqark-task

The worker of the RQArk Ecology System: it claims the scheduled agent runs `rqark-data-cloud` holds, executes each one against `rqark-app`, and reports the outcome back.

## Why rqark-task

`rqark-agent-cloud` used to carry the scheduled-run loop in every web worker, with a Redis lease making sure only one of them claimed at a time. This service is that loop on its own, so a scheduled run no longer competes with a conversation for the same event loop, and the worker can be scaled, restarted or stopped without touching a chat socket.

- **One claimer across every worker** — a Redis lease elects a leader, so `TASK_CONCURRENCY` is the real cap and not that cap multiplied by the worker count.
- **A run is isolated** — it builds its own agent, its own data cloud client and, when sandboxing is on, its own workspace container, then releases all three when it ends.
- **A one-shot mode** — `--once` claims a batch, runs it and exits, so a schedule of a few runs a day does not need a process that lives all day.
- **Memory goes back** — a finished run hands its freed heap to the kernel, so a worker that idles between runs does not sit at the high-water mark of its largest one.

## The ecosystem

| Repository | What it is | Used here for |
| --- | --- | --- |
| [`rqark-core`](../rqark-core) | Stateless agent engine for Python. | The agent loop behind a run. |
| [`rqark-app`](../rqark-app) | Sessions, providers and agents on top of `rqark-core`. | Builds the run's agent and provider, and runs the prompt. |
| [`rqark-sandbox`](../rqark-sandbox) | Docker workspace with file, shell and git tools. | The workspace a run is executed inside. |
| [`rqark-data-cloud`](../rqark-data-cloud) | Accounts and stored data. | Hands out the run to claim and receives its result over `/api/internal`. |
| [`rqark-agent-cloud`](../rqark-agent-cloud) | HTTP and WebSocket service over `rqark-app`. | Nothing; both call the data cloud, not each other. |
| `rqark-task` | This repository. | — |

## Architecture

A plain asyncio process over `rqark-app`. There is no HTTP server here: the loop claims, executes and finalizes, and that is all.

```
main.py                     entrypoint: logging, start the loop, wait for a signal, stop the loop
src/rqark_task/
├── loop.py                 the claim tick and its in-flight runs
├── leader.py               the Redis lease that elects one claiming worker
├── runner.py               the claim payload, the data cloud client, one run's execution
├── config.py               every setting, read from the environment at import
├── memory.py               returns a finished run's heap to the kernel
├── logging.py              log level and format
└── redis.py                the shared Redis client
docker/                     the image and its entrypoint
```

- **The loop is idle until it is configured** — with `DATA_CLOUD_URL` or `INTERNAL_TOKEN` empty, the tick logs once and does nothing, rather than retrying a claim that cannot be accepted.
- **The lease is a Redis key** — the leader renews it every third of `TASK_LEADER_TTL`. A worker that cannot reach Redis stops claiming instead of risking a second leader; runs wait out the outage instead of being duplicated.
- **A run finalizes even when cancelled** — a shutdown cancels the in-flight run, and the finalize call is shielded so the data cloud hears about the run rather than sweeping it to lost later.

## Running

```bash
docker compose up -d --build
```

That builds the image and starts the worker against `host.docker.internal` for the data cloud and Redis. The container mounts the host docker socket, because the workspace containers it starts are siblings rather than children.

On the host instead:

```bash
uv sync
python main.py
```

Both need Redis and a reachable data cloud; `.env` points at both on localhost.

The worker stops on `SIGINT` or `SIGTERM`, cancelling every in-flight run and finalizing it before exiting.

## One shot

```bash
docker compose run --rm task --once
```

`--once` claims one batch, runs it and exits instead of staying up. A schedule of a few runs a day does not need a resident process, and a fresh process cannot carry the peak of yesterday's run: the resident size of a long-lived worker after one run stays at that run's high-water mark, which is the whole footprint of a service that idles at `0.00%` CPU.

`--limit` sets how many runs that batch claims (default `1`). The production stack takes it from a timer:

```
*/15 * * * * cd /srv/rqark-task && docker compose -f docker-compose.production.yml run --rm task --once
```

## How a run flows

1. `LeaderLease` holds a Redis key so one process claims at a time across all workers.
2. `TaskLoop` claims up to `TASK_CONCURRENCY` runs from the data cloud on every tick.
3. `execute` builds the agent and provider from the claimed definitions, opens the workspace when sandboxing is on, runs the prompt, and reports `ok`, `error`, `timeout` or `cancelled` back to the data cloud.
4. The run's transcript is saved when it fails. A successful run leaves no memory row: a run token only reaches its own session, so `RQARK_MEMORY_AUTO_WRITE` is off here.

## Configuration

`.env` holds the values a local checkout needs. Every key falls back to the listed default when it is unset or empty.

| Variable | Default | Meaning |
| --- | --- | --- |
| `PROJECT_NAME` | empty | Prefix for the container name compose creates. |
| `LOG_LEVEL` | `INFO` | Log level for the worker. |
| `DATA_CLOUD_URL` | `http://localhost:7302` | Data cloud base URL. |
| `DATA_CLOUD_TIMEOUT` | `30` | Seconds per call to it. |
| `INTERNAL_TOKEN` | empty | Secret the data cloud expects in `X-Internal-Token`; the loop stays idle while it is empty. |
| `REDIS_SERVER` | `localhost` | Redis host, for the leader lease. |
| `REDIS_PORT` | `6379` | Redis port. |
| `REDIS_URL` | `redis://{server}:{port}/0` | Wins over the two above when set. |
| `TASK_CONCURRENCY` | `1` | Runs executed at once. |
| `TASK_TICK_SECONDS` | `30` | Delay between claims. |
| `TASK_RUN_TIMEOUT` | `600` | A run is cancelled after this many seconds. |
| `TASK_STREAM_MAX_CHARS` | `200000` | Transcript prefix kept per run. |
| `TASK_LEADER_TTL` | `60` | Lifetime of the leader lease. |
| `SANDBOX_ENABLED` | `true` | Run the agent inside a workspace container. |
| `SANDBOX_IMAGE` | `ubuntu:24.04` | Image for that container. |
| `WORKSPACE_START_TIMEOUT` | `30` | Seconds a container may take to start. |
| `WORKSPACE_MAX_BYTES` | `104857600` | Largest file the workspace reads or writes. |
| `WORKSPACE_SHELL_MAX_BYTES` | `8388608` | stdout and stderr cap for one shell call. |
| `RQARK_MEMORY_AUTO_WRITE` | `0` | Whether a finished run is recorded in memory; a run token only reaches its own session, so the writer cannot read the account switches and its writes are refused. |

`WORKSPACE_MAX_BYTES` and `WORKSPACE_SHELL_MAX_BYTES` are read by `rqark-sandbox`; the production compose passes them through from the host.

## Testing

```bash
uv run pytest
```

Before a change, also run `uv run ruff check --fix` and `uv run ty check --fix`; `uv run pyright` and `uv run mypy` are there to confirm type safety. Comments are not written in this repository — the code is expected to say it.

## Deployment

`docker-compose.production.yml` is the deployment shape: one container, the docker socket mounted because the workspace containers it starts are siblings, `stop_grace_period` 90s so an in-flight run can finish, and `mem_limit` 2GB for the workspace work. It joins the external `rqark-project` network and resolves the data cloud as `server:7302` and Redis as `redis`; the network is created once per host with `docker network create rqark-project`.

The host has to carry a local copy of `SANDBOX_IMAGE`. A missing image makes the workspace raise `ImageNotFound` and the run fail, so `docker pull ubuntu:24.04` belongs in the deploy step.

A `v*` tag builds `docker/Dockerfile` and pushes the image to the GitHub Container Registry as both `X.Y.Z` and `X.Y`.

## Contributing

Issues, commits, pull requests, versions and releases follow [`CONTRIBUTING.md`](./CONTRIBUTING.md).
