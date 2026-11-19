# rqark-task

A worker service that claims scheduled agent runs from `rqark-data-cloud` and executes them against an `rqark-app` agent, optionally inside an `rqark-sandbox` workspace.

## Why rqark-task

Scheduled runs used to share the API process: every web worker carried its own task loop, and a lease in Redis made sure only one of them claimed at a time. This service is that loop on its own, so a run no longer competes with a conversation for the same event loop, and the worker can be scaled or restarted without touching a chat socket.

## Running

```bash
docker compose up -d --build
```

A tag on `main` deploys through `docker-compose.production.yml`. Locally, `python main.py` runs the same worker on the host.

The worker stops on `SIGINT` or `SIGTERM`, cancelling every in-flight run and finalizing it before exiting.

## How a run flows

1. `LeaderLease` holds a Redis key so one process claims at a time across all workers.
2. `TaskLoop` claims up to `TASK_CONCURRENCY` runs from the data cloud on every tick.
3. `execute` builds the agent and provider from the claimed definitions, opens the workspace when sandboxing is on, runs the prompt, and reports `ok`, `error`, `timeout` or `cancelled` back to the data cloud.
4. The run's transcript is saved when it fails. A successful run leaves no memory row: a run token only reaches its own session, so `RQARK_MEMORY_AUTO_WRITE` is off here.

## Layout

| Path | Contents |
| --- | --- |
| `main.py` | entrypoint: logging, start the loop, wait for a signal, stop the loop |
| `src/rqark_task/runner.py` | claim payload, data cloud client, one run's execution |
| `src/rqark_task/loop.py` | the claim tick and its in-flight runs |
| `src/rqark_task/leader.py` | the Redis lease that elects one claiming worker |
| `src/rqark_task/config.py` | environment variables |

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATA_CLOUD_URL` | `http://localhost:7302` | data cloud base URL |
| `DATA_CLOUD_TIMEOUT` | `30` | HTTP timeout for the claim and finalize calls |
| `INTERNAL_TOKEN` | empty | token the data cloud expects on `/internal/*`; the loop stays idle while it is empty |
| `TASK_CONCURRENCY` | `1` | runs executed at once |
| `TASK_TICK_SECONDS` | `30` | delay between claims |
| `TASK_RUN_TIMEOUT` | `600` | a run is cancelled after this many seconds |
| `TASK_STREAM_MAX_CHARS` | `200000` | transcript prefix kept per run |
| `TASK_LEADER_TTL` | `60` | lifetime of the leader lease |
| `REDIS_URL` | `redis://localhost:6379/0` | where the leader lease lives |
| `SANDBOX_ENABLED` | `true` | run the agent inside a workspace container |
| `SANDBOX_IMAGE` | `ubuntu:24.04` | image for that container |
| `RQARK_MEMORY_AUTO_WRITE` | `0` in this service | whether a finished run is recorded in memory; a run token only reaches its own session, so the writer cannot read the account switches and its writes are refused |
| `WORKSPACE_START_TIMEOUT` | `30` | how long a container may take to start |
| `WORKSPACE_MAX_BYTES` | `104857600` | largest file the workspace reads or writes |
| `WORKSPACE_SHELL_MAX_BYTES` | `8388608` | stdout and stderr cap for one shell call |

## Deployment

The container mounts the host docker socket: the workspace containers it starts are siblings, not children. The workspace image must already exist on the host.

The container joins the external `rqark-project` network so it can reach `server` and `redis` by name.
