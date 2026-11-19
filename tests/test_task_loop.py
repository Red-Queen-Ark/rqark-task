import asyncio

import httpx
import pytest

from rqark_task import loop as loop_module
from rqark_task.loop import TaskLoop
from rqark_task.runner import ClaimedRun

RUN = ClaimedRun(
    run_id='run-1',
    user_id='user-1',
    task_id='task-1',
    task_name='Morning brief',
    prompt='say hi',
    session_id='session-1',
    actor_token='run-token',
    agent_def={'name': 'assistant'},
    provider_def={'model_name': 'gpt-5'},
)


@pytest.fixture(autouse=True)
def configured(monkeypatch):
    monkeypatch.setattr(loop_module, 'INTERNAL_TOKEN', 'test-token')
    monkeypatch.setattr(loop_module, 'DATA_CLOUD_URL', 'http://data-cloud:7302')
    monkeypatch.setattr(loop_module, 'LeaderLease', AlwaysLeader)


class AlwaysLeader:
    """Stands in for the redis lease so a loop test never needs a broker."""

    def __init__(self, key='tasks:leader', ttl=30):
        self.is_leader = True
        self.key = key

    async def start(self):
        return None

    async def stop(self):
        return None


class FakeClient:
    def __init__(self, runs=None, error=None):
        self.runs = runs or []
        self.error = error
        self.limits: list[int] = []

    async def claim(self, limit):
        self.limits.append(limit)
        if self.error is not None:
            raise self.error
        return list(self.runs)

    async def finalize(self, *args, **kwargs):
        pass


def http_error(status):
    request = httpx.Request('POST', 'http://data-cloud/api/internal/tasks/claim')
    return httpx.HTTPStatusError('boom', request=request, response=httpx.Response(status, request=request))


async def test_a_tick_claims_the_free_slots_and_spawns_one_runner_per_run():
    client = FakeClient(runs=[RUN, RUN])
    executed: list[str] = []

    async def runner(inner, run):
        executed.append(run.run_id)

    loop = TaskLoop(client=client, concurrency=2, tick_seconds=60, runner=runner)
    claimed = await loop.tick()

    assert claimed == 2
    assert client.limits == [2]
    await asyncio.gather(*list(loop._inflight))
    assert executed == ['run-1', 'run-1']
    assert loop.busy() == 0


async def test_a_busy_loop_claims_nothing():
    client = FakeClient()
    gate = asyncio.Event()

    async def runner(inner, run):
        await gate.wait()

    loop = TaskLoop(client=client, concurrency=1, tick_seconds=60, runner=runner)
    loop._spawn(RUN)

    assert loop.free_slots() == 0
    assert await loop.tick() == 0
    assert client.limits == []

    gate.set()
    await asyncio.gather(*list(loop._inflight))


async def test_an_empty_claim_is_not_an_error_and_is_asked_again():
    client = FakeClient(runs=[])
    loop = TaskLoop(client=client, concurrency=1, tick_seconds=60, runner=lambda *_: None)

    assert await loop.tick() == 0
    assert await loop.tick() == 0
    assert client.limits == [1, 1]


async def test_a_failed_claim_does_not_kill_the_tick():
    client = FakeClient(error=http_error(500))
    loop = TaskLoop(client=client, concurrency=1, tick_seconds=60, runner=lambda *_: None)

    assert await loop.tick() == 0
    assert loop._task is None


async def test_a_wrong_token_is_logged_once(caplog):
    client = FakeClient(error=http_error(401))
    loop = TaskLoop(client=client, concurrency=1, tick_seconds=60, runner=lambda *_: None)

    caplog.clear()
    await loop.tick()
    await loop.tick()
    await loop.tick()

    refusals = [record for record in caplog.records if 'INTERNAL_TOKEN' in record.getMessage()]
    assert len(refusals) == 1


async def test_a_crashed_runner_does_not_take_the_loop_down():
    client = FakeClient(runs=[RUN])
    started = asyncio.Event()

    async def runner(inner, run):
        started.set()
        raise RuntimeError('sandbox exploded')

    loop = TaskLoop(client=client, concurrency=1, tick_seconds=60, runner=runner)
    await loop.tick()
    await asyncio.gather(*list(loop._inflight), return_exceptions=True)

    assert started.is_set()
    assert loop.busy() == 0


async def test_the_loop_keeps_ticking_and_stops_cleanly(monkeypatch):
    client = FakeClient()
    loop = TaskLoop(client=client, concurrency=1, tick_seconds=0.01, runner=lambda *_: None)

    await loop.start()
    await asyncio.sleep(0.05)
    await loop.stop()

    assert len(client.limits) >= 2
    assert loop._task is None


async def test_stopping_cancels_the_runs_in_flight():
    client = FakeClient()
    cancelled = asyncio.Event()

    async def runner(inner, run):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    loop = TaskLoop(client=client, concurrency=1, tick_seconds=60, runner=runner)
    loop._spawn(RUN)
    await loop.start()
    await asyncio.sleep(0.01)
    await loop.stop()

    assert cancelled.is_set()
    assert loop.busy() == 0


async def test_an_unconfigured_loop_says_so_and_stays_alive(monkeypatch):
    client = FakeClient()
    loop = TaskLoop(client=client, concurrency=1, tick_seconds=0.01, runner=lambda *_: None)
    monkeypatch.setattr(loop_module, 'INTERNAL_TOKEN', '')

    await loop.start()
    await asyncio.sleep(0.03)
    await loop.stop()

    assert client.limits == []


async def test_start_and_stop_keep_one_loop(monkeypatch):
    created: list[TaskLoop] = []

    def build():
        loop = TaskLoop(client=FakeClient(), concurrency=1, tick_seconds=0.01, runner=lambda *_: None)
        created.append(loop)
        return loop

    monkeypatch.setattr(loop_module, 'TaskLoop', build)
    monkeypatch.setattr(loop_module, '_loop', None)

    first = await loop_module.start()
    second = await loop_module.start()
    assert first is second
    assert len(created) == 1

    await loop_module.stop()
    assert loop_module.current() is None
