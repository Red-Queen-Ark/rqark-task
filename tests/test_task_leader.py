import asyncio

import pytest

from rqark_task import leader as leader_module
from rqark_task.leader import LeaderLease


class FakeRedis:
    def __init__(self):
        self.values: dict[str, tuple[str, int]] = {}
        self.sets: list[tuple[str, int]] = []
        self.evals: list[tuple[str, tuple]] = []
        self.fail = False

    async def set(self, key, value, nx=False, ex=None):
        if self.fail:
            raise RuntimeError('redis is down')
        if nx and key in self.values:
            return None
        self.values[key] = (value, ex or 0)
        self.sets.append((key, ex or 0))
        return True

    async def eval(self, script, numkeys, key, *args):
        if self.fail:
            raise RuntimeError('redis is down')
        self.evals.append((script, args))
        held = self.values.get(key)
        if held is None or held[0] != args[0]:
            return 0
        if 'del' in script:
            self.values.pop(key, None)
            return 1
        self.values[key] = (held[0], int(args[1]))
        return 1


@pytest.fixture
def redis(monkeypatch):
    fake = FakeRedis()

    async def get_redis():
        return fake

    monkeypatch.setattr(leader_module, 'get_redis', get_redis)
    return fake


async def test_a_free_lease_is_taken(redis):
    lease = LeaderLease(ttl=30)

    await lease.start()
    try:
        assert lease.is_leader is True
        assert redis.sets == [('tasks:leader', 30)]
    finally:
        await lease.stop()


async def test_a_lease_another_worker_holds_is_left_alone(redis):
    redis.values['tasks:leader'] = ('someone-else', 30)
    lease = LeaderLease(ttl=30)

    await lease.start()
    try:
        assert lease.is_leader is False
        assert redis.values['tasks:leader'][0] == 'someone-else'
    finally:
        await lease.stop()


async def test_a_taken_over_lease_is_dropped_at_the_next_attempt(redis):
    lease = LeaderLease(ttl=30)
    await lease.start()

    redis.values['tasks:leader'] = ('someone-else', 30)
    await lease._attempt()
    await lease.stop()

    assert lease.is_leader is False


async def test_a_held_lease_is_renewed(redis):
    lease = LeaderLease(ttl=30)
    await lease.start()

    redis.values['tasks:leader'] = (redis.values['tasks:leader'][0], 1)
    await lease._attempt()
    await lease.stop()

    assert redis.values.get('tasks:leader') is None
    assert lease.is_leader is False


async def test_stopping_releases_the_lease(redis):
    lease = LeaderLease(ttl=30)
    await lease.start()

    await lease.stop()

    assert 'tasks:leader' not in redis.values


async def test_a_lease_without_redis_stays_idle(monkeypatch):
    """Without Redis there is no way to tell whether a peer already claims, so nobody claims."""

    async def no_redis():
        return None

    monkeypatch.setattr(leader_module, 'get_redis', no_redis)
    lease = LeaderLease(ttl=30)

    await lease.start()
    try:
        assert lease.is_leader is False
    finally:
        await lease.stop()


async def test_a_failing_redis_does_not_make_a_worker_leader(redis):
    redis.fail = True
    lease = LeaderLease(ttl=30)

    await lease.start()
    try:
        assert lease.is_leader is False
    finally:
        await lease.stop()


async def test_a_lost_lease_stops_the_worker_from_claiming(redis):
    from rqark_task.loop import TaskLoop

    class Client:
        def __init__(self):
            self.claimed = 0

        async def claim(self, limit):
            self.claimed += 1
            return []

    client = Client()
    lease = LeaderLease(ttl=30)
    await lease.start()
    loop = TaskLoop(client=client, concurrency=1, tick_seconds=60, lease=lease)
    assert await loop.tick() == 0
    assert client.claimed == 1

    redis.values['tasks:leader'] = ('someone-else', 30)
    await lease._attempt()

    assert await loop.tick() == 0
    assert client.claimed == 1
    await lease.stop()


async def test_the_lease_loop_is_cancelled_on_stop(redis, monkeypatch):
    monkeypatch.setattr(LeaderLease, '_interval', lambda self: 0.01)
    lease = LeaderLease(ttl=30)
    await lease.start()
    task = lease._task
    await asyncio.sleep(0.05)

    await lease.stop()

    assert task is not None and task.done()


async def test_a_renewal_that_cannot_reach_redis_drops_leadership(redis):
    """An unreachable Redis means ownership is unknown; claiming anyway risks two leaders."""
    lease = LeaderLease(ttl=30)
    await lease.start()

    redis.fail = True
    await lease._attempt()

    assert lease.is_leader is False
    await lease.stop()


async def test_the_renewal_loop_survives_a_failing_attempt(redis, monkeypatch):
    monkeypatch.setattr(LeaderLease, '_interval', lambda self: 0.01)
    lease = LeaderLease(ttl=30)
    await lease.start()

    async def boom():
        raise RuntimeError('redis exploded')

    monkeypatch.setattr(lease, '_attempt', boom)
    await asyncio.sleep(0.05)

    assert lease._task is not None and not lease._task.done()
    assert lease.is_leader is False
    await lease.stop()


async def test_a_hung_redis_does_not_block_start(redis, monkeypatch):
    async def hang(key, value, nx=False, ex=None):
        await asyncio.sleep(30)

    monkeypatch.setattr(redis, 'set', hang)
    lease = LeaderLease(ttl=30, timeout=0.05)

    await asyncio.wait_for(lease.start(), 1)
    try:
        assert lease.is_leader is False
    finally:
        await lease.stop()
