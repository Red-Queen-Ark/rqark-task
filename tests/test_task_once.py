import pytest

import main as entry


@pytest.fixture(autouse=True)
def quiet(monkeypatch):
    monkeypatch.setattr(entry, 'setup_logging', lambda: None)
    monkeypatch.setattr(entry, 'freeze', lambda: None)


def test_no_flag_serves_the_loop(monkeypatch):
    served: list[int] = []

    async def serve():
        served.append(1)

    monkeypatch.setattr(entry, 'serve', serve)
    monkeypatch.setattr(entry, 'once', lambda limit: pytest.fail('--once was not asked for'))

    entry.main([])

    assert served == [1]


def test_once_claims_one_batch_and_never_starts_the_loop(monkeypatch):
    claimed: list[int] = []

    async def once(limit):
        claimed.append(limit)
        return 1

    monkeypatch.setattr(entry, 'once', once)
    monkeypatch.setattr(entry, 'serve', lambda: pytest.fail('--once must not serve'))

    entry.main(['--once'])

    assert claimed == [1]


def test_once_passes_its_limit(monkeypatch):
    claimed: list[int] = []

    async def once(limit):
        claimed.append(limit)

    monkeypatch.setattr(entry, 'once', once)

    entry.main(['--once', '--limit', '3'])

    assert claimed == [3]


def test_a_negative_limit_is_clamped_to_one(monkeypatch):
    claimed: list[int] = []

    async def once(limit):
        claimed.append(limit)

    monkeypatch.setattr(entry, 'once', once)

    entry.main(['--once', '--limit', '-4'])

    assert claimed == [1]


def test_the_entrypoint_freezes_before_it_claims(monkeypatch):
    order: list[str] = []

    monkeypatch.setattr(entry, 'freeze', lambda: order.append('freeze'))

    async def once(limit):
        order.append('once')

    monkeypatch.setattr(entry, 'once', once)

    entry.main(['--once'])

    assert order == ['freeze', 'once']


async def test_once_releases_the_memory_it_used(monkeypatch):
    released: list[int] = []

    async def run_once(limit):
        return 0

    monkeypatch.setattr(entry, 'run_once', run_once)
    monkeypatch.setattr(entry, 'release_memory', lambda: released.append(1))

    assert await entry.once(1) == 0
    assert released == [1]


async def test_once_releases_even_when_the_batch_crashes(monkeypatch):
    released: list[int] = []

    async def run_once(limit):
        raise RuntimeError('data cloud is down')

    monkeypatch.setattr(entry, 'run_once', run_once)
    monkeypatch.setattr(entry, 'release_memory', lambda: released.append(1))

    with pytest.raises(RuntimeError):
        await entry.once(1)

    assert released == [1]
