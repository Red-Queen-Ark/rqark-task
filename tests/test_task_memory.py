import gc

import pytest

from rqark_task import memory as memory_module


@pytest.fixture(autouse=True)
def loaded(monkeypatch):
    monkeypatch.setattr(memory_module, '_trim_loaded', False)
    monkeypatch.setattr(memory_module, '_trim', None)


def test_release_memory_collects_and_trims(monkeypatch):
    collected: list[int] = []
    trimmed: list[int] = []

    monkeypatch.setattr(memory_module.gc, 'collect', lambda: collected.append(1))
    monkeypatch.setattr(memory_module, '_malloc_trim', lambda: lambda pad: trimmed.append(pad) or 1)

    assert memory_module.release_memory() is True
    assert collected == [1]
    assert trimmed == [0]


def test_release_memory_keeps_the_garbage_collection_when_trim_is_missing(monkeypatch):
    collected: list[int] = []

    monkeypatch.setattr(memory_module.gc, 'collect', lambda: collected.append(1))
    monkeypatch.setattr(memory_module, '_malloc_trim', lambda: None)

    assert memory_module.release_memory() is False
    assert collected == [1]


def test_a_trim_that_raises_does_not_escape(monkeypatch):
    def broken(pad):
        raise RuntimeError('libc is not amused')

    monkeypatch.setattr(memory_module, '_malloc_trim', lambda: broken)

    assert memory_module.release_memory() is False


def test_trim_is_not_looked_up_on_a_platform_without_glibc(monkeypatch):
    monkeypatch.setattr(memory_module.platform, 'system', lambda: 'Darwin')

    assert memory_module._malloc_trim() is None


def test_trim_is_looked_up_once(monkeypatch):
    lookups: list[int] = []

    class Trim:
        argtypes = None
        restype = None

        def __call__(self, pad):
            return 0

    trim = Trim()

    def load(name):
        lookups.append(1)
        return trim

    monkeypatch.setattr(memory_module.platform, 'system', lambda: 'Linux')
    monkeypatch.setattr(memory_module.ctypes.util, 'find_library', load)
    monkeypatch.setattr(memory_module.ctypes, 'CDLL', lambda path: type('Libc', (), {'malloc_trim': trim})())

    assert memory_module._malloc_trim() is trim
    assert memory_module._malloc_trim() is trim
    assert lookups == [1]


def test_freeze_walks_the_live_objects_out_of_the_collector(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(memory_module.gc, 'collect', lambda: calls.append('collect'))
    monkeypatch.setattr(memory_module.gc, 'freeze', lambda: calls.append('freeze'))

    memory_module.freeze()

    assert calls == ['collect', 'freeze']


def test_freezing_leaves_the_live_objects_alive():
    keeper = ['kept']

    memory_module.freeze()
    gc.collect()

    assert keeper == ['kept']
