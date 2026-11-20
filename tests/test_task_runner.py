import asyncio

import pytest
from rqark_app.cloud.client import DataCloudError, RQArkDataCloudClient

from rqark_task import runner as runner_module
from rqark_task.runner import ClaimedRun, InternalClient, RunnerError, execute

RUN_ID = 'run-1'
USER_ID = 'user-1'
TASK_ID = 'task-1'
SESSION_ID = 'session-1'

PAYLOAD = {
    'run_id': RUN_ID,
    'user_id': USER_ID,
    'task': {'id': TASK_ID, 'name': 'Morning brief', 'prompt': 'Summarise my inbox.', 'agent_name': 'assistant', 'provider_name': 'openai'},
    'agent_def': {'id': 'agent-1', 'name': 'assistant', 'description': '', 'prompts': [], 'tools': []},
    'provider_def': {'id': 'provider-1', 'name': 'openai', 'model_name': 'gpt-5', 'api_key': 'sk-test', 'api_base': 'https://example.com', 'priority': 1, 'combine': False},
    'session_id': SESSION_ID,
    'actor_token': 'run-token',
}


class FakeClient:
    def __init__(self):
        self.finalized: list[tuple[str, str, str, str, str | None]] = []
        self.claimed: list[int] = []

    async def claim(self, limit):
        self.claimed.append(limit)
        return []

    async def finalize(self, task_id, run_id, status, error='', session_id=None):
        self.finalized.append((task_id, run_id, status, error, session_id))


class FakeStorage:
    def __init__(self):
        self.saved = []

    async def save(self, session):
        self.saved.append(session)


class FakeWorkspace:
    instances: list['FakeWorkspace'] = []

    def __init__(self, user_id, image=''):
        self.user_id = user_id
        self.image = image
        self.ensured = False
        FakeWorkspace.instances.append(self)

    async def ensure(self):
        self.ensured = True


class FakeSessionManager:
    def __init__(self):
        self.sessions = {}

    async def get_or_create(self, session_id):
        session = self.sessions.get(session_id)
        if session is None:
            session = runner_module.Session(uuid=session_id)
            self.sessions[session_id] = session
        return session


class FakeApp:
    behavior = None
    instances: list['FakeApp'] = []

    def __init__(self, storage):
        self.storage = storage
        self.session_mgr = FakeSessionManager()
        self.handlers = {}
        FakeApp.instances.append(self)

    def on_content(self):
        def register(handler):
            self.handlers['content'] = handler
            return handler

        return register

    async def emit_content(self, text, path=()):
        handler = self.handlers.get('content')
        if handler:
            await handler(type('Ev', (), {'path': path, 'payload': runner_module.Chunk('content', text)})())

    async def chat(self, content, agent, session_id=None, **kwargs):
        await self.emit_content('half an ')
        await self.emit_content('answer')
        if FakeApp.behavior == 'timeout':
            await asyncio.sleep(30)
        if FakeApp.behavior == 'fail':
            raise RuntimeError('provider exploded')
        if FakeApp.behavior == 'hang':
            await asyncio.sleep(30)


class SpyDataCloudClient(RQArkDataCloudClient):
    """A data cloud client that records its calls and never reaches the network."""

    instances: list['SpyDataCloudClient'] = []

    def __init__(self, config, timeout=30.0, transport=None):
        self.config = config
        self.timeout = timeout
        self.calls: list[str] = []
        SpyDataCloudClient.instances.append(self)

    async def list_agents(self, *args, **kwargs):
        self.calls.append('list_agents')
        raise AssertionError('the runner must not list agents')

    async def list_providers(self, *args, **kwargs):
        self.calls.append('list_providers')
        raise AssertionError('the runner must not list providers')

    async def create_session(self, *args, **kwargs):
        self.calls.append('create_session')
        raise AssertionError('the runner must not create a session')

    async def update_session(self, *args, **kwargs):
        self.calls.append('update_session')


@pytest.fixture
def wired(monkeypatch):
    FakeApp.behavior = None
    FakeApp.instances = []
    FakeWorkspace.instances = []
    SpyDataCloudClient.instances = []
    storages: list[FakeStorage] = []

    async def build_and_open(run):
        storage = FakeStorage()
        storages.append(storage)
        app = FakeApp(storage)
        workspace = FakeWorkspace(run.user_id)
        agent = type('Agent', (), {'name': 'assistant'})()
        await workspace.ensure()
        return agent, app, storage, workspace, {'agent_id': 'agent-1'}

    monkeypatch.setattr(runner_module, '_build_and_open', build_and_open)
    monkeypatch.setattr(runner_module, 'Workspace', FakeWorkspace)
    monkeypatch.setattr(runner_module, 'RQArkDataCloudClient', SpyDataCloudClient)

    return storages


async def test_a_successful_run_finalizes_ok(wired):
    client = FakeClient()

    await execute(client, ClaimedRun.from_payload(PAYLOAD))

    assert client.finalized == [(TASK_ID, RUN_ID, 'ok', '', SESSION_ID)]
    assert wired[0].saved == []
    assert FakeWorkspace.instances[0].ensured is True


async def test_a_timeout_is_reported_as_an_error_with_the_partial_text(wired, monkeypatch):
    monkeypatch.setattr(runner_module, 'TASK_RUN_TIMEOUT', 0.05)
    FakeApp.behavior = 'timeout'
    client = FakeClient()

    await execute(client, ClaimedRun.from_payload(PAYLOAD))

    assert client.finalized == [(TASK_ID, RUN_ID, 'error', 'timeout', SESSION_ID)]

    saved = wired[0].saved
    assert len(saved) == 1
    assert saved[0].history[0].content == 'Summarise my inbox.'
    assert saved[0].history[1].content == 'half an answer'
    assert saved[0].title == 'Morning brief'


async def test_a_failed_run_is_reported_with_its_reason(wired):
    FakeApp.behavior = 'fail'
    client = FakeClient()

    await execute(client, ClaimedRun.from_payload(PAYLOAD))

    status, error = client.finalized[0][2], client.finalized[0][3]
    assert status == 'error'
    assert 'provider exploded' in error


async def test_a_run_whose_agent_is_missing_never_starts(wired, monkeypatch):
    async def missing_agent(run):
        raise RunnerError('AGENT_NOT_FOUND')

    monkeypatch.setattr(runner_module, '_build_and_open', missing_agent)
    client = FakeClient()

    await execute(client, ClaimedRun.from_payload(PAYLOAD))

    assert client.finalized == [(TASK_ID, RUN_ID, 'error', 'AGENT_NOT_FOUND', SESSION_ID)]


async def test_a_run_whose_workspace_fails_is_reported_rather_than_raised(wired, monkeypatch):
    async def broken(run):
        raise RuntimeError('docker is down')

    monkeypatch.setattr(runner_module, '_build_and_open', broken)
    client = FakeClient()

    await execute(client, ClaimedRun.from_payload(PAYLOAD))

    assert client.finalized[0][:3] == (TASK_ID, RUN_ID, 'error')
    assert 'docker is down' in client.finalized[0][3]


async def test_a_cancelled_run_is_reported_as_cancelled_not_failed(wired):
    FakeApp.behavior = 'hang'
    client = FakeClient()

    task = asyncio.create_task(execute(client, ClaimedRun.from_payload(PAYLOAD)))
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert client.finalized == [(TASK_ID, RUN_ID, 'cancelled', '', SESSION_ID)]


async def test_a_cancel_during_preparation_still_finalizes(wired, monkeypatch):
    gate = asyncio.Event()

    async def slow_open(run):
        await gate.wait()
        raise RunnerError('never reached')

    monkeypatch.setattr(runner_module, '_build_and_open', slow_open)
    client = FakeClient()

    task = asyncio.create_task(execute(client, ClaimedRun.from_payload(PAYLOAD)))
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert client.finalized == [(TASK_ID, RUN_ID, 'cancelled', '', SESSION_ID)]


async def test_the_runner_never_stops_the_workspace(wired):
    """Two runs of one user share a sandbox, so a finishing run must not stop it."""
    client = FakeClient()

    await execute(client, ClaimedRun.from_payload(PAYLOAD))

    assert not hasattr(FakeWorkspace.instances[0], 'stopped')


async def test_the_runner_builds_its_client_from_the_run_token(monkeypatch):
    monkeypatch.setattr(runner_module, 'Workspace', FakeWorkspace)
    monkeypatch.setattr(runner_module, 'RQArkDataCloudClient', SpyDataCloudClient)
    FakeWorkspace.instances = []
    SpyDataCloudClient.instances = []

    run = ClaimedRun.from_payload(PAYLOAD)
    _, app, storage, _, _ = runner_module._build_app(run)

    assert SpyDataCloudClient.instances[0].config.api_key == 'run-token'
    assert SpyDataCloudClient.instances[0].calls == []
    assert storage.source == 'TASK'
    assert app.provider_mgr.provider.name == 'openai'
    # The run's model calls go through the data cloud proxy, so the provider's
    # own key never leaves the data cloud.
    assert app.cloud_client is SpyDataCloudClient.instances[0]


async def test_the_sandbox_is_named_after_the_claiming_user(monkeypatch):
    monkeypatch.setattr(runner_module, 'Workspace', FakeWorkspace)
    monkeypatch.setattr(runner_module, 'RQArkDataCloudClient', SpyDataCloudClient)
    FakeWorkspace.instances = []

    runner_module._build_app(ClaimedRun.from_payload(PAYLOAD))

    assert FakeWorkspace.instances[0].user_id == USER_ID


async def test_a_finalize_that_is_refused_does_not_escape(wired):
    class RefusingClient(FakeClient):
        async def finalize(self, *args, **kwargs):
            raise DataCloudError(409, 'CONFLICT', 'This run is no longer running.')

    await execute(RefusingClient(), ClaimedRun.from_payload(PAYLOAD))


async def test_a_finalize_that_cannot_reach_the_cloud_does_not_escape(wired):
    class BrokenClient(FakeClient):
        async def finalize(self, *args, **kwargs):
            raise DataCloudError(0, 'REQUEST_FAILED', 'cannot reach data cloud')

    await execute(BrokenClient(), ClaimedRun.from_payload(PAYLOAD))


async def test_the_claim_payload_is_read_into_its_parts():
    run = ClaimedRun.from_payload(PAYLOAD)

    assert run.run_id == RUN_ID
    assert run.user_id == USER_ID
    assert run.task_id == TASK_ID
    assert run.task_name == 'Morning brief'
    assert run.prompt == 'Summarise my inbox.'
    assert run.session_id == SESSION_ID
    assert run.actor_token == 'run-token'
    assert run.agent_def['name'] == 'assistant'
    assert run.provider_def['model_name'] == 'gpt-5'


def test_the_internal_client_sends_the_header_and_never_a_bearer_token():
    client = InternalClient(url='http://data-cloud:7302', token='shared-secret')

    assert client.api_url == 'http://data-cloud:7302/api'
    assert client._headers() == {'X-Internal-Token': 'shared-secret'}


async def test_run_once_claims_then_executes(monkeypatch):
    executed: list[str] = []

    class OneRunClient(FakeClient):
        async def claim(self, limit):
            self.claimed.append(limit)
            return [ClaimedRun.from_payload(PAYLOAD)]

    client = OneRunClient()

    async def fake_execute(inner, run):
        executed.append(run.run_id)

    monkeypatch.setattr(runner_module, 'InternalClient', lambda: client)
    monkeypatch.setattr(runner_module, 'execute', fake_execute)

    assert await runner_module.run_once(2) == 1
    assert client.claimed == [2]
    assert executed == [RUN_ID]


class CloseSpy:
    def __init__(self):
        self.closed = False

    async def aclose(self):
        self.closed = True


def _with_client(monkeypatch, workspace):
    spy = CloseSpy()

    async def build_and_open(run):
        app = FakeApp(FakeStorage())
        return type('Agent', (), {'name': 'assistant'})(), app, FakeStorage(), workspace, {runner_module.CLIENT_KEY: spy}

    monkeypatch.setattr(runner_module, '_build_and_open', build_and_open)
    return spy


async def test_a_finished_run_closes_its_data_cloud_client(monkeypatch):
    spy = _with_client(monkeypatch, FakeWorkspace(USER_ID))

    await execute(FakeClient(), ClaimedRun.from_payload(PAYLOAD))

    assert spy.closed is True


async def test_a_workspace_that_never_starts_closes_the_client_and_reports_it(monkeypatch):
    class StuckWorkspace:
        def __init__(self, user_id, image=''):
            pass

        async def ensure(self):
            await asyncio.sleep(30)

    spy = CloseSpy()

    def build_app(run):
        app = FakeApp(FakeStorage())
        return type('Agent', (), {'name': 'assistant'})(), app, FakeStorage(), StuckWorkspace(run.user_id), {runner_module.CLIENT_KEY: spy}

    monkeypatch.setattr(runner_module, '_build_app', build_app)
    monkeypatch.setattr(runner_module, 'WORKSPACE_START_TIMEOUT', 0.05)
    client = FakeClient()

    await execute(client, ClaimedRun.from_payload(PAYLOAD))

    assert client.finalized == [(TASK_ID, RUN_ID, 'error', 'WORKSPACE_TIMEOUT', SESSION_ID)]
    assert spy.closed is True


def test_the_local_stream_stops_growing_at_its_limit():
    stream = runner_module.LocalStream(max_chars=10)

    stream._append('abc')
    stream._append('defghij')
    stream._append('more')

    assert stream.text == 'abcdefghij'
    assert stream.truncated is True


def test_the_local_stream_keeps_everything_under_its_limit():
    stream = runner_module.LocalStream(max_chars=10)

    stream._append('abc')
    stream._append('def')

    assert stream.text == 'abcdef'
    assert stream.truncated is False


async def test_a_finished_run_releases_its_memory(wired, monkeypatch):
    released: list[int] = []
    monkeypatch.setattr(runner_module, 'release_memory', lambda: released.append(1))

    await execute(FakeClient(), ClaimedRun.from_payload(PAYLOAD))

    assert released == [1]


async def test_a_run_releases_its_memory_even_when_the_cleanup_fails(wired, monkeypatch):
    released: list[int] = []

    async def broken_cleanup(coro):
        coro.close()
        raise RuntimeError('finalize is broken')

    monkeypatch.setattr(runner_module, '_run_to_completion', broken_cleanup)
    monkeypatch.setattr(runner_module, 'release_memory', lambda: released.append(1))

    with pytest.raises(RuntimeError):
        await execute(FakeClient(), ClaimedRun.from_payload(PAYLOAD))

    assert released == [1]


def test_a_dropped_stream_keeps_nothing():
    stream = runner_module.LocalStream(max_chars=100)

    stream._append('half an answer')
    stream.drop()

    assert stream.text == ''


def test_the_internal_client_keeps_one_pool():
    client = InternalClient(url='http://data-cloud:7302', token='shared-secret')

    assert client._http() is client._http()


async def test_closing_the_internal_client_closes_its_pool_and_rebuilds_it():
    client = InternalClient(url='http://data-cloud:7302', token='shared-secret')
    pool = client._http()

    await client.aclose()

    assert pool.is_closed is True
    assert client._http() is not pool


async def test_closing_the_internal_client_twice_is_harmless():
    client = InternalClient(url='http://data-cloud:7302', token='shared-secret')

    await client.aclose()
    await client.aclose()


async def test_run_once_closes_its_client(monkeypatch):
    closed: list[int] = []

    class CountingClient(FakeClient):
        async def aclose(self):
            closed.append(1)

    monkeypatch.setattr(runner_module, 'InternalClient', CountingClient)

    await runner_module.run_once(1)

    assert closed == [1]
