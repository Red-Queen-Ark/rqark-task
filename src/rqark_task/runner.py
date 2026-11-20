import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import httpx
from rqark_app.app import AgentApp
from rqark_app.cloud.client import DataCloudError, RQArkDataCloudClient
from rqark_app.config.manager import _build_agent, _build_provider
from rqark_app.session.storage import CloudStorage
from rqark_app.tools.context import AGENT_KEY, CLIENT_KEY, SOURCE_KEY
from rqark_app.types.cloud import CloudConfig
from rqark_app.types.session import Session
from rqark_core.types.chat import AssistantMessage, Chunk, UserMessage
from rqark_sandbox import swap_agent
from rqark_sandbox.workspace import Workspace

from rqark_task.config import DATA_CLOUD_TIMEOUT, DATA_CLOUD_URL, INTERNAL_TOKEN, SANDBOX_ENABLED, SANDBOX_IMAGE, TASK_RUN_TIMEOUT, TASK_STREAM_MAX_CHARS, WORKSPACE_START_TIMEOUT
from rqark_task.memory import release_memory

logger = logging.getLogger(__name__)

TASK_SOURCE = 'TASK'

RUN_OK = 'ok'
RUN_ERROR = 'error'
RUN_TIMEOUT = 'timeout'
RUN_CANCELLED = 'cancelled'


class RunnerError(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass
class ClaimedRun:
    run_id: str
    user_id: str
    task_id: str
    task_name: str
    prompt: str
    session_id: str
    actor_token: str
    agent_def: dict[str, Any]
    provider_def: dict[str, Any]

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> 'ClaimedRun':
        task = payload.get('task') or {}
        return cls(
            run_id=str(payload['run_id']),
            user_id=str(payload['user_id']),
            task_id=str(task['id']),
            task_name=str(task.get('name') or ''),
            prompt=str(task.get('prompt') or ''),
            session_id=str(payload['session_id']),
            actor_token=str(payload['actor_token']),
            agent_def=payload.get('agent_def') or {},
            provider_def=payload.get('provider_def') or {},
        )


class InternalClient:
    def __init__(self, url: str = DATA_CLOUD_URL, token: str = INTERNAL_TOKEN, timeout: float = DATA_CLOUD_TIMEOUT):
        self.url = url.rstrip('/')
        self.token = token
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None

    @property
    def api_url(self) -> str:
        return self.url if self.url.endswith('/api') else f'{self.url}/api'

    def _headers(self) -> dict[str, str]:
        return {'X-Internal-Token': self.token}

    def _http(self) -> httpx.AsyncClient:
        """The one pool this client keeps, instead of one per call.

        A tick builds an HTTP client, a pool and its buffers, then throws them
        away, and the next tick does it again; on a worker that idles at one
        claim every thirty seconds that churn is the whole steady-state cost.
        """
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    async def aclose(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            await client.aclose()

    async def claim(self, limit: int) -> list[ClaimedRun]:
        response = await self._http().post(f'{self.api_url}/internal/tasks/claim', json={'limit': limit}, headers=self._headers())
        response.raise_for_status()
        body = response.json()
        return [ClaimedRun.from_payload(item) for item in body.get('runs') or []]

    async def finalize(self, task_id: str, run_id: str, status: str, error: str = '', session_id: str | None = None) -> None:
        payload: dict[str, Any] = {'status': status, 'error': error}
        if session_id is not None:
            payload['session_id'] = session_id
        response = await self._http().patch(f'{self.api_url}/internal/tasks/{task_id}/runs/{run_id}', json=payload, headers=self._headers())
        response.raise_for_status()


class LocalStream:
    """Collects the run's final text without keeping an unbounded buffer.

    A runaway agent can stream for the whole TASK_RUN_TIMEOUT; the transcript
    only needs a sane prefix of it, so the buffer stops growing at max_chars.
    """

    def __init__(self, max_chars: int = TASK_STREAM_MAX_CHARS) -> None:
        self._chunks: list[str] = []
        self._size = 0
        self._max = max_chars
        self.truncated = False

    @property
    def text(self) -> str:
        return ''.join(self._chunks)

    def drop(self) -> None:
        """Drops the kept prefix once the transcript has been written."""
        self._chunks.clear()
        self._size = 0

    def _append(self, text: str) -> None:
        if self._max <= 0:
            self._chunks.append(text)
            return
        if self.truncated or not text:
            return
        room = self._max - self._size
        if room <= 0:
            self.truncated = True
            logger.warning('run output exceeded %d chars, dropping the rest', self._max)
            return
        if len(text) > room:
            text = text[:room]
            self.truncated = True
            logger.warning('run output exceeded %d chars, dropping the rest', self._max)
        self._chunks.append(text)
        self._size += len(text)

    def connect(self, app: AgentApp) -> None:
        @app.on_content()
        async def content(ev: Any) -> None:
            if ev.path:
                return
            chunk = ev.payload
            if isinstance(chunk, Chunk) and chunk.type == 'content':
                self._append(str(chunk.content))


def _build_app(run: ClaimedRun) -> tuple[Any, AgentApp, CloudStorage, Workspace | None, dict[str, object]]:
    try:
        agent = _build_agent(run.agent_def)
    except (KeyError, ValueError) as error:
        raise RunnerError('AGENT_NOT_FOUND') from error
    try:
        provider = _build_provider(run.provider_def)
    except (KeyError, TypeError, ValueError) as error:
        raise RunnerError('PROVIDER_NOT_FOUND') from error

    workspace = Workspace(run.user_id, image=SANDBOX_IMAGE) if SANDBOX_ENABLED else None
    if workspace is not None:
        swap_agent(agent, workspace)

    client = RQArkDataCloudClient(CloudConfig(url=DATA_CLOUD_URL, api_key=run.actor_token), timeout=DATA_CLOUD_TIMEOUT)
    storage = CloudStorage(client, source=TASK_SOURCE)
    # The run's model calls go through the same proxy as a person's: the data
    # cloud holds the provider key, and the run token is what the proxy accepts.
    app = AgentApp([provider], storage, cloud_client=client)
    app.provider_mgr.switch_to(0)

    return agent, app, storage, workspace, {CLIENT_KEY: client, AGENT_KEY: str(run.agent_def.get('id') or ''), SOURCE_KEY: TASK_SOURCE}


async def _close_client(extras: dict[str, object] | None) -> None:
    """Releases the per-run data cloud client, whose httpx pool outlives a run otherwise."""
    client = (extras or {}).get(CLIENT_KEY)
    close = getattr(client, 'aclose', None)
    if close is None:
        return
    try:
        await close()
    except Exception:
        logger.exception('cannot close the data cloud client')


async def _build_and_open(run: ClaimedRun) -> tuple[Any, AgentApp, CloudStorage, Workspace | None, dict[str, object]]:
    agent, app, storage, workspace, extras = _build_app(run)
    if workspace is None:
        return agent, app, storage, workspace, extras
    try:
        await asyncio.wait_for(workspace.ensure(), WORKSPACE_START_TIMEOUT)
    except TimeoutError as error:
        await _close_client(extras)
        raise RunnerError('WORKSPACE_TIMEOUT') from error
    except BaseException:
        await _close_client(extras)
        raise
    return agent, app, storage, workspace, extras


async def _save_transcript(storage: CloudStorage, run: ClaimedRun, text: str) -> None:
    session = Session(uuid=run.session_id)
    session.title = run.task_name
    session.history = [UserMessage(run.prompt)]
    if text:
        session.history.append(AssistantMessage(text))
    await storage.save(session)


_background_cleanups: set[asyncio.Task] = set()


def _discard_cleanup(task: asyncio.Task) -> None:
    _background_cleanups.discard(task)
    if not task.cancelled():
        error = task.exception()
        if error is not None:
            logger.error('cleanup failed', exc_info=error)


async def _run_to_completion(coro: Any) -> None:
    """Runs a cleanup step to the end even if the caller is cancelled again.

    A shutdown cancels the run task; the cleanup below it then has to reach the
    data cloud, which is an await of its own. A second cancellation arriving while
    it is in flight would leave the run unfinalized, and the dispatcher would only
    learn of it when the stale sweep fired — a run reported as lost that in fact
    completed.

    When the second cancellation lands anyway, the cleanup keeps running in the
    background instead of being dropped: an unreferenced task can be collected
    mid-flight, and its exceptions would never be reported.
    """
    task = asyncio.create_task(coro)
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            _background_cleanups.add(task)
            task.add_done_callback(_discard_cleanup)
            logger.warning('cleanup is still running in the background')
        raise


async def _finalize(client: InternalClient, run: ClaimedRun, status: str, error: str) -> None:
    try:
        await client.finalize(run.task_id, run.run_id, status, error, run.session_id)
    except DataCloudError:
        logger.exception('finalize refused run=%s', run.run_id)
    except Exception:
        logger.exception('finalize failed run=%s', run.run_id)


async def execute(client: InternalClient, run: ClaimedRun) -> None:
    status = RUN_OK
    error = ''
    cancelled = False

    stream = LocalStream()
    storage: CloudStorage | None = None
    app: AgentApp | None = None
    agent: Any = None
    extras: dict[str, object] | None = None

    try:
        agent, app, storage, _, extras = await _build_and_open(run)
        stream.connect(app)

        session = await app.session_mgr.get_or_create(run.session_id)
        session.title = run.task_name
        await asyncio.wait_for(app.chat(run.prompt, agent, session_id=run.session_id, extras=extras), TASK_RUN_TIMEOUT)
    except RunnerError as failure:
        status, error = RUN_ERROR, failure.reason
    except TimeoutError:
        logger.warning('run timed out run=%s', run.run_id)
        status, error = RUN_ERROR, RUN_TIMEOUT
    except asyncio.CancelledError:
        logger.warning('run cancelled run=%s', run.run_id)
        status, error = RUN_CANCELLED, ''
        cancelled = True
    except Exception as failure:
        logger.exception('run failed run=%s', run.run_id)
        status, error = RUN_ERROR, str(failure) or RUN_ERROR
    finally:

        async def close_out() -> None:
            try:
                if storage is not None and status != RUN_OK:
                    try:
                        await _save_transcript(storage, run, stream.text)
                    except Exception:
                        logger.exception('cannot save the partial transcript run=%s', run.run_id)
                await _finalize(client, run, status, error)
            finally:
                await _close_client(extras)

        try:
            await _run_to_completion(close_out())
        finally:
            stream.drop()
            release_memory()

    if cancelled:
        raise asyncio.CancelledError


async def run_once(limit: int = 1) -> int:
    client = InternalClient()
    try:
        runs = await client.claim(limit)
        for run in runs:
            await execute(client, run)
        return len(runs)
    finally:
        close = getattr(client, 'aclose', None)
        if close is not None:
            await close()
