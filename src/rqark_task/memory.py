import ctypes
import ctypes.util
import gc
import logging
import platform
from collections.abc import Callable

logger = logging.getLogger(__name__)

_trim_loaded = False
_trim: Callable[[int], int] | None = None


def _malloc_trim() -> Callable[[int], int] | None:
    """The glibc call that hands free heap pages back to the kernel.

    Freed memory stays in the process until something asks for it back. A run
    builds an agent, a provider, a sandbox client and a whole transcript, and
    each of those leaves a mountain of small freed blocks behind; without this
    call the resident size keeps the peak of the largest run forever, which is
    what a worker that runs one task a day ends up sitting at.

    The symbol only exists on glibc, so anything else (musl, macOS) gets nothing
    rather than an error: the garbage collector still ran, it is only the return
    to the kernel that is skipped.
    """
    global _trim_loaded, _trim
    if _trim_loaded:
        return _trim
    _trim_loaded = True
    if platform.system() != 'Linux':
        return None
    try:
        libc = ctypes.CDLL(ctypes.util.find_library('c') or 'libc.so.6')
    except OSError:
        logger.debug('cannot load libc, skipping malloc_trim')
        return None
    trim = getattr(libc, 'malloc_trim', None)
    if trim is None:
        return None
    trim.argtypes = [ctypes.c_size_t]
    trim.restype = ctypes.c_int
    _trim = trim
    return _trim


def release_memory() -> bool:
    """Collect what a finished run left behind and return the empty pages.

    Returns whether the kernel actually got anything back, so a caller can log
    a release that did not happen without having to know how it works.
    """
    gc.collect()
    trim = _malloc_trim()
    if trim is None:
        return False
    try:
        return bool(trim(0))
    except Exception:
        logger.exception('malloc_trim failed')
        return False


def freeze() -> None:
    """Move everything already alive out of the collector's scan set.

    A worker imports its whole dependency tree before it claims anything, and
    none of that is ever collected. Freezing it at startup keeps every later
    collection from walking it.
    """
    gc.collect()
    gc.freeze()
