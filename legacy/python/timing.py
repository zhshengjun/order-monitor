"""Latency logs with scoped identifiers; never log request arguments or exceptions."""
import contextlib
import contextvars
import logging
import time

LOG = logging.getLogger('monitor')
_CONTEXT = contextvars.ContextVar('timing_context', default={})


@contextlib.contextmanager
def timing_context(**fields):
    token = _CONTEXT.set({**_CONTEXT.get(), **fields})
    try:
        yield
    finally:
        _CONTEXT.reset(token)


def timing_event(stage, **fields):
    values = {**_CONTEXT.get(), **fields}
    LOG.info('timing stage=%s %s', stage, ' '.join('%s=%s' % (key, value) for key, value in values.items()))


@contextlib.contextmanager
def timed(stage, **fields):
    start = time.monotonic()
    outcome = 'ok'
    try:
        yield
    except BaseException:
        outcome = 'error'
        raise
    finally:
        timing_event(stage, **fields, outcome=outcome, duration_ms=round((time.monotonic()-start)*1000, 3))
