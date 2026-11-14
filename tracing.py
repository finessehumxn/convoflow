"""OpenTelemetry tracing, on when OTEL_EXPORTER_OTLP_ENDPOINT is set and a no-op otherwise.

The API opens one span per request and stamps its trace and span ids on every
event it queues. A worker batch span then links to each message it stored, so
a single message can be followed from the HTTP call to the database write.
"""
import contextlib
import os
import uuid

_tracer = None


def configure(exporter=None):
    """Sets up the tracer. Tests pass an in-memory exporter; production uses OTLP over HTTP."""
    global _tracer
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor

    provider = TracerProvider(resource=Resource.create({"service.name": os.environ.get("OTEL_SERVICE_NAME", "convoflow")}))
    if exporter is None:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    else:
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    _tracer = provider.get_tracer("convoflow")
    return provider


def _get():
    global _tracer
    if _tracer is None:
        _tracer = False
        if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
            try:
                configure()
            except ImportError:
                _tracer = False
    return _tracer


def disable():
    global _tracer
    _tracer = False


class _Ids:
    """What a request stamps on its events. Falls back to random ids when tracing is off."""

    def __init__(self, span=None):
        if span is not None:
            ctx = span.get_span_context()
            self.trace_id, self.span_id = f"{ctx.trace_id:032x}", f"{ctx.span_id:016x}"
        else:
            self.trace_id, self.span_id = uuid.uuid4().hex, None


@contextlib.contextmanager
def request_span(name, **attrs):
    tracer = _get()
    if not tracer:
        yield _Ids()
        return
    with tracer.start_as_current_span(name, attributes=attrs) as span:
        yield _Ids(span)


@contextlib.contextmanager
def batch_span(name, events, **attrs):
    """One span per worker batch, linked to the request span of each message it holds."""
    tracer = _get()
    if not tracer:
        yield None
        return
    from opentelemetry.trace import Link, SpanContext, TraceFlags

    links = []
    for e in events[:128]:
        try:
            links.append(Link(SpanContext(int(e["trace_id"], 16), int(e["span_id"], 16), is_remote=True,
                                          trace_flags=TraceFlags(TraceFlags.SAMPLED))))
        except (KeyError, TypeError, ValueError):
            continue
    with tracer.start_as_current_span(name, links=links, attributes=attrs) as span:
        yield span
