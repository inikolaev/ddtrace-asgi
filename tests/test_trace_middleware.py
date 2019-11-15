import importlib
import time
import typing

import httpx
import pytest
from ddtrace import tracer as global_tracer
from ddtrace.ext import http as http_ext
from ddtrace.propagation import http as http_propagation
from ddtrace.span import Span
from ddtrace.tracer import Tracer
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from ddtrace_asgi.middleware import TraceMiddleware
from tests.utils.asgi import mock_app, mock_http_scope, mock_receive, mock_send
from tests.utils.config import override_config
from tests.utils.tracer import DummyTracer

# TIP: use 'pytest -k <id>' to run tests for a given application only.
APPLICATIONS = [
    pytest.param("tests.applications.raw:application", id="raw"),
    pytest.param("tests.applications.starlette:application", id="starlette"),
    pytest.param("tests.applications.fastapi:application", id="fastapi"),
]


@pytest.fixture(name="application", params=APPLICATIONS)
def fixture_application(request: typing.Any) -> ASGIApp:
    module_path, app_name = request.param.split(":")
    module = importlib.import_module(module_path)
    app = getattr(module, app_name)
    return app


@pytest.fixture
def tracer() -> Tracer:
    return DummyTracer()


@pytest.fixture
def client(application: ASGIApp, tracer: Tracer) -> typing.Iterator[httpx.Client]:
    app = TraceMiddleware(application, tracer=tracer, service="test.asgi.service")
    with httpx.Client(app=app, base_url="http://testserver") as client:
        yield client


def test_app(client: httpx.Client, tracer: DummyTracer) -> None:
    r = client.get("/example")
    assert r.status_code == 200
    assert r.text == "Hello, world!"

    traces = tracer.writer.pop_traces()
    assert len(traces) == 1
    spans: typing.List[Span] = traces[0]
    assert len(spans) == 1
    span = spans[0]
    assert span.span_id
    assert span.trace_id
    assert span.parent_id is None
    assert span.name == "asgi.request"
    assert span.service == "test.asgi.service"
    assert span.resource == "GET /example"
    assert span.get_tag(http_ext.STATUS_CODE) == "200"
    assert span.get_tag(http_ext.URL) == "http://testserver/example"
    assert span.get_tag(http_ext.QUERY_STRING) is None


@pytest.mark.asyncio
async def test_invalid_asgi(tracer: Tracer) -> None:
    """Test that TraceMiddleware does not crash in case of ASGI protocol violation."""

    async def invalid(scope: Scope, receive: Receive, send: Send) -> None:
        for key in "type", "headers", "status":
            message = {"type": "http.response.start", "headers": [], "status": 200}
            message.pop(key)
            await send(message)

    app = TraceMiddleware(invalid, tracer=tracer)

    for key in "method", "path", "headers", "query_string":
        scope = dict(mock_http_scope)
        scope.pop(key)
        await app(scope, mock_receive, mock_send)

    await app(mock_http_scope, mock_receive, mock_send)


def test_child(client: httpx.Client, tracer: Tracer) -> None:
    start = time.time()
    r = client.get("/child")
    end = time.time()
    assert r.status_code == 200
    assert r.text == "Hello, child!"

    traces = tracer.writer.pop_traces()
    assert len(traces) == 1
    spans: typing.List[Span] = traces[0]
    assert len(spans) == 2
    spans_by_name = {s.name: s for s in spans}

    span = spans_by_name["asgi.request"]
    assert span.span_id
    assert span.trace_id
    assert span.parent_id is None
    assert span.service == "test.asgi.service"
    assert span.resource == "GET /child"
    assert span.get_tag("hello") is None
    assert span.start >= start
    assert span.duration <= end - start
    assert span.error == 0

    child_span = spans_by_name["asgi.request.child"]
    assert child_span.span_id
    assert child_span.trace_id == span.trace_id
    assert child_span.parent_id == span.span_id
    assert child_span.service == "test.asgi.service"
    assert child_span.resource == "child"
    assert child_span.get_tag("hello") == "world"
    assert child_span.start >= start
    assert child_span.duration <= end - start
    assert child_span.error == 0


@pytest.mark.asyncio
async def test_not_http_no_traces(tracer: Tracer) -> None:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        pass

    app = TraceMiddleware(app)

    scope = {"type": "lifespan"}
    await app(scope, mock_receive, mock_send)
    traces = tracer.writer.pop_traces()
    assert len(traces) == 0

    scope = {"type": "websocket"}
    await app(scope, mock_receive, mock_send)
    traces = tracer.writer.pop_traces()
    assert len(traces) == 0


def test_default_tracer() -> None:
    middleware = TraceMiddleware(app=mock_app)
    assert middleware.tracer is global_tracer


def test_default_service() -> None:
    middleware = TraceMiddleware(app=mock_app)
    assert middleware.service == "asgi"


@pytest.mark.asyncio
async def test_tracer_scope_item(tracer: Tracer) -> None:
    async def spy_app(scope: Scope, receive: Receive, send: Send) -> None:
        assert scope["type"] == "http"
        await send({"tracer": scope["ddtrace_asgi.tracer"]})

    messages = []

    async def send(message: Message) -> None:
        messages.append(message)

    app = TraceMiddleware(spy_app, tracer=tracer)
    await app(scope=mock_http_scope, receive=mock_receive, send=send)

    assert messages == [{"tracer": tracer}]


@pytest.fixture
def trace_query_string() -> typing.Iterator[None]:
    with override_config("asgi", trace_query_string=True):
        yield


@pytest.mark.usefixtures("trace_query_string")
def test_trace_query_string(client: httpx.Client, tracer: DummyTracer) -> None:
    r = client.get("/example", params={"foo": "bar"})
    assert r.status_code == 200
    assert r.text == "Hello, world!"

    traces = tracer.writer.pop_traces()
    assert len(traces) == 1
    spans: typing.List[Span] = traces[0]
    assert len(spans) == 1
    span = spans[0]
    assert span.get_tag(http_ext.QUERY_STRING) == "foo=bar"


def test_app_exception(client: httpx.Client, tracer: DummyTracer) -> None:
    with pytest.raises(RuntimeError):
        start = time.time()
        client.get("/exception")
    end = time.time()

    # Ensure any open span was closed.
    assert not tracer.current_span()

    traces = tracer.writer.pop_traces()
    assert len(traces) == 1
    spans: typing.List[Span] = traces[0]
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "asgi.request"
    assert span.service == "test.asgi.service"
    assert span.resource == "GET /exception"
    assert span.start >= start
    assert span.duration <= end - start
    assert span.error == 1
    assert span.get_tag(http_ext.STATUS_CODE) == "500"
    assert span.get_tag(http_ext.METHOD) == "GET"


def test_distributed_tracing(client: httpx.Client, tracer: DummyTracer) -> None:
    headers = {
        http_propagation.HTTP_HEADER_TRACE_ID: "1234",
        http_propagation.HTTP_HEADER_PARENT_ID: "5678",
    }
    r = client.get("/example", headers=headers)
    assert r.status_code == 200
    assert r.text == "Hello, world!"

    traces = tracer.writer.pop_traces()
    assert len(traces) == 1
    spans: typing.List[Span] = traces[0]
    assert len(spans) == 1
    span = spans[0]
    assert span.trace_id == 1234
    assert span.parent_id == 5678


@pytest.mark.parametrize(
    "tags, expected_tags",
    [
        ("", {}),
        ({}, {}),
        ("env:testing", {"env": "testing"}),
        ({"env": "testing"}, {"env": "testing"}),
        ({"env": "testing", "live": "false"}, {"env": "testing", "live": "false"}),
        ("env:testing,live:false", {"env": "testing", "live": "false"}),
        ("env:testing, live:false", {"env": "testing", "live": "false"}),
        ("env:staging:east", {"env": "staging:east"}),
        ("env-testing", ValueError),
    ],
)
def test_tags(
    application: ASGIApp,
    tracer: DummyTracer,
    tags: typing.Union[str, dict],
    expected_tags: typing.Any,
) -> None:
    if expected_tags is ValueError:
        with pytest.raises(ValueError):
            TraceMiddleware(application, tags=tags)
        return

    assert isinstance(expected_tags, dict)

    app = TraceMiddleware(
        application, tracer=tracer, service="test.asgi.service", tags=tags,
    )

    with httpx.Client(app=app, base_url="http://testserver") as client:
        r = client.get("/example")
        assert r.status_code == 200
        assert r.text == "Hello, world!"

        traces = tracer.writer.pop_traces()
        assert len(traces) == 1
        spans: typing.List[Span] = traces[0]
        assert len(spans) == 1
        span = spans[0]
        assert span.name == "asgi.request"
        assert span.service == "test.asgi.service"
        assert span.resource == "GET /example"
        for key, value in expected_tags.items():
            assert span.get_tag(key) == value
