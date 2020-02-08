import re

from ddtrace import Tracer
from starlette.types import Receive, Scope, Send


async def hello_world(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"text/plain"]],
        }
    )
    await send({"type": "http.response.body", "body": b"Hello, world!"})


async def child(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    tracer: Tracer = scope["ddtrace_asgi.tracer"]
    with tracer.trace("asgi.request.child", resource="child") as span:
        span.set_tag("hello", "world")
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [[b"content-type", b"text/plain"]],
            }
        )
        await send({"type": "http.response.body", "body": b"Hello, child!"})


async def exception(scope: Scope, receive: Receive, send: Send) -> None:
    exc = RuntimeError("Oops")
    await send(
        {
            "type": "http.response.start",
            "status": 500,
            "headers": [[b"content-type", b"text/plain"]],
        }
    )
    await send({"type": "http.response.body", "body": str(exc).encode()})
    raise exc


async def path_parameters(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    parameter = scope["path"].split("/")[2]
    if scope["method"] == "GET":
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [[b"content-type", b"text/plain"]],
            }
        )
        await send(
            {"type": "http.response.body", "body": f"Hello, {parameter}!".encode()}
        )
    else:
        await send(
            {
                "type": "http.response.start",
                "status": 405,
                "headers": [[b"content-type", b"text/plain"]],
            }
        )
        await send({"type": "http.response.body", "body": b"Method No Allowed"})


async def application(scope: Scope, receive: Receive, send: Send) -> None:
    if scope["path"] == "/child":
        await child(scope, receive, send)
    elif scope["path"] == "/exception":
        await exception(scope, receive, send)
    elif re.match(r"/path-parameters/[^/]+", scope["path"]):
        await path_parameters(scope, receive, send)
    else:
        await hello_world(scope, receive, send)
