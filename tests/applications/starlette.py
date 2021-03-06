import typing

from ddtrace import Tracer
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response

application = Starlette()


@application.route("/example")
async def example(request: Request) -> Response:
    return PlainTextResponse("Hello, world!")


@application.route("/child")
async def child(request: Request) -> Response:
    tracer: Tracer = request["ddtrace_asgi.tracer"]
    with tracer.trace("asgi.request.child", resource="child") as span:
        span.set_tag("hello", "world")
        return PlainTextResponse("Hello, child!")


@application.route("/exception")
async def exception(request: Request) -> typing.NoReturn:
    raise RuntimeError("Oops")
