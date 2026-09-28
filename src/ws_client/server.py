"""Local web UI for driving an outbound WebSocket connection.

The browser talks to this server over a local WebSocket (``/control``). The
server opens the real connection to the target URL with the ``websockets``
library, which lets us report the exact HTTP handshake request and response —
something the browser's native WebSocket API does not expose.

Control protocol (browser -> server):
  text   {"type": "connect", "url": "..."}
  text   {"type": "disconnect"}
  text   {"type": "send_text", "data": "..."}
  binary payload is forwarded to the target as a single binary frame

Events (server -> browser), all JSON text:
  {"type": "status", "state": "connecting" | "connected" | "disconnected"}
  {"type": "log", "kind": <kind>, "text": "...", ...}
"""

import argparse
import asyncio
import json
from importlib.resources import files
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.responses import HTMLResponse
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidURI
from websockets.http11 import Request, Response
from websockets.uri import parse_uri

MAX_MESSAGE_SIZE = 256 * 1024 * 1024  # 256 MiB, for both legs


def format_request(request: Request) -> str:
    return f"GET {request.path} HTTP/1.1\r\n{request.headers}"


def format_response(response: Response) -> str:
    text = f"HTTP/1.1 {response.status_code} {response.reason_phrase}\r\n{response.headers}"
    if response.body:
        text += response.body.decode("utf-8", errors="replace")
    return text


class Session:
    """One browser tab, owning at most one outbound WebSocket connection."""

    def __init__(self, browser: WebSocket) -> None:
        self.browser = browser
        self.send_lock = asyncio.Lock()
        self.target: ClientConnection | None = None
        self.reader: asyncio.Task[None] | None = None

    async def emit(self, **event: Any) -> None:
        async with self.send_lock:
            try:
                await self.browser.send_text(json.dumps(event))
            except (WebSocketDisconnect, RuntimeError):
                pass  # browser went away; the main loop will clean up

    async def log(self, kind: str, text: str, **extra: Any) -> None:
        await self.emit(type="log", kind=kind, text=text, **extra)

    async def status(self, state: str) -> None:
        await self.emit(type="status", state=state)

    def make_connection_class(self) -> type[ClientConnection]:
        session = self

        class CapturingConnection(ClientConnection):
            async def handshake(self, *args: Any, **kwargs: Any) -> None:
                try:
                    await super().handshake(*args, **kwargs)
                finally:
                    # Runs for every attempt, including redirects and failures.
                    if self.request is not None:
                        await session.log("request", format_request(self.request))
                    if self.response is not None:
                        await session.log("response", format_response(self.response))

        return CapturingConnection

    async def connect(self, url: str) -> None:
        if self.target is not None:
            await self.log("error", "Already connected; disconnect first.")
            return
        url = url.strip()
        try:
            parse_uri(url)
        except InvalidURI as exc:
            await self.log("error", f"Invalid URL: {exc}")
            return

        await self.status("connecting")
        await self.log("info", f"Connecting to {url}")
        try:
            self.target = await connect(
                url,
                create_connection=self.make_connection_class(),
                max_size=MAX_MESSAGE_SIZE,
                open_timeout=15,
                close_timeout=2,
            )
        except Exception as exc:
            await self.log("error", f"Connection failed: {type(exc).__name__}: {exc}")
            await self.status("disconnected")
            return

        await self.log("info", "Handshake complete; connection open.")
        await self.status("connected")
        self.reader = asyncio.create_task(self.read_target(self.target))

    async def read_target(self, target: ClientConnection) -> None:
        try:
            async for message in target:
                if isinstance(message, str):
                    await self.log("recv-text", message, length=len(message.encode()))
                else:
                    await self.log("recv-binary", f"Binary frame received: {len(message):,} bytes",
                                   length=len(message))
        except ConnectionClosed:
            pass
        except Exception as exc:
            await self.log("error", f"Receive error: {type(exc).__name__}: {exc}")
        finally:
            await self.report_close(target)
            if self.target is target:
                self.target = None
                self.reader = None
            await self.status("disconnected")

    async def report_close(self, target: ClientConnection) -> None:
        protocol = target.protocol
        rcvd, sent = protocol.close_rcvd, protocol.close_sent
        if rcvd is not None:
            who = "server" if protocol.close_rcvd_then_sent or sent is None else "client"
            reason = f" ({rcvd.reason})" if rcvd.reason else ""
            await self.log("info", f"Connection closed by {who}: code {rcvd.code}{reason}")
        elif sent is not None:
            await self.log("info", f"Connection closed: sent code {sent.code}, no close frame received")
        else:
            await self.log("info", "Connection lost (no close frame).")

    async def disconnect(self) -> None:
        target, reader = self.target, self.reader
        if target is None:
            await self.log("error", "Not connected.")
            return
        await self.log("info", "Closing connection (code 1000)...")
        await target.close()
        if reader is not None:
            await reader

    async def send(self, data: str | bytes, name: str | None = None) -> None:
        if self.target is None:
            await self.log("error", "Not connected.")
            return
        try:
            await self.target.send(data)
        except ConnectionClosed:
            await self.log("error", "Send failed: connection is closed.")
            return
        if isinstance(data, str):
            await self.log("sent-text", data, length=len(data.encode()))
        else:
            label = f" ({name})" if name else ""
            await self.log("sent-binary", f"Binary frame sent{label}: {len(data):,} bytes", length=len(data))

    async def run(self) -> None:
        pending_name: str | None = None
        try:
            while True:
                message = await self.browser.receive()
                if message["type"] == "websocket.disconnect":
                    break
                if message.get("bytes") is not None:
                    await self.send(message["bytes"], pending_name)
                    pending_name = None
                    continue
                try:
                    cmd = json.loads(message.get("text") or "")
                except json.JSONDecodeError:
                    continue
                match cmd.get("type"):
                    case "connect":
                        await self.connect(str(cmd.get("url", "")))
                    case "disconnect":
                        await self.disconnect()
                    case "send_text":
                        await self.send(str(cmd.get("data", "")))
                    case "file_name":
                        # Sent right before a binary payload so the log can name the file.
                        pending_name = str(cmd.get("name", "")) or None
        finally:
            if self.target is not None:
                await self.target.close()
            if self.reader is not None:
                await self.reader


async def index(_request) -> HTMLResponse:
    html = files("ws_client").joinpath("static/index.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


async def control(websocket: WebSocket) -> None:
    await websocket.accept()
    await Session(websocket).run()


app = Starlette(routes=[Route("/", index), WebSocketRoute("/control", control)])


def main() -> None:
    parser = argparse.ArgumentParser(description="Local web-based WebSocket client")
    parser.add_argument("--host", default="127.0.0.1", help="interface to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="port to listen on (default: 8000)")
    args = parser.parse_args()
    print(f"WebSocket client UI: http://{args.host}:{args.port}/")
    uvicorn.run(app, host=args.host, port=args.port, ws_max_size=MAX_MESSAGE_SIZE, log_level="warning")
