# websotester

A local, browser-based WebSocket client for poking at `ws://` and `wss://` servers, ideal for debugging WebSocket code.

## Features

- Connect / disconnect to any WebSocket URL
- Send text frames from a text box (Ctrl+Enter)
- Drag and drop files (or click the drop zone) to send each file as a binary frame
- Console showing the exact HTTP handshake request and response, full text frames,
  a one-line summary (with length) per binary frame, and close codes/reasons

## Run

```sh
uv run websotester                 # http://127.0.0.1:8000/
uv run websotester --port 9000     # choose another port
uv run websotester --host 0.0.0.0  # expose on the LAN (no auth, be careful)
```

## How it works

Browsers don't expose the raw WebSocket handshake, so the page doesn't connect to
the target itself. It talks to the local Python server over `/control`. The server
opens the real connection with the [`websockets`](https://websockets.readthedocs.io/)
library, records the handshake request and response (including redirects and
rejected handshakes), and relays frames in both directions.

The `User-Agent` and handshake headers come from Python's `websockets` library, not
from your browser. Messages of up to 256 MiB are allowed on both legs.
