# PixelMug HA Connector

PixelMug HA Connector is a Home Assistant OS add-on that connects your Home Assistant instance to the PixelMug cloud through an outbound WebSocket connection.

It allows approved remote clients to access a limited Home Assistant Core API bridge without exposing inbound ports on your local network.

## Features

- Outbound-only WebSocket connection to the PixelMug connector service.
- Uses the Home Assistant Supervisor token inside the add-on.
- Supports controlled Home Assistant REST API calls.
- Supports Home Assistant WebSocket sessions for state reads, subscriptions, and service calls.
- Includes reconnects, heartbeat checks, request timeouts, payload limits, and concurrency limits.

## Configuration

Set `server_url` to the connector server origin only, for example:

```yaml
server_url: "wss://example.com:5080"
```

The add-on appends `/ha/connect` automatically.

Use `access_mode` to control what remote clients can do:

- `read_only`: read and subscribe only.
- `control`: read, subscribe, and call Home Assistant services.
- `admin`: allow all supported bridge commands.

Use `wss://` in production. Enable `allow_insecure_ws` only for development.

## More Information

See [DOCS.md](DOCS.md) for installation notes and protocol details.
