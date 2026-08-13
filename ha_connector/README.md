# PixelMug HA Connector

PixelMug HA Connector is a Home Assistant OS add-on that connects your Home Assistant instance to the PixelMug cloud through an outbound WebSocket connection.

It allows approved remote clients to access a limited Home Assistant Core API bridge without exposing inbound ports on your local network.

## Features

- Outbound-only WebSocket connection to the PixelMug connector service.
- Uses the Home Assistant Supervisor token inside the add-on.
- Supports optionally read-only or read/write Home Assistant REST tunneling.
- Supports Home Assistant WebSocket sessions for state reads, subscriptions, and service calls.
- Includes reconnects, heartbeat checks, request timeouts, payload limits, and concurrency limits.

## Configuration

Set `server_url` to the connector server origin only, for example:

```yaml
server_url: "wss://example.com:5080"
```

The add-on appends `/ha/connect` automatically.

Channel permissions are deliberately coarse-grained and business-agnostic:

- `enable_ha_websocket` enables or disables the opaque HA WebSocket tunnel.
- `enable_ha_rest` enables or disables the HA REST tunnel.
- `allow_ha_rest_write` controls whether the REST tunnel accepts methods other than GET and HEAD.

The Connector does not inspect HA WebSocket command types or implement entity/service shortcuts. Authorize users and homes before the server routes requests to a Connector.

Use `wss://` in production. Enable `allow_insecure_ws` only for development.

## More Information

See [DOCS.md](DOCS.md) for installation notes and protocol details.
