# PixelMug HA Connector Docs

PixelMug HA Connector is a Home Assistant OS add-on. It creates an outbound WebSocket connection to a PixelMug connector service and exposes a limited bridge to Home Assistant Core APIs.

The add-on does not open inbound ports, does not require host networking, and does not mount the Home Assistant configuration directory.

## Configuration

| Option | Description |
|---|---|
| `server_url` | Connector WebSocket server origin, for example `wss://example.com:5880`. The add-on appends `/ha/connect` automatically. |
| `token` | Registration token sent to the connector service. Keep it secret. |
| `allow_insecure_ws` | Allows `ws://` without TLS. Use only for development. |
| `enable_ha_websocket` | Enables Home Assistant WebSocket API bridging. |
| `enable_ha_rest` | Enables Home Assistant REST API bridging. |
| `allow_ha_rest_write` | Allows POST, PUT, PATCH and DELETE through the REST tunnel. When false, only GET and HEAD are allowed. |
| `log_level` | Connector log verbosity: `debug`, `info`, `warn`, or `error`. |
| `request_timeout_ms` | Timeout for connector, REST, and WebSocket operations. |
| `max_payload_bytes` | Maximum message/API response size. Valid range: 65,536 to 16,777,216 bytes. |
| `max_concurrent_requests` | Maximum number of remote requests handled at once. |
| `max_websocket_sessions` | Maximum number of bridged Home Assistant WebSocket sessions. |
| `reconnect_initial_ms` | Initial reconnect delay after the connector service connection closes. |
| `reconnect_max_ms` | Maximum reconnect delay. |
| `heartbeat_interval_ms` | Ping/pong heartbeat interval for the connector service connection. |
| `status_report_interval_ms` | Interval for periodic connector status events. Set to `0` to disable. |

Example:

```yaml
server_url: "wss://example.com:5880"
token: "replace-with-your-token"
allow_insecure_ws: false
enable_ha_websocket: true
enable_ha_rest: true
allow_ha_rest_write: false
```

`server_url` should not include `/ha/connect`. The add-on normalizes the configured origin and appends that path in code.

## Channel Permissions

Permissions operate at the transport level and do not depend on Home Assistant domains, services, registries, entities, or WebSocket command types.

`enable_ha_websocket` exposes an opaque authenticated Home Assistant WebSocket session. When enabled, the Connector forwards any valid JSON object without interpreting its `type`. User, home and operation authorization must happen in the Connector service before routing.

`enable_ha_rest` enables the generic REST tunnel under Home Assistant Core `/api/`. With `allow_ha_rest_write: false`, only GET and HEAD are accepted. Enable REST writes only when the upstream service applies appropriate authorization.

## Local Installation

1. Install and start Samba or SSH on Home Assistant OS.
2. Copy the `ha_connector` directory into `/addons/ha_connector`.
3. Open Home Assistant Settings, then Apps/Add-ons, then App store.
4. Use the menu to check for updates.
5. Open `PixelMug HA Connector` under local apps and install it.
6. Fill in the configuration values, especially `server_url` and `token`.
7. Start the add-on and check the logs for `Connector registered successfully`.

When changing source code, `Dockerfile`, or add-on metadata, bump the `version` in `config.yaml` so Home Assistant can rebuild or reinstall the add-on cleanly. Runtime configuration changes do not require a version bump.

## Git Repository Installation

This repository is structured as a Home Assistant add-on repository:

```text
repository.yaml
ha_connector/
```

After publishing the repository, add its URL to Home Assistant App store/Add-on store repositories. For production releases, consider building and publishing multi-architecture images with the official Home Assistant builder workflow, then set the `image` field in `config.yaml`.

## Runtime Behavior

On startup, the add-on connects to the configured connector service, registers with the configured token, and then waits for authorized remote requests.

The add-on uses the Supervisor-provided `SUPERVISOR_TOKEN` internally to reach Home Assistant Core. Users do not need to create a Home Assistant long-lived access token for the add-on.

Large Home Assistant WebSocket messages are split into smaller connector events so they can pass through services with smaller frame limits.

The connector reports periodic status events when `status_report_interval_ms` is non-zero.

## Security Notes

Use `wss://` in production. `ws://` can expose tokens, entity states, and control commands to the network.

Use high-entropy, revocable connector tokens. Device-side connector credentials and user-side client credentials should be separate or authorized by the server.

The server should bind users, homes, and connector instances explicitly. Do not route access using only a global shared token.

Disable unused channels. Keep `allow_ha_rest_write` false unless REST mutations are required. Enabling the WebSocket tunnel permits both reads and mutations at the transport layer, so the upstream service must authorize access before routing requests.

Do not log `SUPERVISOR_TOKEN` or registration tokens.
