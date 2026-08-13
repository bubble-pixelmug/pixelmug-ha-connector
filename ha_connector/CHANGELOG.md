# Changelog

## 0.2.0

- Replace business-aware `access_mode` command filtering with transport-level `enable_ha_websocket`, `enable_ha_rest`, and `allow_ha_rest_write` permissions.
- Remove the `set_light` and `ha_call_service` shortcut events; clients use the opaque WebSocket or REST tunnels.
- Encode binary REST responses as base64 so remote clients can retrieve camera images safely.

## 0.1.1

- Normalize `server_url` as a server origin and append `/ha/connect` in code.
- Replace runtime messages and add-on UI prompts with English text.
- Add chunked forwarding for large Home Assistant WebSocket messages.
- Refresh README and DOCS to match current configuration behavior.

## 0.1.0

- Initial outbound WebSocket connector.
- Home Assistant REST and WebSocket API bridge.
- Access modes, reconnect, heartbeat, size limits, and concurrency limits.
