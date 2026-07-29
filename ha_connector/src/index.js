import { readFile } from "node:fs/promises";
import process from "node:process";
import WebSocket, { WebSocket as WebSocketType } from "ws";

const OPTIONS_PATH = "/data/options.json";
const HA_REST_ORIGIN = "http://supervisor";
const HA_WS_URL = "ws://supervisor/core/websocket";
const CONNECTOR_WS_PATH = "/ha/connect";
const CONNECTOR_VERSION = "0.1.1";
const HA_WS_MESSAGE_CHUNK_BYTES = 48 * 1024;

const READ_ONLY_WS_TYPES = new Set([
  "get_config",
  "get_panels",
  "get_services",
  "get_states",
  "ping",
  "subscribe_events",
  "unsubscribe_events",
]);

const CONTROL_WS_TYPES = new Set([
  ...READ_ONLY_WS_TYPES,
  "call_service",
]);

class ConnectorError extends Error {
  constructor(code, message, status = undefined) {
    super(message);
    this.name = "ConnectorError";
    this.code = code;
    this.status = status;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clampNumber(value, fallback, min, max) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(parsed)));
}

function safeJsonParse(text, description = "JSON") {
  try {
    return JSON.parse(text);
  } catch {
    throw new ConnectorError("invalid_json", `${description} is not valid JSON`);
  }
}

function normalizeServerUrl(value) {
  const text = String(value ?? "").trim();
  if (!text || text === "ws://your-server-host:port") {
    throw new ConnectorError("config_error", "server_url is required");
  }

  let url;
  try {
    url = new URL(text);
  } catch {
    throw new ConnectorError("config_error", "server_url is not a valid URL");
  }

  if (!["ws:", "wss:"].includes(url.protocol)) {
    throw new ConnectorError("config_error", "server_url must use ws:// or wss://");
  }

  url.pathname = CONNECTOR_WS_PATH;
  url.search = "";
  url.hash = "";
  return url.toString();
}

function normalizeOptions(raw) {
  const options = {
    serverUrl: normalizeServerUrl(raw.server_url),
    token: String(raw.token ?? ""),
    allowInsecureWs: Boolean(raw.allow_insecure_ws),
    accessMode: ["read_only", "control", "admin"].includes(raw.access_mode)
      ? raw.access_mode
      : "read_only",
    enableHaWebSocket: Boolean(raw.enable_ha_websocket),
    logLevel: ["debug", "info", "warn", "error"].includes(raw.log_level)
      ? raw.log_level
      : "info",
    requestTimeoutMs: clampNumber(raw.request_timeout_ms, 15000, 1000, 120000),
    maxPayloadBytes: clampNumber(raw.max_payload_bytes, 4 * 1024 * 1024, 65536, 16 * 1024 * 1024),
    maxConcurrentRequests: clampNumber(raw.max_concurrent_requests, 16, 1, 64),
    maxWebSocketSessions: clampNumber(raw.max_websocket_sessions, 4, 0, 16),
    reconnectInitialMs: clampNumber(raw.reconnect_initial_ms, 1000, 250, 30000),
    reconnectMaxMs: clampNumber(raw.reconnect_max_ms, 30000, 1000, 300000),
    heartbeatIntervalMs: clampNumber(raw.heartbeat_interval_ms, 30000, 5000, 120000),
    statusReportIntervalMs: clampNumber(raw.status_report_interval_ms, 60000, 0, 3600000),
  };

  if (!options.token) {
    throw new ConnectorError("config_error", "token is required");
  }

  const serverUrl = new URL(options.serverUrl);
  if (serverUrl.protocol === "ws:" && !options.allowInsecureWs) {
    throw new ConnectorError(
      "config_error",
      "server_url uses ws:// while allow_insecure_ws=false; use wss:// in production",
    );
  }

  return options;
}

function createLogger(level) {
  const priorities = { debug: 10, info: 20, warn: 30, error: 40 };
  const threshold = priorities[level] ?? priorities.info;

  function emit(name, args) {
    if (priorities[name] < threshold) return;
    const timestamp = new Date().toISOString();
    const output = [`[${timestamp}]`, `[${name.toUpperCase()}]`, ...args];
    const target = name === "error" ? console.error : name === "warn" ? console.warn : console.log;
    target(...output);
  }

  return {
    debug: (...args) => emit("debug", args),
    info: (...args) => emit("info", args),
    warn: (...args) => emit("warn", args),
    error: (...args) => emit("error", args),
  };
}

function validateEventId(value) {
  if ((typeof value !== "string" && typeof value !== "number") || String(value).length > 128) {
    throw new ConnectorError("invalid_event_id", "eventId must be a string or number no longer than 128 characters");
  }
  return value;
}

function validateEntityId(value) {
  const entityId = String(value ?? "");
  if (!/^[a-z0-9_]+\.[a-z0-9_]+$/i.test(entityId)) {
    throw new ConnectorError("invalid_entity_id", "entityId has an invalid format");
  }
  return entityId;
}

function validateSessionId(value) {
  const sessionId = String(value ?? "");
  if (!/^[A-Za-z0-9._:-]{1,128}$/.test(sessionId)) {
    throw new ConnectorError("invalid_session_id", "sessionId has an invalid format");
  }
  return sessionId;
}

function byteLengthOfJson(value) {
  return Buffer.byteLength(JSON.stringify(value), "utf8");
}

async function readLimitedBody(response, maxBytes) {
  if (!response.body) return null;

  const reader = response.body.getReader();
  const chunks = [];
  let total = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel();
        throw new ConnectorError("response_too_large", `HA response exceeds ${maxBytes} bytes`);
      }
      chunks.push(Buffer.from(value));
    }
  } finally {
    reader.releaseLock();
  }

  if (total === 0) return null;
  const buffer = Buffer.concat(chunks, total);
  const contentType = response.headers.get("content-type") ?? "";
  const text = buffer.toString("utf8");

  if (contentType.includes("application/json")) {
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  }
  return text;
}

class HaWebSocketSession {
  constructor({ sessionId, connector, logger, supervisorToken, options }) {
    this.sessionId = sessionId;
    this.connector = connector;
    this.logger = logger;
    this.supervisorToken = supervisorToken;
    this.options = options;
    this.socket = null;
    this.ready = false;
    this.closed = false;
    this.openPromise = null;
  }

  async open() {
    if (this.openPromise) return this.openPromise;

    this.openPromise = new Promise((resolve, reject) => {
      const socket = new WebSocket(HA_WS_URL, {
        handshakeTimeout: this.options.requestTimeoutMs,
        perMessageDeflate: false,
        maxPayload: this.options.maxPayloadBytes,
      });
      this.socket = socket;

      const timeout = setTimeout(() => {
        reject(new ConnectorError("ha_ws_timeout", "Timed out connecting to Home Assistant WebSocket"));
        socket.terminate();
      }, this.options.requestTimeoutMs);

      const failOpen = (error) => {
        clearTimeout(timeout);
        if (!this.ready) reject(error);
      };

      socket.on("error", (error) => {
        this.logger.warn(`HA WebSocket session ${this.sessionId} error: ${error.message}`);
        failOpen(new ConnectorError("ha_ws_error", "Home Assistant WebSocket connection failed"));
      });

      socket.on("message", (raw, isBinary) => {
        try {
          if (isBinary) {
            throw new ConnectorError("ha_ws_binary_unsupported", "HA WebSocket binary messages are not supported yet");
          }
          const message = safeJsonParse(raw.toString("utf8"), "HA WebSocket message");

          if (!this.ready) {
            if (message.type === "auth_required") {
              socket.send(JSON.stringify({
                type: "auth",
                access_token: this.supervisorToken,
              }));
              return;
            }
            if (message.type === "auth_ok") {
              clearTimeout(timeout);
              this.ready = true;
              this.logger.info(`HA WebSocket session established: ${this.sessionId}`);
              resolve();
              return;
            }
            if (message.type === "auth_invalid") {
              failOpen(new ConnectorError("ha_ws_auth_invalid", "Home Assistant WebSocket authentication failed"));
              socket.close(1008, "Authentication failed");
              return;
            }
            return;
          }

          this.connector.sendHaWsMessage(this.sessionId, message);
        } catch (error) {
          this.logger.warn(`Failed to handle HA WebSocket message: ${error.message}`);
          this.close(1003, "Invalid HA message");
        }
      });

      socket.on("close", (code, reasonBuffer) => {
        clearTimeout(timeout);
        const reason = reasonBuffer.toString("utf8");
        const wasReady = this.ready;
        this.ready = false;
        this.closed = true;
        this.connector.removeHaSession(this.sessionId, this);

        if (!wasReady) {
          failOpen(new ConnectorError("ha_ws_closed", `Home Assistant WebSocket closed before authentication, code=${code}`));
        }

        this.connector.sendDeviceEvent("ha_ws_closed", {
          sessionId: this.sessionId,
          code,
          reason,
        });
      });
    });

    return this.openPromise;
  }

  send(message) {
    if (!this.ready || !this.socket || this.socket.readyState !== WebSocketType.OPEN) {
      throw new ConnectorError("ha_ws_not_ready", "Home Assistant WebSocket session is not ready");
    }
    if (!message || typeof message !== "object" || Array.isArray(message)) {
      throw new ConnectorError("invalid_ha_ws_message", "message must be a JSON object");
    }
    if (byteLengthOfJson(message) > this.options.maxPayloadBytes) {
      throw new ConnectorError("payload_too_large", "HA WebSocket message exceeds the size limit");
    }

    this.enforceAccessMode(message);
    this.socket.send(JSON.stringify(message));
  }

  enforceAccessMode(message) {
    const type = String(message.type ?? "");
    if (!type) {
      throw new ConnectorError("invalid_ha_ws_message", "HA WebSocket command is missing type");
    }

    if (this.options.accessMode === "admin") return;
    if (this.options.accessMode === "control" && CONTROL_WS_TYPES.has(type)) return;
    if (this.options.accessMode === "read_only" && READ_ONLY_WS_TYPES.has(type)) return;

    throw new ConnectorError(
      "ha_ws_command_forbidden",
      `access_mode=${this.options.accessMode} does not allow HA WebSocket command: ${type}`,
    );
  }

  close(code = 1000, reason = "Closed") {
    if (this.closed) return;
    this.closed = true;
    if (this.socket?.readyState === WebSocketType.CONNECTING) {
      this.socket.terminate();
    } else if (this.socket?.readyState === WebSocketType.OPEN) {
      this.socket.close(code, reason.slice(0, 123));
    }
  }
}

class Connector {
  constructor(options, supervisorToken, logger) {
    this.options = options;
    this.supervisorToken = supervisorToken;
    this.logger = logger;
    this.socket = null;
    this.registered = false;
    this.stopping = false;
    this.reconnectDelayMs = options.reconnectInitialMs;
    this.heartbeatTimer = null;
    this.statusTimer = null;
    this.registrationTimer = null;
    this.awaitingPong = false;
    this.activeRequests = 0;
    this.haSessions = new Map();
  }

  async run() {
    while (!this.stopping) {
      try {
        await this.connectOnce();
      } catch (error) {
        if (!this.stopping) {
          this.logger.warn(`Connector connection ended: ${error.message}`);
        }
      }

      if (this.stopping) break;

      const jitter = Math.floor(Math.random() * Math.max(250, this.reconnectDelayMs * 0.2));
      const delay = Math.min(this.options.reconnectMaxMs, this.reconnectDelayMs + jitter);
      this.logger.info(`Reconnecting in ${delay}ms`);
      await sleep(delay);
      this.reconnectDelayMs = Math.min(this.options.reconnectMaxMs, this.reconnectDelayMs * 2);
    }
  }

  connectOnce() {
    return new Promise((resolve, reject) => {
      this.registered = false;
      this.awaitingPong = false;
      this.closeAllHaSessions(1012, "Connector reconnecting");

      const socket = new WebSocket(this.options.serverUrl, {
        handshakeTimeout: this.options.requestTimeoutMs,
        perMessageDeflate: false,
        maxPayload: this.options.maxPayloadBytes,
      });
      this.socket = socket;

      let settled = false;
      const finish = (error) => {
        if (settled) return;
        settled = true;
        this.clearConnectionTimers();
        this.socket = null;
        this.registered = false;
        if (error) reject(error);
        else resolve();
      };

      socket.on("open", () => {
        this.logger.info(`Connected to Connector service: ${this.redactUrl(this.options.serverUrl)}`);
        socket.send(JSON.stringify({ type: "register", token: this.options.token }));
        this.registrationTimer = setTimeout(() => {
          socket.close(1008, "Registration timeout");
        }, this.options.requestTimeoutMs);
      });

      socket.on("message", (raw, isBinary) => {
        this.handleRemoteMessage(raw, isBinary).catch((error) => {
          this.logger.warn(`Failed to handle remote message: ${error.message}`);
          if (!this.registered && socket.readyState === WebSocketType.OPEN) {
            socket.close(1008, "Registration failed");
          }
        });
      });

      socket.on("pong", () => {
        this.awaitingPong = false;
      });

      socket.on("error", (error) => {
        this.logger.warn(`Connector WebSocket error: ${error.message}`);
      });

      socket.on("close", (code, reasonBuffer) => {
        const reason = reasonBuffer.toString("utf8");
        this.logger.warn(`Connector WebSocket closed: code=${code}, reason=${reason || "none"}`);
        finish(new ConnectorError("remote_closed", `Remote connection closed, code=${code}`));
      });

      this.heartbeatTimer = setInterval(() => {
        if (socket.readyState !== WebSocketType.OPEN) return;
        if (this.awaitingPong) {
          this.logger.warn("Connector heartbeat timed out; disconnecting");
          socket.terminate();
          return;
        }
        this.awaitingPong = true;
        socket.ping();
      }, this.options.heartbeatIntervalMs);
    });
  }

  async handleRemoteMessage(raw, isBinary) {
    if (isBinary) {
      throw new ConnectorError("binary_unsupported", "Remote binary messages are not supported");
    }
    if (raw.length > this.options.maxPayloadBytes) {
      throw new ConnectorError("payload_too_large", "Remote message exceeds the size limit");
    }

    const message = safeJsonParse(raw.toString("utf8"), "remote message");

    if (!this.registered) {
      if (message.type !== "registered") {
        throw new ConnectorError("register_failed", `Registration failed, response type=${String(message.type)}`);
      }
      clearTimeout(this.registrationTimer);
      this.registrationTimer = null;
      this.registered = true;
      this.reconnectDelayMs = this.options.reconnectInitialMs;
      this.logger.info("Connector registered successfully");
      this.startStatusReports();
      return;
    }

    if (message.type !== "event") {
      this.logger.debug(`Ignoring non-event message: ${String(message.type)}`);
      return;
    }

    const eventId = validateEventId(message.eventId);
    if (this.activeRequests >= this.options.maxConcurrentRequests) {
      this.sendResponse(eventId, false, null, {
        code: "too_many_requests",
        message: "Connector concurrent request limit reached",
      });
      return;
    }

    this.activeRequests += 1;
    try {
      const data = await this.dispatchEvent(String(message.event ?? ""), message.data ?? {});
      this.sendResponse(eventId, true, data);
    } catch (error) {
      const normalized = this.normalizeError(error);
      this.logger.warn(`Event ${String(message.event)} failed: ${normalized.message}`);
      this.sendResponse(eventId, false, null, normalized);
    } finally {
      this.activeRequests -= 1;
    }
  }

  async dispatchEvent(event, data) {
    switch (event) {
      case "ping":
        return { pong: true, timestamp: new Date().toISOString() };
      case "ha_rest":
        return this.handleHaRest(data);
      case "ha_call_service":
        return this.handleHaCallService(data);
      case "set_light":
        return this.handleSetLight(data);
      case "ha_ws_open":
        return this.handleHaWsOpen(data);
      case "ha_ws_send":
        return this.handleHaWsSend(data);
      case "ha_ws_close":
        return this.handleHaWsClose(data);
      default:
        throw new ConnectorError("unsupported_event", `Unsupported event: ${event}`);
    }
  }

  async handleHaRest(data) {
    const method = String(data.method ?? "GET").toUpperCase();
    const path = String(data.path ?? "");

    this.enforceRestAccess(method, path);

    const url = new URL(`${HA_REST_ORIGIN}/core${path}`);
    if (data.query !== undefined) {
      if (!data.query || typeof data.query !== "object" || Array.isArray(data.query)) {
        throw new ConnectorError("invalid_query", "query must be a JSON object");
      }
      for (const [key, value] of Object.entries(data.query)) {
        if (Array.isArray(value)) {
          for (const item of value) url.searchParams.append(key, String(item));
        } else if (value !== null && value !== undefined) {
          url.searchParams.set(key, String(value));
        }
      }
    }

    let body;
    if (!["GET", "HEAD"].includes(method) && data.body !== undefined) {
      body = JSON.stringify(data.body);
      if (Buffer.byteLength(body, "utf8") > this.options.maxPayloadBytes) {
        throw new ConnectorError("payload_too_large", "Request body exceeds the size limit");
      }
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.options.requestTimeoutMs);

    let response;
    try {
      response = await fetch(url, {
        method,
        headers: {
          Authorization: `Bearer ${this.supervisorToken}`,
          Accept: "application/json",
          ...(body === undefined ? {} : { "Content-Type": "application/json" }),
        },
        body,
        signal: controller.signal,
      });
    } catch (error) {
      if (error?.name === "AbortError") {
        throw new ConnectorError("ha_timeout", "Home Assistant REST API request timed out");
      }
      throw new ConnectorError("ha_unreachable", `Unable to access Home Assistant REST API: ${error.message}`);
    } finally {
      clearTimeout(timer);
    }

    const responseBody = await readLimitedBody(response, this.options.maxPayloadBytes);
    const result = {
      status: response.status,
      ok: response.ok,
      headers: {
        "content-type": response.headers.get("content-type"),
      },
      body: responseBody,
    };

    if (!response.ok) {
      throw new ConnectorError("ha_http_error", `Home Assistant returned HTTP ${response.status}`, response.status);
    }

    return result;
  }

  enforceRestAccess(method, path) {
    let decodedPath;
    try {
      decodedPath = decodeURIComponent(path);
    } catch {
      throw new ConnectorError("path_forbidden", "API path contains invalid URL encoding");
    }

    if (
      !path.startsWith("/api/") ||
      path.includes("?") ||
      path.includes("#") ||
      decodedPath.includes("..") ||
      decodedPath.includes("\\")
    ) {
      throw new ConnectorError("path_forbidden", "Only Home Assistant Core API paths under /api/ are allowed");
    }

    const allowedMethods = this.options.accessMode === "read_only"
      ? new Set(["GET", "HEAD"])
      : this.options.accessMode === "control"
        ? new Set(["GET", "HEAD", "POST"])
        : new Set(["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"]);

    if (!allowedMethods.has(method)) {
      throw new ConnectorError(
        "method_forbidden",
        `access_mode=${this.options.accessMode} does not allow ${method} requests`,
      );
    }
  }

  async handleHaCallService(data) {
    if (this.options.accessMode === "read_only") {
      throw new ConnectorError("access_forbidden", "read_only mode does not allow service calls");
    }

    const domain = String(data.domain ?? "");
    const service = String(data.service ?? "");
    if (!/^[a-z0-9_]+$/.test(domain) || !/^[a-z0-9_]+$/.test(service)) {
      throw new ConnectorError("invalid_service", "domain or service has an invalid format");
    }

    return this.handleHaRest({
      method: "POST",
      path: `/api/services/${domain}/${service}`,
      body: data.serviceData ?? {},
    });
  }

  async handleSetLight(data) {
    const entityId = validateEntityId(data.entityId);
    if (!entityId.startsWith("light.")) {
      throw new ConnectorError("invalid_light_entity", "set_light only allows light.* entities");
    }

    const state = String(data.state ?? "").toLowerCase();
    if (!["on", "off"].includes(state)) {
      throw new ConnectorError("invalid_light_state", "state must be on or off");
    }

    const result = await this.handleHaCallService({
      domain: "light",
      service: state === "on" ? "turn_on" : "turn_off",
      serviceData: { entity_id: entityId },
    });

    return { entityId, state, ha: result };
  }

  async handleHaWsOpen(data) {
    if (!this.options.enableHaWebSocket) {
      throw new ConnectorError("ha_ws_disabled", "HA WebSocket bridge is disabled");
    }
    if (this.options.maxWebSocketSessions === 0) {
      throw new ConnectorError("ha_ws_disabled", "max_websocket_sessions=0");
    }

    const sessionId = validateSessionId(data.sessionId);
    if (this.haSessions.has(sessionId)) {
      throw new ConnectorError("session_exists", `sessionId already exists: ${sessionId}`);
    }
    if (this.haSessions.size >= this.options.maxWebSocketSessions) {
      throw new ConnectorError("too_many_sessions", "HA WebSocket session limit reached");
    }

    const session = new HaWebSocketSession({
      sessionId,
      connector: this,
      logger: this.logger,
      supervisorToken: this.supervisorToken,
      options: this.options,
    });
    this.haSessions.set(sessionId, session);

    try {
      await session.open();
      return { sessionId, opened: true };
    } catch (error) {
      this.haSessions.delete(sessionId);
      session.close(1011, "Open failed");
      throw error;
    }
  }

  handleHaWsSend(data) {
    const sessionId = validateSessionId(data.sessionId);
    const session = this.haSessions.get(sessionId);
    if (!session) {
      throw new ConnectorError("session_not_found", `HA WebSocket session not found: ${sessionId}`);
    }
    session.send(data.message);
    return { sessionId, accepted: true };
  }

  handleHaWsClose(data) {
    const sessionId = validateSessionId(data.sessionId);
    const session = this.haSessions.get(sessionId);
    if (!session) return { sessionId, closed: false, reason: "not_found" };

    session.close(1000, "Remote requested close");
    this.haSessions.delete(sessionId);
    return { sessionId, closed: true };
  }

  sendResponse(eventId, success, data = null, error = undefined) {
    this.sendJson({
      type: "response",
      eventId,
      success,
      ...(success ? { data } : { error }),
    });
  }

  sendHaWsMessage(sessionId, message) {
    const json = JSON.stringify(message);
    const bytes = Buffer.byteLength(json, "utf8");
    if (bytes <= HA_WS_MESSAGE_CHUNK_BYTES) {
      return this.sendDeviceEvent("ha_ws_message", { sessionId, message });
    }

    const transferId = `${sessionId}:${Date.now().toString(36)}:${Math.random()
      .toString(36)
      .slice(2, 10)}`;
    const encoded = Buffer.from(json, "utf8").toString("base64");
    const chunkChars = Math.floor((HA_WS_MESSAGE_CHUNK_BYTES * 3) / 4);
    const total = Math.ceil(encoded.length / chunkChars);

    for (let index = 0; index < total; index += 1) {
      const chunk = encoded.slice(index * chunkChars, (index + 1) * chunkChars);
      const ok = this.sendDeviceEvent("ha_ws_message_chunk", {
        sessionId,
        transferId,
        index,
        total,
        encoding: "base64json",
        chunk,
      });
      if (!ok) return false;
    }
    return true;
  }
  sendDeviceEvent(event, data) {
    if (!this.registered) return false;
    return this.sendJson({ type: "event", event, data });
  }

  sendJson(message) {
    const socket = this.socket;
    if (!socket || socket.readyState !== WebSocketType.OPEN) return false;

    const encoded = JSON.stringify(message);
    if (Buffer.byteLength(encoded, "utf8") > this.options.maxPayloadBytes) {
      this.logger.warn(`Dropping oversized outbound message: type=${message.type}`);
      return false;
    }
    socket.send(encoded);
    return true;
  }

  startStatusReports() {
    if (this.statusTimer) clearInterval(this.statusTimer);
    if (this.options.statusReportIntervalMs === 0) return;

    const report = async () => {
      let coreReachable = false;
      try {
        const result = await this.handleHaRest({ method: "GET", path: "/api/" });
        coreReachable = result.ok;
      } catch {
        coreReachable = false;
      }

      this.sendDeviceEvent("device_status", {
        online: true,
        coreReachable,
        connectorVersion: CONNECTOR_VERSION,
        accessMode: this.options.accessMode,
        activeRequests: this.activeRequests,
        haWebSocketSessions: this.haSessions.size,
        timestamp: new Date().toISOString(),
      });
    };

    void report();
    this.statusTimer = setInterval(() => void report(), this.options.statusReportIntervalMs);
  }

  removeHaSession(sessionId, expectedSession) {
    if (this.haSessions.get(sessionId) === expectedSession) {
      this.haSessions.delete(sessionId);
    }
  }

  closeAllHaSessions(code, reason) {
    for (const session of this.haSessions.values()) {
      session.close(code, reason);
    }
    this.haSessions.clear();
  }

  normalizeError(error) {
    if (error instanceof ConnectorError) {
      return {
        code: error.code,
        message: error.message,
        ...(error.status === undefined ? {} : { status: error.status }),
      };
    }
    return {
      code: "internal_error",
      message: "Connector internal error",
    };
  }

  redactUrl(value) {
    try {
      const url = new URL(value);
      url.username = "";
      url.password = "";
      return url.toString();
    } catch {
      return "<invalid-url>";
    }
  }

  clearConnectionTimers() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.statusTimer) clearInterval(this.statusTimer);
    if (this.registrationTimer) clearTimeout(this.registrationTimer);
    this.heartbeatTimer = null;
    this.statusTimer = null;
    this.registrationTimer = null;
  }

  stop() {
    if (this.stopping) return;
    this.stopping = true;
    this.clearConnectionTimers();
    this.closeAllHaSessions(1001, "Connector stopping");

    if (this.socket?.readyState === WebSocketType.CONNECTING) {
      this.socket.terminate();
    } else if (this.socket?.readyState === WebSocketType.OPEN) {
      this.socket.close(1000, "Connector stopping");
      setTimeout(() => this.socket?.terminate(), 2000).unref();
    }
  }
}

async function main() {
  const rawOptions = safeJsonParse(await readFile(OPTIONS_PATH, "utf8"), "options.json");
  const options = normalizeOptions(rawOptions);
  const logger = createLogger(options.logLevel);
  const supervisorToken = process.env.SUPERVISOR_TOKEN;

  if (!supervisorToken) {
    throw new ConnectorError(
      "missing_supervisor_token",
      "SUPERVISOR_TOKEN is missing; ensure config.yaml has homeassistant_api: true",
    );
  }

  const connector = new Connector(options, supervisorToken, logger);
  const shutdown = (signal) => {
    logger.info(`Received ${signal}; stopping Connector`);
    connector.stop();
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("uncaughtException", (error) => {
    logger.error("Uncaught exception:", error);
    connector.stop();
    process.exitCode = 1;
  });
  process.on("unhandledRejection", (error) => {
    logger.error("Unhandled Promise rejection:", error);
    connector.stop();
    process.exitCode = 1;
  });

  logger.info(`PixelMug HA Connector ${CONNECTOR_VERSION}`);
  logger.info(`Access mode: ${options.accessMode}`);
  logger.warn(
    options.serverUrl.startsWith("ws://")
      ? "Using insecure ws:// transport; tokens and data may be exposed. Use wss:// in production."
      : "Connector is using encrypted wss:// transport",
  );

  await connector.run();
}

main().catch((error) => {
  console.error(`[FATAL] ${error.message}`);
  process.exit(1);
});
