import { WebSocket, getGlobalDispatcher } from 'undici';

/** Adapt one Responses WebSocket to the SDK's existing event parser.
 * The Response is an in-process parser interface, NOT an HTTP response from
 * the server. Receipts record the actual GET upgrade and response.create frame.
 * No reconnect, continuation, SSE fallback, or independent idle deadline.
 */
export function websocketRequest(wire) {
  const url = new URL(wire.url);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  const { stream, background, ...body } = wire.body;
  const headers = { ...wire.headers };
  for (const name of ['content-type', 'content-encoding', 'content-length', 'accept', 'openai-beta']) delete headers[name];
  return { url: url.href, method: 'GET', headers, body: { type: 'response.create', ...body } };
}

export async function websocketResponse(wire, init, metadata, signal, redact) {
  const headers = new Headers(init.headers);
  for (const name of ['content-type', 'content-encoding', 'content-length', 'accept', 'openai-beta']) headers.delete(name);
  // Real private credentials stay inside the connection. `wire` has redacted headers.
  // Undici's WebSocket error event erases rejected upgrade statuses. Observe
  // this socket's real handshake through its dispatcher, without probing,
  // reconnecting, changing the request or mixing concurrent sockets' receipts.
  const observeHeaders = (status, raw) => {
    metadata.http_status = status;
    const responseHeaders = new Headers(Array.isArray(raw)
      ? Array.from({ length: raw.length / 2 }, (_, i) => [String(raw[i * 2]), String(raw[i * 2 + 1])])
      : raw);
    for (const [name, key] of [['retry-after', 'retry_after'], ['retry-after-ms', 'retry_after_ms'],
      ['x-agent-request-id', 'proxy_request_id'], ['x-request-id', 'request_id']]) {
      const value = responseHeaders.get(name);
      if (value) metadata[key] = redact(value).slice(0, 1024);
    }
    const retry = responseHeaders.get('x-should-retry');
    if (retry === 'true' || retry === 'false') metadata.should_retry = retry === 'true';
  };
  const dispatcher = getGlobalDispatcher().compose(dispatch => (options, handler) =>
    dispatch(options, new Proxy(handler, { get(target, name) {
      const value = Reflect.get(target, name, target);
      if (name === 'onResponseStart') return (controller, status, raw, ...rest) => {
        observeHeaders(status, raw);
        return value.call(target, controller, status, raw, ...rest);
      };
      // Undici wraps callbacks in a class with private state. Preserve their
      // receiver and all lifecycle methods rather than copying enumerable keys.
      return typeof value === 'function' ? value.bind(target) : value;
    } })));
  const socket = new WebSocket(wire.url, { headers, dispatcher });
  metadata.websocket_frames = 0;
  metadata.websocket_bytes = 0;
  let controller;
  let terminal = false;
  let opened = false;
  let ended = false;
  let resolveUpgrade;
  let rejectUpgrade;
  const upgrade = new Promise((resolve, reject) => { resolveUpgrade = resolve; rejectUpgrade = reject; });
  const close = () => { try { socket.close(); } catch { /* Already closed. */ } };
  const cleanup = () => signal.removeEventListener('abort', onAbort);
  const fail = error => {
    if (ended) return;
    ended = true;
    cleanup();
    if (opened) controller.error(error);
    else rejectUpgrade(error);
    close();
  };
  const onAbort = () => fail(signal.reason);
  const body = new ReadableStream({ start(value) { controller = value; },
    cancel() { ended = true; cleanup(); close(); } });
  socket.addEventListener('open', () => {
    if (ended) { close(); return; }
    opened = true;
    metadata.http_status = 101;
    try {
      socket.send(JSON.stringify(wire.body));
      resolveUpgrade(new Response(body, { headers: { 'content-type': 'text/event-stream' } }));
    } catch (error) { fail(error); }
  });
  socket.addEventListener('message', event => {
    if (ended) return;
    try {
      if (typeof event.data !== 'string') throw new Error('Expected a text Responses WebSocket frame');
      const value = JSON.parse(event.data);
      metadata.websocket_frames++;
      metadata.websocket_bytes += Buffer.byteLength(event.data);
      controller.enqueue(new TextEncoder().encode(`data: ${event.data}\n\n`));
      if (['response.completed', 'response.done', 'response.failed', 'response.incomplete', 'error'].includes(value.type)) {
        terminal = true;
        metadata.terminal_event = value.type;
        ended = true;
        cleanup();
        controller.close();
        close();
      }
    } catch (error) { fail(error); }
  });
  socket.addEventListener('error', event => fail(event.error ?? new Error(event.message || 'WebSocket connection failed')));
  socket.addEventListener('close', event => {
    metadata.websocket_close = { code: event.code, reason: event.reason, was_clean: event.wasClean };
    if (!terminal) fail(new Error(`WebSocket closed before terminal response (${event.code}): ${event.reason}`));
  });
  signal.addEventListener('abort', onAbort, { once: true });
  if (signal.aborted) onAbort();
  return upgrade;
}
