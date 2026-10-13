import { test } from 'node:test';
import assert from 'node:assert/strict';
import { WebSocketServer } from 'ws';
import { createServer } from 'node:http';
import { prepare, execute, preview } from './model.mjs';

const config = { provider: 'openai-codex', model: 'gpt-6.1-sol', reasoning: 'xhigh',
  transport: 'websocket', prompt: 'Exact prompt. ☕', timeout_seconds: 2,
  schema: { type: 'object', properties: { ok: { type: 'boolean' } }, required: ['ok'], additionalProperties: false } };
const token = 'test.' + Buffer.from(JSON.stringify({
  'https://api.openai.com/auth': { chatgpt_account_id: 'test-account' },
})).toString('base64') + '.signature';

async function localRun(onMessage, extra = {}) {
  const server = new WebSocketServer({ port: 0, host: '127.0.0.1' });
  await new Promise(resolve => server.once('listening', resolve));
  let connections = 0;
  let messages = 0;
  server.on('connection', socket => {
    connections++;
    socket.on('message', data => { messages++; onMessage(socket, JSON.parse(data)); });
  });
  try {
    const request = await prepare({ ...config, ...extra });
    request.model = { ...request.model, baseUrl: `http://127.0.0.1:${server.address().port}/test/pi` };
    const receipt = await execute(request, { apiKey: token,
      fetch: () => { throw new Error('SSE fallback is forbidden'); } });
    return { receipt, connections, messages };
  } finally {
    for (const client of server.clients) client.terminate();
    await new Promise(resolve => server.close(resolve));
  }
}

test('WebSocket: one real connection, exact request, SDK parsing and terminal usage', async () => {
  let sent;
  const { receipt, connections, messages } = await localRun((socket, body) => {
    sent = body;
    const item = { id: 'msg-ws', type: 'message', role: 'assistant', status: 'completed',
      content: [{ type: 'output_text', text: '{"ok":true}', annotations: [] }] };
    for (const event of [
      { type: 'response.created', response: { id: 'resp-ws', status: 'in_progress' } },
      { type: 'response.output_item.added', output_index: 0, item: { ...item, content: [] } },
      { type: 'response.output_text.delta', output_index: 0, content_index: 0, delta: '{"ok":true}' },
      { type: 'response.output_item.done', output_index: 0, item },
      { type: 'response.completed', response: { id: 'resp-ws', model: config.model, status: 'completed',
        output: [item], usage: { input_tokens: 11, output_tokens: 7,
          input_tokens_details: { cached_tokens: 3 }, output_tokens_details: { reasoning_tokens: 2 } } } },
    ]) socket.send(JSON.stringify(event));
  });
  assert.equal(connections, 1);
  assert.equal(messages, 1);
  assert.equal(sent.type, 'response.create');
  assert.equal(sent.stream, undefined);
  assert.equal(sent.background, undefined);
  assert.equal(sent.store, false);
  assert.equal(sent.model, config.model);
  assert.deepEqual(sent.reasoning, { effort: 'xhigh' });
  assert.equal(sent.input[0].content[0].text, config.prompt);
  assert.deepEqual(sent.text.format.schema, config.schema);
  assert.equal(receipt.valid, true, receipt.error);
  assert.deepEqual(receipt.output, { ok: true });
  assert.equal(receipt.transport, 'websocket');
  assert.equal(receipt.response_metadata.http_status, 101);
  assert.equal(receipt.response_metadata.terminal_event, 'response.completed');
  assert.equal(receipt.response_id, 'resp-ws');
  assert.equal(receipt.usage.output, 7);
  assert.equal(receipt.usage.cacheRead, 3);
  assert.equal(receipt.http_attempts, 1);
  assert.equal(receipt.request.method, 'GET');
  assert.match(receipt.request.url, /^ws:\/\//);
  assert.deepEqual(receipt.request.body, sent);
  assert.ok(!JSON.stringify(receipt).includes(token));
});

test('WebSocket: offline preview changes only transport envelope', async () => {
  const ws = await preview(await prepare(config));
  const sse = await preview(await prepare({ ...config, transport: 'sse' }));
  const { type, ...body } = ws.body;
  const { stream, background, ...expected } = sse.body;
  assert.equal(type, 'response.create');
  assert.deepEqual(body, expected);
  assert.equal(ws.method, 'GET');
  assert.equal(ws.network_requests, 0);
  assert.equal(ws.credentials_read, false);
});

for (const code of ['websocket_connection_limit_reached', 'previous_response_not_found']) {
  test(`WebSocket: ${code} is not retried or sent through SSE`, async () => {
    const { receipt, connections, messages } = await localRun(socket => socket.send(JSON.stringify({
      type: 'error', error: { code, message: code },
    })));
    assert.equal(receipt.valid, false);
    assert.match(receipt.error, new RegExp(code));
    assert.equal(connections, 1);
    assert.equal(messages, 1);
  });
}

test('WebSocket: close without terminal event is a failure, not empty success', async () => {
  const { receipt, connections } = await localRun(socket => socket.close(1011, 'test failure'));
  assert.equal(receipt.valid, false);
  assert.equal(receipt.response_metadata.websocket_close.code, 1011);
  assert.match(receipt.error, /terminal|closed/i);
  assert.equal(connections, 1);
});

test('WebSocket: caller deadline aborts an idle connection without retry', async () => {
  const { receipt, connections, messages } = await localRun(() => {}, { timeout_seconds: 1 });
  assert.equal(receipt.valid, false);
  assert.ok(receipt.response_metadata.abort_reason);
  assert.equal(connections, 1);
  assert.equal(messages, 1);
});

test('WebSocket: unsupported provider and unknown transport are refused', async () => {
  await assert.rejects(prepare({ ...config, provider: 'openrouter', model: 'xiaomi/mimo-v2.6-flash' }), /WebSocket.*subscription/i);
  await assert.rejects(prepare({ ...config, transport: 'auto' }), /transport/);
});

for (const status of [403, 429, 503]) test(`WebSocket: rejected ${status} upgrade retains status and retry headers without fallback`, async () => {
  let upgrades = 0;
  let posts = 0;
  const server = createServer((_request, response) => { posts++; response.writeHead(500); response.end(); });
  server.on('upgrade', (_request, socket) => { upgrades++; socket.end(
    `HTTP/1.1 ${status} Fixture failure\r\nContent-Length: 0\r\nRetry-After: 120\r\nRetry-After-Ms: 120000\r\n\r\n`); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  try {
    const request = await prepare(config);
    request.model = { ...request.model, baseUrl: `http://127.0.0.1:${server.address().port}/test/pi` };
    const receipt = await execute(request, { apiKey: token });
    assert.equal(receipt.valid, false);
    assert.equal(receipt.response_metadata.http_status, status);
    assert.equal(receipt.response_metadata.retry_after, '120');
    assert.equal(receipt.response_metadata.retry_after_ms, '120000');
    assert.equal(receipt.response_metadata.transport_error.phase, 'headers');
    assert.equal(upgrades, 1);
    assert.equal(posts, 0);
    assert.equal(receipt.http_attempts, 1);
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
});

test('WebSocket: concurrent rejected upgrades keep separate statuses and deadlines', async () => {
  const server = createServer((_request, response) => { response.writeHead(500); response.end(); });
  let upgrades = 0;
  server.on('upgrade', (request, socket) => {
    upgrades++;
    const status = request.url.startsWith('/busy/') ? 503 : 403;
    const delay = status === 503 ? 12 : 34;
    socket.end(`HTTP/1.1 ${status} Fixture failure\r\nContent-Length: 0\r\nRetry-After: ${delay}\r\n\r\n`);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  try {
    const results = await Promise.all(['busy', 'denied'].map(async name => {
      const request = await prepare(config);
      request.model = { ...request.model, baseUrl: `http://127.0.0.1:${server.address().port}/${name}/pi` };
      return execute(request, { apiKey: token });
    }));
    assert.deepEqual(results.map(result => result.response_metadata.http_status), [503, 403]);
    assert.deepEqual(results.map(result => result.response_metadata.retry_after), ['12', '34']);
    assert.equal(upgrades, 2);
    assert.ok(results.every(result => result.http_attempts === 1 && !result.valid));
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
});
