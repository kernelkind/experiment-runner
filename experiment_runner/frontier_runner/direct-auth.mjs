// pi-ai owns provider login and refresh. This store only persists its credentials.
import { access, mkdir, open, realpath, rename, rm } from 'node:fs/promises';
import { constants } from 'node:fs';
import { createHash, randomUUID } from 'node:crypto';
import { homedir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { createInterface } from 'node:readline/promises';
import lockfile from 'proper-lockfile';
import { createModels } from '@earendil-works/pi-ai';
import { openaiCodexProvider } from '@earendil-works/pi-ai/providers/openai-codex';
import { openrouterProvider } from '@earendil-works/pi-ai/providers/openrouter';

export function authenticationMode() {
  const mode = process.env.EXPERIMENT_RUNNER_AUTH ?? 'service';
  if (!['service', 'direct'].includes(mode)) {
    throw new Error('EXPERIMENT_RUNNER_AUTH must be service or direct');
  }
  return mode;
}

async function credentialPath() {
  const configured = resolve(process.env.EXPERIMENT_RUNNER_AUTH_FILE
    || join(process.env.XDG_CONFIG_HOME || join(homedir(), '.config'), 'experiment-runner', 'auth.json'));
  // Resolve existing parent symlinks before checking repository boundaries, even
  // before the config directory exists. Never use a project's auth.json.
  let parent = dirname(configured);
  const missing = [];
  while (true) {
    try { parent = await realpath(parent); break; }
    catch (error) {
      if (error.code !== 'ENOENT') throw error;
      missing.unshift(basename(parent)); parent = dirname(parent);
    }
  }
  for (let at = parent; ; at = dirname(at)) {
    try { await access(join(at, '.git')); throw new Error('Direct credentials must be outside Git repositories'); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    if (dirname(at) === at) break;
  }
  return join(parent, ...missing, basename(configured));
}

class FileCredentialStore {
  constructor(path) { this.path = path; }

  async contents() {
    let file;
    try { file = await open(this.path, constants.O_RDONLY | constants.O_NOFOLLOW); }
    catch (error) { if (error.code === 'ENOENT') return {}; throw error; }
    try {
      const info = await file.stat();
      if (!info.isFile() || (info.mode & 0o777) !== 0o600 || info.size > 1024 * 1024) {
        throw new Error('Direct credential file must be a private regular file (mode 0600)');
      }
      const value = JSON.parse(await file.readFile('utf8'));
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid credential store');
      return value;
    } finally { await file.close(); }
  }

  async read(provider, { signal } = {}) {
    signal?.throwIfAborted();
    const value = (await this.contents())[provider];
    if (value === undefined) return undefined;
    const text = v => typeof v === 'string' && v.trim().length > 0;
    if (!value || (value.type === 'api_key' ? !text(value.key)
      : value.type === 'oauth' ? !text(value.access) || !text(value.refresh) || !Number.isFinite(value.expires)
      : true)) throw new Error('Invalid stored credential');
    return value;
  }

  async list({ signal } = {}) {
    signal?.throwIfAborted();
    return Object.entries(await this.contents()).map(([providerId, value]) => ({ providerId, type: value.type }));
  }

  async update(provider, fn, { signal } = {}) {
    signal?.throwIfAborted();
    await mkdir(dirname(this.path), { recursive: true, mode: 0o700 });
    const release = await lockfile.lock(this.path, { realpath: false,
      retries: { retries: 100, minTimeout: 100, maxTimeout: 250 } });
    const temporary = this.path + '.' + randomUUID() + '.tmp';
    try {
      signal?.throwIfAborted();
      const data = await this.contents();
      const next = await fn(data[provider]);
      signal?.throwIfAborted();
      if (next === undefined) return data[provider];
      if (next === null) delete data[provider];
      else data[provider] = next;
      const output = await open(temporary, 'wx', 0o600);
      try { await output.writeFile(JSON.stringify(data) + '\n'); await output.sync(); }
      finally { await output.close(); }
      await rename(temporary, this.path);
      return data[provider];
    } finally { await rm(temporary, { force: true }); await release(); }
  }

  modify(provider, fn, options) { return this.update(provider, fn, options); }
  delete(provider, options) { return this.update(provider, async () => null, options); }
}

async function providerModels(providerName) {
  const provider = providerName === 'openai-codex' ? openaiCodexProvider()
    : providerName === 'openrouter' ? openrouterProvider() : null;
  if (!provider) throw new Error('Direct authentication supports openai-codex and openrouter');
  const path = await credentialPath();
  const models = createModels({ credentials: new FileCredentialStore(path) });
  models.setProvider(provider);
  return { models, provider, path };
}

export async function directConnection(providerName) {
  try {
    const { models, provider } = await providerModels(providerName);
    const resolved = await models.getAuth(providerName, { signal: AbortSignal.timeout(30000) });
    const apiKey = resolved?.auth.apiKey;
    if (typeof apiKey !== 'string' || !apiKey.trim()) throw new Error();
    // Account identity stays stable when subscription tokens rotate. API-key
    // accounts use a digest; credentials never enter queue state or receipts.
    const account = providerName === 'openai-codex'
      ? JSON.parse(Buffer.from(apiKey.split('.')[1], 'base64url').toString('utf8'))
        ['https://api.openai.com/auth']?.chatgpt_account_id
      : apiKey;
    if (typeof account !== 'string' || !account) throw new Error();
    const hash = value => createHash('sha256').update(value).digest('hex');
    return { apiKey, baseUrl: provider.baseUrl, scopes: {
      account: hash(`direct\0${providerName}\0${account}`),
      transport: hash(`direct\0${providerName}\0${provider.baseUrl}`),
    } };
  } catch {
    // Provider errors can contain token responses. Never print their bodies.
    throw new Error(`Direct authentication unavailable for ${providerName}; check the private credential file or log in again. OpenRouter also accepts OPENROUTER_API_KEY when no credential is stored.`);
  }
}

export async function loginDirect(providerName) {
  const { models, path } = await providerModels(providerName);
  const input = createInterface({ input: process.stdin, output: process.stderr });
  const abort = new AbortController();
  const stop = () => abort.abort(new Error('Login cancelled'));
  process.once('SIGINT', stop); process.once('SIGTERM', stop);
  try {
    await models.login(providerName, 'oauth', {
      signal: abort.signal,
      prompt: async prompt => {
        if (prompt.type === 'secret') throw new Error('Use the provider browser login');
        if (prompt.type === 'select') {
          process.stderr.write(prompt.options.map((option, index) => `${index + 1}. ${option.label}`).join('\n') + '\n');
        }
        const answer = await input.question(prompt.message + ' ', { signal: prompt.signal ?? abort.signal });
        if (prompt.type !== 'select') return answer;
        const selected = prompt.options[Number(answer.trim()) - 1];
        if (!selected) throw new Error('Invalid selection');
        return selected.id;
      },
      notify: event => {
        if (event.type === 'auth_url') process.stderr.write(`Open: ${event.url}\n${event.instructions ?? ''}\n`);
        else if (event.type === 'device_code') process.stderr.write(`Open: ${event.verificationUri}\nCode: ${event.userCode}\n`);
        else if (event.message) process.stderr.write(event.message + '\n');
      },
    });
    process.stdout.write(`Direct credentials saved to ${path}\n`);
  } catch { throw new Error('Direct login failed or was cancelled; no credential details printed'); }
  finally {
    input.close(); process.off('SIGINT', stop); process.off('SIGTERM', stop);
  }
}
