import { afterEach, beforeEach, describe, expect, jest, test } from '@jest/globals';
import { act, StrictMode } from 'react';
import { createRoot, Root } from 'react-dom/client';
import App from './App';
import { leader_index } from './alto_types/alto_types.js';
import { getClusterConfig } from './config';
import { ConsensusWorkerPool } from './consensusWorkerPool';
import { createConsensusWorker } from './createConsensusWorker';
import StatsSection from './StatsSection';
import { CertifiedBlockJs, SeedJs } from './types';

jest.mock('./createConsensusWorker', () => {
  const { jest } = require('@jest/globals');
  return { createConsensusWorker: jest.fn() };
});
jest.mock('./alto_types/alto_types.js', () => {
  const { jest } = require('@jest/globals');
  return { __esModule: true, default: async () => {}, leader_index: jest.fn(() => 0) };
});
jest.mock('./config', () => {
  const { jest } = require('@jest/globals');
  const actual = jest.requireActual('./config') as typeof import('./config');
  const config = {
    BACKEND_URL: 'localhost', PUBLIC_KEY_HEX: '00', LOCATIONS: [],
    CERTIFICATE_MODE: 'standard', name: 'Test', description: '',
  };
  return {
    MODE: 'public',
    getInitialCluster: () => 'local',
    getClusterConfig: () => config,
    getClusters: () => ({ local: config }),
    getHttpBackendUrl: actual.getHttpBackendUrl,
    getWebSocketBackendUrl: actual.getWebSocketBackendUrl,
  };
});
jest.mock('./useClockSkew', () => {
  const adjustTime = (time: number) => time;
  return { useClockSkew: () => adjustTime };
});
jest.mock('react-leaflet', () => ({
  useMap: () => ({}),
  MapContainer: ({ children }: { children: React.ReactNode }) => children,
  TileLayer: () => null,
  Marker: ({ children }: { children: React.ReactNode }) => children,
  Popup: ({ children }: { children: React.ReactNode }) => children,
}));
jest.mock('leaflet', () => ({ LatLng: class {}, DivIcon: class {} }));
jest.mock('./KeyModal', () => () => null);
jest.mock('./MaintenancePage', () => () => null);
jest.mock('./SearchModal', () => () => null);
jest.mock('./StatsSection', () => {
  const { jest } = require('@jest/globals');
  return jest.fn(() => null);
});

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

class FakeWorker {
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((event: ErrorEvent) => void) | null = null;
  pending: { kind: number; payload: Uint8Array } | null = null;
  terminated = false;

  postMessage(message: { kind: number; payload: Uint8Array }) { this.pending = message; }
  terminate() { this.terminated = true; }
  fail() { this.onerror?.({ preventDefault() {} } as ErrorEvent); }
  reply(artifact: CertifiedBlockJs | SeedJs | null) {
    expect(this.pending).not.toBeNull();
    this.pending = null;
    this.onmessage?.({ data: { artifact } } as MessageEvent);
  }
}

class FakeWebSocket {
  onopen: (() => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  closed = false;

  constructor() { sockets.push(this); }
  close() { this.closed = true; }
}

const originalFetch = globalThis.fetch;
const originalWebSocket = globalThis.WebSocket;
let workers: FakeWorker[];
let sockets: FakeWebSocket[];
let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(100_000);
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
  workers = [];
  sockets = [];
  const config = getClusterConfig('local');
  config.BACKEND_URL = 'localhost';
  config.PUBLIC_KEY_HEX = '00';
  config.CERTIFICATE_MODE = 'standard';
  config.description = '';
  config.LOCATIONS = [];
  delete config.PARTICIPANTS;
  jest.mocked(leader_index).mockReset().mockReturnValue(0);
  jest.mocked(StatsSection).mockClear();
  jest.mocked(createConsensusWorker).mockImplementation(() => {
    const worker = new FakeWorker();
    workers.push(worker);
    return worker as unknown as Worker;
  });
  globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
  globalThis.fetch = jest.fn<ReturnType<typeof fetch>, Parameters<typeof fetch>>()
    .mockResolvedValue({ status: 200 } as Response);
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  globalThis.fetch = originalFetch;
  globalThis.WebSocket = originalWebSocket;
  jest.clearAllTimers();
  jest.useRealTimers();
  jest.restoreAllMocks();
});

async function advance(ms: number) {
  await act(async () => { jest.advanceTimersByTime(ms); });
}

const certifiedBlock = (view: number, leader: number[] = []): CertifiedBlockJs => ({
  view, signature: [],
  block: { leader, height: view, timestamp: 99_900, digest: [], parent: [] },
});

async function reply(socket: FakeWebSocket, kind: number, artifact: CertifiedBlockJs | SeedJs | null) {
  await act(async () => {
    socket.onmessage?.({ data: new Uint8Array([kind, 0]).buffer } as MessageEvent);
    const worker = workers.find(worker => !worker.terminated && worker.pending);
    expect(worker).toBeDefined();
    worker!.reply(artifact);
  });
}

async function deliver(socket: FakeWebSocket, view: number) {
  await reply(socket, 2, certifiedBlock(view));
  await advance(100);
  expect(container.textContent).toContain(`#${view} |`);
}

async function mount(strict: boolean) {
  await act(async () => { root.render(strict ? <StrictMode><App /></StrictMode> : <App />); });
  expect(sockets).toHaveLength(1);
  await act(async () => { sockets[0].onopen?.(); });
  await deliver(sockets[0], 10);
  return sockets[0];
}

async function close(socket: FakeWebSocket) {
  await act(async () => { socket.onclose?.({ code: 1000 } as CloseEvent); });
}

describe.each([false, true])('connection lifecycle (StrictMode: %s)', strict => {
  test('keeps newest views after overload followed by delayed seeds', async () => {
    getClusterConfig('local').CERTIFICATE_MODE = 'vrf';
    jest.spyOn(console, 'warn').mockImplementation(() => {});
    const socket = await mount(strict);
    for (let view = 300; view <= 427; view++) {
      await deliver(socket, view);
    }

    // Seed and notarization uploads can arrive after their finalizations.
    const burst = [
      { kind: 2, artifact: certifiedBlock(299) },
      { kind: 2, artifact: certifiedBlock(20) },
      ...Array.from({ length: 127 }, (_, index) => ({
        kind: 0, artifact: { view: 300 + index, signature: [] },
      })),
      ...Array.from({ length: 128 }, (_, index) => ({
        kind: 1, artifact: certifiedBlock(300 + index),
      })),
    ];
    const drain = jest.spyOn(ConsensusWorkerPool.prototype, 'drain');

    // Exceed the real consumption window before any worker replies.
    await act(async () => {
      burst.forEach((fixture, id) => {
        const frame = new Uint8Array(5);
        frame[0] = fixture.kind;
        new DataView(frame.buffer).setUint32(1, id);
        socket.onmessage?.({ data: frame.buffer } as MessageEvent);
      });

      // Match replies to dispatched payloads as workers reuse their slots.
      while (true) {
        const worker = workers.find(worker => !worker.terminated && worker.pending);
        if (!worker) break;
        const { kind, payload } = worker.pending!;
        const id = new DataView(payload.buffer, payload.byteOffset, payload.byteLength).getUint32(0);
        expect(kind).toBe(burst[id].kind);
        worker.reply(burst[id].artifact);
      }
    });

    // Check after the whole retained burst, with no later artifact left to repair its order.
    await advance(100);
    const batch = drain.mock.results[0].value as ReturnType<ConsensusWorkerPool['drain']>;
    expect(batch).toHaveLength(256);
    expect(batch[0]).toMatchObject({ kind: 2, artifact: { view: 20 }, skipped: 1 });
    expect(Array.from(container.querySelectorAll('.view-number'), node => Number(node.textContent)))
      .toEqual(Array.from({ length: 50 }, (_, index) => 427 - index));
    expect(jest.mocked(StatsSection).mock.calls.at(-1)?.[0].views.map(view => view.view))
      .toEqual(Array.from({ length: 128 }, (_, index) => 427 - index));

    await advance(5000);
    expect(Array.from(container.querySelectorAll('.view-number'), node => Number(node.textContent)))
      .toEqual(Array.from({ length: 50 }, (_, index) => 427 - index));
  });

  test('keeps known leader locations aligned across an unmapped validator', async () => {
    const config = getClusterConfig('local');
    config.PARTICIPANTS = ['01', '02', '03'];
    config.LOCATIONS = [[[1, 2], 'First'], null, [[3, 4], 'Third']];
    const socket = await mount(strict);

    for (const [view, leader] of [[11, 2], [12, 3], [9, 1]]) {
      await reply(socket, 2, certifiedBlock(view, [leader]));
      await advance(100);
      if (leader === 2) {
        expect(container.textContent).not.toContain('Location:');
      } else {
        expect(container.textContent).toContain('Location: Third');
      }
    }
    expect(container.querySelector('.overlay-value')?.textContent).toBe('3');
  });

  test.each([false, true])('maps seeded leaders with full cardinality (participant keys: %s)', async withParticipants => {
    const config = getClusterConfig('local');
    config.CERTIFICATE_MODE = 'vrf';
    config.LOCATIONS = [[[1, 2], 'First'], null, [[3, 4], 'Third']];
    if (withParticipants) config.PARTICIPANTS = ['01', '02', '03'];
    const socket = await mount(strict);

    for (const index of [1, 2]) {
      jest.mocked(leader_index).mockReturnValue(index);
      const seed = { view: 9 + index, signature: [] };
      await reply(socket, 0, seed);
      await advance(100);
      expect(leader_index).toHaveBeenLastCalledWith(seed, 3);
      if (index === 1) {
        expect(container.textContent).not.toContain('Location:');
      } else {
        expect(container.textContent).toContain('Location: Third');
      }
    }
    expect(container.querySelector('.overlay-value')?.textContent).toBe('3');

    // A seed-only leader remains mapped even when its view times out.
    await advance(5000);
    expect(container.textContent).toContain('View: 12');
    expect(container.textContent).toContain('Location: Third');
    expect(jest.mocked(StatsSection).mock.calls.slice(-1)[0][0].views
      .find(view => view.view === 12)?.status).toBe('timed_out');

    if (withParticipants) {
      // A certified block identifies its proposer independently of the seed prediction.
      await reply(socket, 2, certifiedBlock(12, [1]));
      await advance(100);
      expect(container.textContent).toContain('Location: First');
      await reply(socket, 0, { view: 11, signature: [] });
      await advance(100);
      expect(container.textContent).toContain('Location: First');
      expect(jest.mocked(StatsSection).mock.calls.slice(-1)[0][0].views
        .find(view => view.view === 12)?.status).toBe('finalized');
    }
  });

  test.each(['standard', 'vrf'] as const)('describes the selected %s deployment in About', async mode => {
    const config = getClusterConfig('local');
    config.CERTIFICATE_MODE = mode;
    config.BACKEND_URL = mode === 'standard' ? window.location.host : 'vrf.example.test';
    config.PUBLIC_KEY_HEX = (mode === 'standard' ? 'ab' : 'cd').repeat(48);
    config.description = 'A cluster of <strong>4 validators</strong> running c7gd.4xlarge in <strong>1 region</strong> (us-west-2).';
    await mount(strict);
    await act(async () => { container.querySelector<HTMLButtonElement>('.about-header-button')!.click(); });

    const modal = container.querySelector('.about-modal');
    expect(modal).not.toBeNull();
    const command = Array.from(modal!.querySelectorAll('code'))
      .find(element => element.textContent?.startsWith('inspector '))!.textContent;
    const indexer = mode === 'standard' ? window.location.origin : 'https://vrf.example.test';
    expect(command).toContain(`--certificate-mode '${mode}'`);
    expect(command).toContain(`--indexer '${indexer}'`);
    expect(command).toContain(`--identity '${config.PUBLIC_KEY_HEX}'`);
    expect(modal!.textContent).toContain('4 validators');
    expect(modal!.textContent).toContain('c7gd.4xlarge');
    expect(modal!.textContent).toContain('1 region');
    expect(modal!.textContent).toContain('us-west-2');
    expect(Array.from(modal!.querySelectorAll('strong')).some(element => element.textContent === '4 validators')).toBe(true);
    expect(modal!.textContent).not.toMatch(/50 validators|c8g\.large|USA Cluster|exoware::relay/);
    expect(modal!.querySelector('a[href="https://github.com/commonwarexyz/alto/tree/main/indexer"]')).not.toBeNull();
    expect(modal!.querySelector('a[href="https://docs.rs/commonware-cryptography/latest/commonware_cryptography/bls12381/index.html"]')).not.toBeNull();

    await act(async () => { modal!.querySelector<HTMLButtonElement>('.about-button')!.click(); });
    expect(container.querySelector('.about-modal')).toBeNull();
  });

  test.each(['idle', 'rejected', 'duplicate'])(
    'advances growing latency while batches are %s',
    async batch => {
      getClusterConfig('local').CERTIFICATE_MODE = 'vrf';
      const socket = await mount(strict);
      await reply(socket, 0, { view: 10, signature: [] });
      await advance(100);
      expect(container.querySelector('.growing-latency')?.textContent).toBe('100ms');

      for (let tick = 1; tick <= 10; tick++) {
        if (batch === 'rejected') {
          await reply(socket, 2, null);
        } else if (batch === 'duplicate') {
          await reply(socket, 2, certifiedBlock(10));
        }
        // Assert each repaint before another timer can conceal a stalled display.
        await advance(100);
        expect(container.querySelector('.growing-latency')?.textContent).toBe(`${100 + tick * 100}ms`);
      }
    },
  );

  test.each([false, true])('stops on fatal verification failure (reconnect pending: %s)', async pending => {
    const socket = await mount(strict);
    if (pending) await close(socket);

    // Repeated host errors exhaust the real pool's replacement budget.
    await act(async () => {
      workers[0].fail();
      workers[workers.length - 1].fail();
      workers[workers.length - 1].fail();
    });
    expect(container.querySelector('.error-message')?.textContent).toContain('Refresh to reconnect');
    expect(socket.closed).toBe(true);
    expect(workers.every(worker => worker.terminated)).toBe(true);

    // A queued close event must not restart transport after verification has stopped.
    if (!pending) await close(socket);
    await advance(11_000);
    expect(sockets).toHaveLength(1);
    expect(container.textContent).toContain('#10 |');
  });

  test('continues verification after an ordinary socket reconnect', async () => {
    const socket = await mount(strict);
    await close(socket);
    await advance(11_000);
    expect(sockets).toHaveLength(2);
    await act(async () => { sockets[1].onopen?.(); });
    await deliver(sockets[1], 11);
    expect(workers.every(worker => !worker.terminated)).toBe(true);
  });
});
