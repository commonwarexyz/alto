import { afterEach, beforeEach, describe, expect, jest, test } from '@jest/globals';
import { act, StrictMode } from 'react';
import { createRoot, Root } from 'react-dom/client';
import App from './App';
import { sha256 } from './alto_types/alto_types.js';
import { getClusterConfig } from './config';
import { ConsensusWorkerPool } from './consensusWorkerPool';
import { createConsensusWorker } from './createConsensusWorker';
import StatsSection from './StatsSection';
import { CertifiedBlockJs } from './types';
import { hexToUint8Array } from './utils';

jest.mock('./createConsensusWorker', () => {
  const { jest } = require('@jest/globals');
  return { createConsensusWorker: jest.fn() };
});
jest.mock('./alto_types/alto_types.js', () => {
  const { jest } = require('@jest/globals');
  return {
    __esModule: true,
    default: async () => {},
    sha256: jest.fn(),
  };
});
jest.mock('./config', () => {
  const { jest } = require('@jest/globals');
  const actual = jest.requireActual('./config') as typeof import('./config');
  const config = {
    BACKEND_URL: 'localhost', PUBLIC_KEY_HEX: '00', LOCATIONS: [], name: 'Test', description: '',
  };
  return {
    getClusterConfig: () => config,
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
  reply(artifact: CertifiedBlockJs | null) {
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
// Encoded FN-DSA-512 participant set: a varint count of 4, then four 897-byte public keys.
const fnDsaIdentityHex = '04' + [1, 2, 3, 4].map(key => key.toString(16).padStart(2, '0').repeat(897)).join('');
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
  const config = getClusterConfig();
  config.BACKEND_URL = 'localhost';
  config.PUBLIC_KEY_HEX = '00';
  config.description = '';
  config.LOCATIONS = [];
  delete config.PARTICIPANTS;
  jest.mocked(sha256).mockReset().mockReturnValue(new Uint8Array(32).fill(0x5a));
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

async function reply(socket: FakeWebSocket, kind: number, artifact: CertifiedBlockJs | null) {
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
  test('keeps newest views after overload followed by delayed notarizations', async () => {
    jest.spyOn(console, 'warn').mockImplementation(() => {});
    const socket = await mount(strict);
    for (let view = 300; view <= 427; view++) {
      await deliver(socket, view);
    }

    // Notarization uploads can arrive after their finalizations.
    const burst = [
      { kind: 2, artifact: certifiedBlock(299) },
      { kind: 2, artifact: certifiedBlock(20) },
      ...Array.from({ length: 255 }, (_, index) => ({
        kind: 1, artifact: certifiedBlock(300 + (index % 128)),
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
    const config = getClusterConfig();
    config.PARTICIPANTS = ['01', '02', '03'];
    config.LOCATIONS = [[[1, 2], 'First'], null, [[3, 4], 'Third']];
    const socket = await mount(strict);
    expect(container.querySelector('.map-container')).not.toBeNull();

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

  test.each([
    { backend: 'same-host', indexer: window.location.origin },
    { backend: 'pq.example.test', indexer: 'http://pq.example.test' },
  ])('describes a $backend deployment without inlining its participant set', async ({ backend, indexer }) => {
    const config = getClusterConfig();
    config.BACKEND_URL = backend === 'same-host' ? window.location.host : backend;
    config.PUBLIC_KEY_HEX = fnDsaIdentityHex;
    config.description = 'A cluster of <strong>4 validators</strong> running c7gd.4xlarge in <strong>1 region</strong> (us-west-2).';
    await mount(strict);
    expect(container.querySelector('.map-container')).toBeNull();
    await act(async () => { container.querySelector<HTMLButtonElement>('.about-header-button')!.click(); });

    const modal = container.querySelector('.about-modal')!;
    const codes = Array.from(modal.querySelectorAll('code'), element => element.textContent);
    expect(codes).toContain('cargo install --git https://github.com/commonwarexyz/alto --branch pq alto-inspector');
    expect(codes).toContain(
      `inspector get block 10 --indexer '${indexer}' --identity "$(cat identity.hex)"`,
    );
    expect(modal.textContent).not.toContain(fnDsaIdentityHex.slice(0, 64));
    expect(Array.from(modal.querySelectorAll('strong')).some(element => element.textContent === '4 validators')).toBe(true);
    expect(modal.textContent).toContain('c7gd.4xlarge in 1 region (us-west-2)');
    expect(modal.textContent).not.toContain('commonware_deployer::aws');
    expect(modal.querySelector('a[href="https://github.com/commonwarexyz/alto/tree/main/indexer"]')).not.toBeNull();
    expect(modal.querySelector('a[href="https://github.com/commonwarexyz/monorepo/blob/1950760f8bc64f6d0c45bef3d68c0947c94284b2/cryptography/src/fn_dsa/mod.rs"]')).not.toBeNull();

    await act(async () => { modal.querySelector<HTMLButtonElement>('.about-button')!.click(); });
    expect(container.querySelector('.about-modal')).toBeNull();
  });

  test('summarizes an FN-DSA participant set by count and digest', async () => {
    getClusterConfig().PUBLIC_KEY_HEX = fnDsaIdentityHex;
    await mount(strict);
    await act(async () => { container.querySelector<HTMLButtonElement>('.key-header-button')!.click(); });

    const modal = container.querySelector('.about-modal')!;
    expect(modal.textContent).toContain('contains 4 validators');
    expect(modal.querySelector('.code-block')?.textContent).toBe('5a'.repeat(32));
    expect(sha256).toHaveBeenLastCalledWith(hexToUint8Array(fnDsaIdentityHex));
    expect(modal.textContent).not.toContain(fnDsaIdentityHex.slice(0, 64));
    const download = modal.querySelector<HTMLAnchorElement>('a[download="identity.hex"]')!;
    expect(download.getAttribute('href')).toBe(`data:text/plain;charset=utf-8,${fnDsaIdentityHex}`);
    expect(modal.querySelector('a[href="https://github.com/commonwarexyz/monorepo/blob/1950760f8bc64f6d0c45bef3d68c0947c94284b2/cryptography/src/fn_dsa/mod.rs"]')).not.toBeNull();
  });

  test.each(['idle', 'rejected', 'duplicate'])(
    'advances unknown-view latency while batches are %s',
    async batch => {
      // Finalizing view 12 after view 10 inserts an unknown placeholder for view 11.
      const socket = await mount(strict);
      await reply(socket, 2, certifiedBlock(12));
      await advance(100);
      expect(container.querySelector('.growing-latency')?.textContent).toBe('100ms');

      for (let tick = 1; tick <= 10; tick++) {
        if (batch === 'rejected') {
          await reply(socket, 2, null);
        } else if (batch === 'duplicate') {
          await reply(socket, 2, certifiedBlock(12));
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
