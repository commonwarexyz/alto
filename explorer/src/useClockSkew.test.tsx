import { afterEach, beforeEach, expect, jest, test } from '@jest/globals';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';
import { useClockSkew } from './useClockSkew';

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const originalFetch = globalThis.fetch;
const originalTimeout = Object.getOwnPropertyDescriptor(AbortSignal, 'timeout');
let container: HTMLDivElement;
let root: Root | null;
let sample: string | Error | null;
let elapsed: number;
let clockSkew: number;

function Clock() {
    const adjustTime = useClockSkew();
    return <output>{adjustTime(12345)}</output>;
}

beforeEach(() => {
    jest.useFakeTimers();
    elapsed = 0;
    clockSkew = 200;
    jest.spyOn(Date, 'now').mockImplementation(() => 1_000_000 + elapsed);
    jest.spyOn(performance, 'now').mockImplementation(() => elapsed);
    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
    Object.defineProperty(AbortSignal, 'timeout', {
        configurable: true,
        value: jest.fn(() => new AbortController().signal),
    });
    sample = null;
    globalThis.fetch = jest.fn<ReturnType<typeof fetch>, Parameters<typeof fetch>>().mockImplementation(async (_, options) => {
        if (options?.method === 'HEAD') {
            return { ok: true } as Response;
        }
        const midpoint = 1_000_000 + elapsed + 50;
        elapsed += 100;
        if (sample instanceof Error) throw sample;
        const text = sample ?? `ts=${(midpoint - clockSkew) / 1000}\n`;
        return { ok: true, text: async () => text } as Response;
    });
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
});

afterEach(async () => {
    await act(async () => root?.unmount());
    container.remove();
    globalThis.fetch = originalFetch;
    if (originalTimeout) {
        Object.defineProperty(AbortSignal, 'timeout', originalTimeout);
    } else {
        Reflect.deleteProperty(AbortSignal, 'timeout');
    }
    jest.restoreAllMocks();
    jest.useRealTimers();
});

async function mount() {
    await act(async () => root!.render(<Clock />));
}

test('warms the connection and measures with uncached GETs when HEAD requests fail CORS', async () => {
    const fetchSample = jest.mocked(globalThis.fetch).getMockImplementation()!;
    jest.mocked(globalThis.fetch).mockImplementation((url, options) => {
        if (options?.method === 'HEAD') return Promise.reject(new TypeError('CORS blocked HEAD'));
        return fetchSample(url, options);
    });
    await mount();

    expect(container.textContent).toBe('12145');
    expect(globalThis.fetch).toHaveBeenCalledTimes(2);
    for (const request of [1, 2]) {
        expect(globalThis.fetch).toHaveBeenNthCalledWith(request, 'https://1.1.1.1/cdn-cgi/trace', {
            cache: 'no-store',
            signal: expect.any(AbortSignal),
        });
    }
    expect(AbortSignal.timeout).toHaveBeenCalledWith(3000);
});

test.each([
    [200, '12145'],
    [-200, '12545'],
] as const)('corrects a %s ms clock offset using the request midpoint', async (offset, adjusted) => {
    clockSkew = offset;
    await mount();

    expect(container.textContent).toBe(adjusted);
});

test('excludes cold connection setup from the measured request', async () => {
    let cold = true;
    const fetchSample = jest.mocked(globalThis.fetch).getMockImplementation()!;
    jest.mocked(globalThis.fetch).mockImplementation((url, options) => {
        if (cold) {
            elapsed += 600;
            cold = false;
        }
        return fetchSample(url, options);
    });
    await mount();

    expect(container.textContent).toBe('12145');
});

test('excludes response body download time from the measured round trip', async () => {
    const fetchSample = jest.mocked(globalThis.fetch).getMockImplementation()!;
    jest.mocked(globalThis.fetch).mockImplementation(async (url, options) => {
        const response = await fetchSample(url, options);
        return {
            ...response,
            text: async () => {
                elapsed += 800;
                return response.text();
            },
        } as Response;
    });
    await mount();

    expect(container.textContent).toBe('12145');
});

test('retains a valid correction through failed samples, recovers, and stops polling on unmount', async () => {
    await mount();
    expect(container.textContent).toBe('12145');

    for (const invalid of [
        new TypeError('Network unavailable'),
        'ip=127.0.0.1\n', 'ts=\n', 'ts=999.850junk\n',
        'ts=NaN\n', 'ts=Infinity\n', 'ts=-Infinity\n', 'ts=1e309\n', 'ts=1e307\n',
    ]) {
        sample = invalid;
        await act(async () => jest.advanceTimersByTime(15000));
        expect(container.textContent).toBe('12145');
    }

    sample = null;
    clockSkew = -200;
    await act(async () => jest.advanceTimersByTime(15000));
    expect(container.textContent).toBe('12545');

    await act(async () => {
        root!.unmount();
        root = null;
    });
    const requests = jest.mocked(globalThis.fetch).mock.calls.length;
    await act(async () => jest.advanceTimersByTime(30000));
    expect(globalThis.fetch).toHaveBeenCalledTimes(requests);
});
