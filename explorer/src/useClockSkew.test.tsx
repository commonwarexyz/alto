import { afterEach, beforeEach, expect, jest, test } from '@jest/globals';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';
import { useClockSkew } from './useClockSkew';

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const originalFetch = globalThis.fetch;
const originalTimeout = Object.getOwnPropertyDescriptor(AbortSignal, 'timeout');
let container: HTMLDivElement;
let root: Root | null;
let sample: (oracleTime: number) => string;
const oracleEpoch = 1_000_000;
const roundTrip = 20;
const interval = 15_333;
// A burst narrows the estimate to the spacing of the two samples around the oracle's second
// rollover (the 40 ms pacing) plus one round trip
const tolerance = Math.ceil((40 + roundTrip) / 2);

// Both trace formats use the same oracle clock. The whole-second format includes a `.000`
// suffix even though it has no sub-second precision.
const wholeSecondSample = (oracleTime: number) => `ts=${Math.floor(oracleTime / 1000)}.000\n`;
const millisecondSample = (oracleTime: number) => `ts=${(oracleTime / 1000).toFixed(3)}\n`;

function Clock() {
    const adjustTime = useClockSkew();
    return <output>{adjustTime(12345)}</output>;
}

beforeEach(() => {
    // Jest advances timers, wall time, and monotonic time together. A simulated clock step
    // changes only wall time, preserving elapsed time and scheduled request completions.
    jest.useFakeTimers();
    jest.setSystemTime(oracleEpoch + 200);

    jest.spyOn(console, 'log').mockImplementation(() => {});
    jest.spyOn(console, 'error').mockImplementation(() => {});
    Object.defineProperty(AbortSignal, 'timeout', {
        configurable: true,
        value: jest.fn(() => new AbortController().signal),
    });
    sample = wholeSecondSample;
    globalThis.fetch = jest.fn<ReturnType<typeof fetch>, Parameters<typeof fetch>>().mockImplementation(async () => {
        // The oracle reads its clock halfway through the request. Its monotonic time source
        // keeps a local wall-clock step from also moving the oracle.
        await delay(roundTrip / 2);
        const oracleTime = oracleEpoch + performance.now();
        await delay(roundTrip / 2);
        const text = sample(oracleTime);
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

const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

// Resolve each timer's promises before advancing to the next request or polling deadline.
async function tick(ms: number) {
    let done = false;
    setTimeout(() => { done = true; }, ms);
    while (!done) {
        await act(async () => { jest.advanceTimersToNextTimer(); });
    }
}

async function mount(ms = 1300) {
    await act(async () => root!.render(<Clock />));
    await tick(ms);
}

const shown = () => Number(container.textContent);

test.each([200, -200])('narrows a whole-second oracle with a %s ms clock offset', async (offset) => {
    jest.setSystemTime(oracleEpoch + offset);
    await mount();

    expect(Math.abs(shown() - (12345 - offset))).toBeLessThanOrEqual(tolerance);
    for (const call of jest.mocked(globalThis.fetch).mock.calls) {
        expect(call).toEqual(['https://1.1.1.1/cdn-cgi/trace', {
            cache: 'no-store',
            signal: expect.any(AbortSignal),
        }]);
    }
    expect(AbortSignal.timeout).toHaveBeenCalledWith(3000);
});

test('uses a fractional oracle timestamp at full precision', async () => {
    sample = millisecondSample;
    await mount();

    expect(Math.abs(shown() - 12145)).toBeLessThanOrEqual(roundTrip / 2);
    expect(Number.isInteger(shown())).toBe(true);
});

test.each(['succeed', 'fail'])('discards the cold warmup when measured requests %s', async (outcome) => {
    // In the failure case, only the cold warmup succeeds. Later samples cannot narrow away
    // its setup delay and mask accidental use of the warmup as a measurement.
    const fetchSample = jest.mocked(globalThis.fetch).getMockImplementation()!;
    jest.mocked(globalThis.fetch).mockImplementationOnce(async (url, options) => {
        await delay(600);
        return fetchSample(url, options);
    });
    if (outcome === 'fail') {
        jest.mocked(globalThis.fetch).mockRejectedValue(new TypeError('Network unavailable'));
    }
    await mount(2000);

    if (outcome === 'fail') {
        expect(shown()).toBe(12345);
    } else {
        expect(Math.abs(shown() - 12145)).toBeLessThanOrEqual(tolerance);
    }
});

test('warms the connection again when retrying initial acquisition', async () => {
    // Initial acquisition fails. Its retry pays connection setup before the only successful
    // measured response, so later samples cannot hide a cold measurement's bias.
    sample = millisecondSample;
    const fetchSample = jest.mocked(globalThis.fetch).getMockImplementation()!;
    jest.mocked(globalThis.fetch)
        .mockRejectedValueOnce(new TypeError('Network unavailable'))
        .mockImplementationOnce(async (url, options) => {
            await delay(600);
            return fetchSample(url, options);
        })
        .mockImplementationOnce((url, options) => fetchSample(url, options))
        .mockRejectedValue(new TypeError('Network unavailable'));
    await mount();
    expect(shown()).toBe(12345);
    await tick(interval + 2000);

    expect(Math.abs(shown() - 12145)).toBeLessThanOrEqual(roundTrip / 2);
});

test.each(['warmup', 'burst'])('retains the estimate after a recovery %s failure and retries warm', async (failure) => {
    // Establish a correction before the clock moves, so failed recovery has a value to retain.
    sample = millisecondSample;
    await mount();
    const initial = shown();

    // The request detecting the clock step is cold. Interrupt acquisition at the chosen stage
    // and require that detection-only bounds never replace the published correction.
    jest.setSystemTime(Date.now() + 2000);
    const fetchSample = jest.mocked(globalThis.fetch).getMockImplementation()!;
    jest.mocked(globalThis.fetch).mockImplementationOnce(async (url, options) => {
        await delay(1000);
        return fetchSample(url, options);
    });
    if (failure === 'burst') {
        jest.mocked(globalThis.fetch).mockImplementationOnce((url, options) => fetchSample(url, options));
    }
    jest.mocked(globalThis.fetch).mockRejectedValue(new TypeError('Network unavailable'));
    await tick(interval + 3000);
    expect(shown()).toBe(initial);

    // A failed acquisition must retry through warmup, even when its next request is cold too.
    jest.mocked(globalThis.fetch)
        .mockImplementationOnce(async (url, options) => {
            await delay(600);
            return fetchSample(url, options);
        })
        .mockImplementation((url, options) => fetchSample(url, options));
    await tick(interval);

    expect(Math.abs(shown() - (12345 - 2200))).toBeLessThanOrEqual(roundTrip / 2);
});

test('excludes response body download time from the measured round trip', async () => {
    // A precise oracle isolates body-download time from whole-second timestamp uncertainty.
    sample = millisecondSample;
    const fetchSample = jest.mocked(globalThis.fetch).getMockImplementation()!;
    jest.mocked(globalThis.fetch).mockImplementation(async (url, options) => {
        const response = await fetchSample(url, options);
        return {
            ...response,
            text: async () => {
                await delay(800);
                return response.text();
            },
        } as Response;
    });
    await mount(3000);

    expect(Math.abs(shown() - 12145)).toBeLessThanOrEqual(roundTrip / 2);
});

test('retains sub-second accuracy in a long-lived tab', async () => {
    await mount();
    await tick(70 * interval);
    expect(Math.abs(shown() - 12145)).toBeLessThanOrEqual(tolerance);
});

test.each([-400, 400])('retains the estimate on failure, recovers a %s ms clock step, and stops on unmount', async (step) => {
    await mount();
    const initial = shown();

    // Network and parsing failures must preserve the last successful correction.
    for (const invalid of [
        new TypeError('Network unavailable'),
        'ip=127.0.0.1\n', 'ts=\n', 'ts=999.850junk\n',
        'ts=NaN\n', 'ts=Infinity\n', 'ts=-Infinity\n', 'ts=1e309\n', 'ts=1e307\n',
    ]) {
        sample = () => {
            if (invalid instanceof Error) throw invalid;
            return invalid;
        };
        await tick(interval);
        expect(shown()).toBe(initial);
    }

    // Once a refinement sample contradicts the old estimate, a burst re-acquires the new offset
    sample = wholeSecondSample;
    jest.setSystemTime(Date.now() + step);
    const adjusted = 12345 - (200 + step);
    for (let i = 0; i < 8 && Math.abs(shown() - adjusted) > tolerance; i++) {
        await tick(interval);
    }
    expect(Math.abs(shown() - adjusted)).toBeLessThanOrEqual(tolerance);

    await act(async () => {
        root!.unmount();
        root = null;
    });
    const requests = jest.mocked(globalThis.fetch).mock.calls.length;
    await tick(2 * interval);
    expect(globalThis.fetch).toHaveBeenCalledTimes(requests);
});

test('does not start a warmup when a refinement completes after unmount', async () => {
    await mount();

    // Stop halfway through a request whose result will contradict the current bounds.
    // Completing that request after cleanup must not initiate recovery.
    jest.setSystemTime(Date.now() + 2000);
    await tick(interval + roundTrip / 2 - performance.now());
    await act(async () => {
        root!.unmount();
        root = null;
    });
    const requests = jest.mocked(globalThis.fetch).mock.calls.length;
    await tick(roundTrip);

    expect(globalThis.fetch).toHaveBeenCalledTimes(requests);
});
