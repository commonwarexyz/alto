import { useState, useEffect } from 'react';

// External time oracle for estimating local clock skew.
// The trace response's `ts` field reports seconds since the Unix epoch.
const endpoint = 'https://1.1.1.1/cdn-cgi/trace';

// Timeout for each request, in milliseconds
const timeout = 3000;

// Interval between refinement samples, in milliseconds. Not a multiple of a second, so successive
// samples fall at different points within the oracle's second instead of at one fixed phase.
const interval = 15_333;

// A burst samples for just over a second so the oracle's second rolls over within it
const burstDuration = 1100;

// Minimum spacing between burst samples, in milliseconds
const burstSpacing = 40;

/** Bounds on the local clock's skew implied by one sample: the skew lies in (lower, upper]. */
interface Bounds {
    lower: number;
    upper: number;
}

/**
 * Bounds local clock skew using a single oracle response. The oracle reads its clock somewhere
 * between request start and response headers. A reported time S with resolution R, both in
 * milliseconds, represents an actual oracle time in [S, S + R).
 *
 * Skew is therefore greater than the local request start minus (S + R), and at most the local
 * response arrival minus S. Connection and network delays widen this interval.
 */
async function sample(): Promise<Bounds> {
    // Anchor in wall time and measure duration monotonically. Stop at response headers
    // so body delivery and parsing do not inflate the measured round trip.
    const startTime = performance.now();
    const localStartTime = Date.now();
    const response = await fetch(endpoint, {
        cache: 'no-store',
        signal: AbortSignal.timeout(timeout),
    });

    const elapsed = performance.now() - startTime;
    if (!response.ok) {
        throw new Error(`API returned status ${response.status}`);
    }

    // Trace timestamps are Unix seconds, while browser timestamps and skew bounds use milliseconds.
    const text = await response.text();
    const ts = text.split('\n').find(line => line.startsWith('ts='))?.slice(3) ?? '';
    const serverTime = Number(ts) * 1000;
    if (!Number.isFinite(serverTime) || serverTime <= 0) {
        throw new Error('Invalid ts field');
    }

    // Whole seconds, unless the field carries a non-zero fraction and so has millisecond precision
    const resolution = /\.\d*[1-9]/.test(ts) ? 1 : 1000;
    return {
        lower: localStartTime - serverTime - resolution,
        upper: localStartTime + elapsed - serverTime,
    };
}

/**
 * Estimates local clock skew against the oracle by intersecting bounds from multiple samples.
 *
 * A whole-second timestamp leaves roughly a second of uncertainty. The startup burst seeks
 * observations on both sides of a rollover. Successful samples on either side narrow the
 * interval to their spacing plus the request round trip. Periodic samples refine those bounds.
 *
 * A contradiction means the stored bounds no longer describe the clock, so acquisition starts
 * again with a warm connection and fresh samples. The last published correction stays in use
 * until a new acquisition succeeds, including when requests or timestamp parsing fail.
 */
export const useClockSkew = () => {
    const [clockSkew, setClockSkew] = useState<number>(0);

    useEffect(() => {
        let active = true;
        let lower = -Infinity;
        let upper = Infinity;

        // Compatible samples narrow the current interval. A contradictory sample replaces it.
        // Return whether replacement was necessary so a refinement can trigger fresh acquisition.
        const updateBounds = (bounds: Bounds): boolean => {
            const reset = bounds.lower > upper || bounds.upper < lower;
            lower = reset ? bounds.lower : Math.max(lower, bounds.lower);
            upper = reset ? bounds.upper : Math.min(upper, bounds.upper);
            return reset;
        };
        const report = (err: unknown) => {
            if (active) console.error('Failed to fetch skew:', err);
        };

        const refresh = async () => {
            try {
                // An uninitialized clock must warm before its first measurement. Once initialized,
                // a periodic sample can narrow the bounds or trigger a fresh acquisition.
                if (upper === Infinity || updateBounds(await sample())) {
                    // A periodic request may finish after cleanup, when it must not start a warmup.
                    if (!active) return;

                    // The detection request may include connection setup. Discard its bounds and
                    // drain an unmeasured warmup before recording fresh samples. If acquisition
                    // fails, infinite bounds force another warmup while the published skew survives.
                    lower = -Infinity;
                    upper = Infinity;
                    const warmup = await fetch(endpoint, {
                        cache: 'no-store',
                        signal: AbortSignal.timeout(timeout),
                    });
                    await warmup.text();

                    // Samples just before and after a second rollover constrain opposite sides
                    // of the skew interval, so acquisition spans slightly more than one second.
                    const end = performance.now() + burstDuration;
                    while (active) {
                        // One failed request must not discard bounds from successful samples.
                        const next = performance.now() + burstSpacing;
                        try {
                            updateBounds(await sample());
                        } catch (err) {
                            report(err);
                        }
                        if (!active || performance.now() >= end) break;
                        await new Promise(resolve => setTimeout(resolve, Math.max(0, next - performance.now())));
                    }
                }

                // Publish only an active, initialized estimate. Its midpoint still has uncertainty
                // from sampling and network delays. Rounding keeps corrected timestamps integral.
                if (active && upper < Infinity) {
                    const skew = Math.round((lower + upper) / 2);
                    console.log(`Measured clock skew: ${skew}ms (±${Math.round((upper - lower) / 2)}ms)`);
                    setClockSkew(skew);
                }
            } catch (err) {
                report(err);
            }
        };

        // Estimate skew immediately, then refresh periodically
        refresh();
        const intervalId = setInterval(refresh, interval);
        return () => {
            // Stop polling and ignore completions from this effect after cleanup
            active = false;
            clearInterval(intervalId);
        };
    }, []);

    // Convert browser wall time to the time oracle's clock
    return (timestamp: number): number => timestamp - clockSkew;
};
