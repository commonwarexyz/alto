import { useState, useEffect } from 'react';

// External time oracle
const endpoint = 'https://1.1.1.1/cdn-cgi/trace';

// Timeout for each request, in milliseconds
const timeout = 3000;

// Interval between samples, in milliseconds
const interval = 15000;

/**
 * Estimates local clock skew on mount and every 15 seconds.
 * Retains the latest successful estimate when a sample fails.
 */
export const useClockSkew = () => {
    const [clockSkew, setClockSkew] = useState<number>(0);

    useEffect(() => {
        let active = true;
        const fetchSkew = async () => {
            try {
                // Warm the connection with a CORS-enabled GET to reduce setup delay in the sample
                // Consume its body before issuing the measured request so the connection can be reused
                const warmup = await fetch(endpoint, {
                    cache: 'no-store',
                    signal: AbortSignal.timeout(timeout),
                });
                await warmup.text();
                if (!active) return;

                // Anchor the sample in wall time and measure its duration with a monotonic clock
                const startTime = performance.now();
                const localStartTime = Date.now();
                const response = await fetch(endpoint, {
                    cache: 'no-store',
                    signal: AbortSignal.timeout(timeout),
                });

                // Stop timing at the response headers, before body delivery and parsing
                const elapsed = performance.now() - startTime;
                if (!response.ok) {
                    throw new Error(`API returned status ${response.status}`);
                }
                const text = await response.text();

                // Trace timestamps are Unix seconds, while browser timestamps are milliseconds
                const ts = text.split('\n').find(line => line.startsWith('ts='))?.slice(3);
                const serverTime = Number(ts) * 1000;
                if (!Number.isFinite(serverTime) || serverTime <= 0) {
                    throw new Error('Invalid ts field');
                }

                // The midpoint assumes similar request and response delays
                // Timestamp precision and network asymmetry limit the estimate's accuracy
                const skew = localStartTime + elapsed / 2 - serverTime;
                if (active) {
                    console.log(`Measured clock skew: ${skew}ms`);
                    setClockSkew(skew);
                }
            } catch (err) {
                // Keep the last successful estimate when either request or timestamp parsing fails
                if (active) console.error('Failed to fetch skew:', err);
            }
        };

        // Sample immediately, then refresh periodically
        fetchSkew();
        const intervalId = setInterval(fetchSkew, interval);
        return () => {
            // Stop polling and ignore completions from this effect after cleanup
            active = false;
            clearInterval(intervalId);
        };
    }, []);

    // Convert browser wall time to the time oracle's clock
    return (timestamp: number): number => timestamp - clockSkew;
};
