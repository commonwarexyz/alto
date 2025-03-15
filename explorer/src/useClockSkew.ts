import { useState, useEffect } from 'react';

/**
 * A hook that detects and manages clock skew between the user's local time
 * and an external time source.
 *
 * @returns An object containing:
 *   - clockSkew: The detected clock skew in milliseconds (can be positive or negative)
 *   - adjustTime: A function to adjust a timestamp using the detected skew
 *   - loading: Boolean indicating if the skew detection is in progress
 *   - error: Any error that occurred during skew detection
 */
export const useClockSkew = () => {
    const [clockSkew, setClockSkew] = useState<number>(0);
    const [loading, setLoading] = useState<boolean>(true);
    const [error, setError] = useState<Error | null>(null);

    // Calculate clock skew using reliable time services
    useEffect(() => {
        const detectClockSkew = async () => {
            try {
                setLoading(true);

                // Try Cloudflare's time API first
                const skew = await detectClockSkewWithCloudflare() || await detectClockSkewWithGoogle();

                if (skew !== null) {
                    console.log(`Clock skew detected: ${skew}ms (${skew > 0 ? 'ahead' : 'behind'})`);
                    setClockSkew(skew);
                } else {
                    // If both methods fail, default to no adjustment
                    console.warn('Failed to detect clock skew, using system clock');
                    setClockSkew(0);
                }

                setLoading(false);
            } catch (err) {
                console.error('Clock skew detection failed:', err);
                setError(err instanceof Error ? err : new Error(String(err)));
                setLoading(false);

                // Fallback to no skew adjustment in case of error
                setClockSkew(0);
            }
        };

        // Method 1: Use Cloudflare's time.cloudflare.com API
        const detectClockSkewWithCloudflare = async (): Promise<number | null> => {
            try {
                // Record request start time
                const requestStartTime = Date.now();

                // Fetch time from Cloudflare
                const response = await fetch('https://time.cloudflare.com', {
                    // Set a timeout to prevent hanging
                    signal: AbortSignal.timeout(3000)
                });

                // Record request end time
                const requestEndTime = Date.now();

                if (!response.ok) {
                    throw new Error('Cloudflare time API returned non-OK response');
                }

                const data = await response.json();

                // Cloudflare returns time in seconds with fractional part
                const serverTime = data.now * 1000; // Convert to milliseconds

                // Calculate approximate network latency (round-trip / 2)
                const networkLatency = Math.floor((requestEndTime - requestStartTime) / 2);

                // Calculate user's local time at the moment server responded
                const adjustedLocalTime = requestStartTime + networkLatency;

                // Calculate skew (positive means user's clock is ahead, negative means it's behind)
                return adjustedLocalTime - serverTime;
            } catch (err) {
                console.warn('Cloudflare time detection failed:', err);
                return null;
            }
        };

        // Method 2: Use Google's servers via HTTP date header
        const detectClockSkewWithGoogle = async (): Promise<number | null> => {
            try {
                // Record request start time
                const requestStartTime = Date.now();

                // Fetch from Google with a unique query param to prevent caching
                const response = await fetch(`https://www.google.com/generate_204?nocache=${Date.now()}`, {
                    method: 'HEAD',
                    // Set a timeout to prevent hanging
                    signal: AbortSignal.timeout(3000)
                });

                // Record request end time
                const requestEndTime = Date.now();

                if (!response.ok) {
                    throw new Error('Google HEAD request failed');
                }

                // Get Date header from response
                const dateHeader = response.headers.get('date');

                if (!dateHeader) {
                    throw new Error('No date header in Google response');
                }

                // Parse the server timestamp (comes in HTTP date format)
                const serverTime = new Date(dateHeader).getTime();

                // Calculate approximate network latency (round-trip / 2)
                const networkLatency = Math.floor((requestEndTime - requestStartTime) / 2);

                // Calculate user's local time at the moment server responded
                const adjustedLocalTime = requestStartTime + networkLatency;

                // Calculate skew (positive means user's clock is ahead, negative means it's behind)
                return adjustedLocalTime - serverTime;
            } catch (err) {
                console.warn('Google time detection failed:', err);
                return null;
            }
        };

        detectClockSkew();
    }, []);

    // Function to adjust any timestamp using the detected skew
    const adjustTime = (timestamp: number): number => {
        return timestamp - clockSkew;
    };

    return { clockSkew, adjustTime, loading, error };
};