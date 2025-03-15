import { useState, useEffect, useRef } from 'react';

// The source to use as a time oracle
const endpoint = 'https://1.1.1.1/cdn-cgi/trace';

// Timeout for any request (in milliseconds)
const timeout = 3000;

// Interval to fetch server time (in milliseconds)
const interval = 30000;

/**
 * Custom hook to detect clock skew between client and server
 * Runs once on mount and then every 30 seconds, using the latest successful measurement as the skew
 */
export const useClockSkew = () => {
    const [clockSkew, setClockSkew] = useState<number>(0);
    const isFirstMountRef = useRef(true);

    useEffect(() => {
        const fetchSkew = async () => {
            try {
                // Establish connection with a HEAD request
                const controller = new AbortController();
                const connectionTimeoutId = setTimeout(() => {
                    controller.abort('Connection timeout exceeded');
                }, timeout);

                try {
                    await fetch(endpoint, {
                        method: 'HEAD',
                        signal: controller.signal,
                    });
                    clearTimeout(connectionTimeoutId);
                } catch (error) {
                    if (!(error instanceof DOMException && error.name === 'AbortError')) {
                        throw error;
                    }
                    clearTimeout(connectionTimeoutId);
                }

                // Perform the GET request to fetch server time
                const startTime = performance.now();
                const localStartTime = Date.now();
                const response = await fetch(endpoint, {
                    signal: AbortSignal.timeout(timeout),
                });
                if (!response.ok) {
                    throw new Error(`API returned status ${response.status}`);
                }
                const endTime = performance.now();
                const networkLatency = Math.floor((endTime - startTime) / 4);

                // Parse server time from the response
                const text = await response.text();
                const lines = text.split('\n');
                const tsLine = lines.find(line => line.startsWith('ts='));
                if (!tsLine) {
                    throw new Error('ts field not found in response');
                }
                const serverTimeStr = tsLine.substring(3);
                const serverTimeFloat = parseFloat(serverTimeStr);
                if (isNaN(serverTimeFloat)) {
                    throw new Error('Invalid ts field format');
                }
                const serverTime = Math.floor(serverTimeFloat * 1000); // Convert to ms

                // Calculate skew
                const adjustedLocalTime = localStartTime + networkLatency;
                const skew = adjustedLocalTime - serverTime;
                console.log('Local clock skew:', skew);

                // Update state with the new skew
                setClockSkew(skew);
            } catch (err) {
                console.error('Failed to fetch skew:', err);
                // Keep the previous skew if the request fails
            }
        };

        // Run immediately only on the first mount
        if (isFirstMountRef.current) {
            isFirstMountRef.current = false;
            fetchSkew();
        }

        // Set up an interval to run every 30 seconds
        const intervalId = setInterval(fetchSkew, interval);

        // Cleanup interval on unmount
        return () => clearInterval(intervalId);
    }, []);

    // Utility functions
    const adjustTime = (timestamp: number): number => {
        return timestamp - clockSkew;
    };

    return adjustTime;
};