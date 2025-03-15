import { useState, useEffect, useRef } from 'react';

/**
 * Options for the clock skew detection
 */
interface ClockSkewOptions {
    /** The endpoint URL to fetch server time from */
    endpoint?: string;
    /** Number of samples to collect for statistical analysis */
    sampleCount?: number;
    /** Request timeout in milliseconds */
    timeout?: number;
    /** Delay between retries in milliseconds */
    retryDelay?: number;
    /** Maximum number of retry attempts per sample */
    maxRetries?: number;
}

/**
 * Remove statistical outliers from an array of numbers
 * Uses the Interquartile Range (IQR) method
 */
const removeOutliers = (samples: number[]): number[] => {
    if (samples.length < 4) return [...samples]; // Need at least 4 samples for quartile calculation

    // Sort the samples
    const sorted = [...samples].sort((a, b) => a - b);

    // Calculate Q1 (25th percentile) and Q3 (75th percentile)
    const q1Index = Math.floor(sorted.length * 0.25);
    const q3Index = Math.floor(sorted.length * 0.75);
    const q1 = sorted[q1Index];
    const q3 = sorted[q3Index];

    // Calculate IQR and bounds
    const iqr = q3 - q1;
    const lowerBound = q1 - 1.5 * iqr;
    const upperBound = q3 + 1.5 * iqr;

    // Filter out samples outside the bounds
    return sorted.filter(sample => sample >= lowerBound && sample <= upperBound);
};

export const useClockSkew = (options: ClockSkewOptions = {}) => {
    const {
        endpoint = 'https://1.1.1.1/cdn-cgi/trace',
        sampleCount = 7,
        timeout = 3000,
        retryDelay = 1000,
        maxRetries = 3
    } = options;

    const detectionStartedRef = useRef(false);
    const [clockSkew, setClockSkew] = useState<number>(0);
    const [loading, setLoading] = useState<boolean>(true);
    const [error, setError] = useState<Error | null>(null);

    useEffect(() => {
        // Skip if detection was already started
        if (detectionStartedRef.current) {
            console.log('Clock skew detection already started, skipping duplicate initialization');
            return;
        }

        // Mark as started before we begin
        detectionStartedRef.current = true;
        console.log('Starting clock skew detection');

        const detectClockSkew = async () => {
            try {
                setLoading(true);

                // Collect multiple samples
                const skewSamples: number[] = [];
                let successfulSamples = 0;
                let currentRetry = 0;

                while (successfulSamples < sampleCount && currentRetry < maxRetries) {
                    try {
                        // Create a connection timer
                        const controller = new AbortController();
                        const connectionTimeoutId = setTimeout(() => {
                            controller.abort('Connection timeout exceeded');
                        }, 200);

                        // First, do a HEAD request to establish connection
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

                        // Perform the actual request
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

                        // Parse the server time from the /cdn-cgi/trace response
                        const text = await response.text();
                        const lines = text.split('\n');
                        const tsLine = lines.find(line => line.startsWith('ts='));
                        if (!tsLine) {
                            throw new Error('ts field not found in response');
                        }
                        const serverTimeStr = tsLine.substring(3); // Extract value after 'ts='
                        const serverTimeFloat = parseFloat(serverTimeStr);
                        if (isNaN(serverTimeFloat)) {
                            throw new Error('Invalid ts field format');
                        }
                        const serverTime = Math.floor(serverTimeFloat * 1000); // Convert seconds to milliseconds

                        // Calculate the adjusted local time when server responded
                        const adjustedLocalTime = localStartTime + networkLatency;

                        // Calculate skew and store it
                        const skew = adjustedLocalTime - serverTime;
                        skewSamples.push(skew);
                        successfulSamples++;

                        // Add a small delay between requests
                        if (successfulSamples < sampleCount) {
                            const randomDelay = 200 + Math.floor(Math.random() * 100);
                            await new Promise(resolve => setTimeout(resolve, randomDelay));
                        }
                    } catch (err) {
                        console.warn(`Sample ${successfulSamples + 1} failed:`, err);
                        currentRetry++;

                        if (currentRetry < maxRetries) {
                            const delay = retryDelay * Math.pow(2, currentRetry - 1);
                            await new Promise(resolve => setTimeout(resolve, delay));
                        }
                    }
                }

                if (skewSamples.length > 0) {
                    // Apply outlier detection and removal
                    const filteredSamples = removeOutliers(skewSamples);

                    if (filteredSamples.length === 0) {
                        // Fallback to median if all samples are outliers
                        skewSamples.sort((a, b) => a - b);
                        const mid = Math.floor(skewSamples.length / 2);
                        const fallbackSkew = skewSamples.length % 2 === 0
                            ? Math.round((skewSamples[mid - 1] + skewSamples[mid]) / 2)
                            : skewSamples[mid];

                        console.log(`All samples considered outliers, using median as fallback: ${fallbackSkew}ms`);
                        setClockSkew(fallbackSkew);
                    } else {
                        // Calculate mean of filtered samples
                        const sum = filteredSamples.reduce((acc, val) => acc + val, 0);
                        const meanSkew = Math.round(sum / filteredSamples.length);

                        console.log(`Clock skew detected: ${meanSkew}ms (${meanSkew > 0 ? 'ahead' : 'behind'})`);
                        console.log(`All samples (ms): ${skewSamples.join(', ')}`);
                        console.log(`After outlier removal (ms): ${filteredSamples.join(', ')}`);
                        console.log(`Removed ${skewSamples.length - filteredSamples.length} outliers`);

                        setClockSkew(meanSkew);
                    }
                } else {
                    console.warn('Failed to collect any valid clock skew samples');
                    setClockSkew(0);
                }

                setLoading(false);
            } catch (err) {
                console.error('Clock skew detection failed:', err);
                setError(err instanceof Error ? err : new Error(String(err)));
                setClockSkew(0);
                setLoading(false);
            }
        };

        detectClockSkew();
    }, [endpoint, sampleCount, timeout, retryDelay, maxRetries]);

    // Function to adjust any timestamp using the detected skew
    const adjustTime = (timestamp: number): number => {
        return timestamp - clockSkew;
    };

    // Function to convert local time to server time
    const toServerTime = (localTime: number = Date.now()): number => {
        return localTime - clockSkew;
    };

    // Function to convert server time to local time
    const toLocalTime = (serverTime: number): number => {
        return serverTime + clockSkew;
    };

    return {
        clockSkew,
        adjustTime,
        toServerTime,
        toLocalTime,
        loading,
        error
    };
};