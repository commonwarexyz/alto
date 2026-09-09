import { afterEach, beforeEach, expect, jest, test } from '@jest/globals';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';
import StatsSection from './StatsSection';
import { ClusterConfig } from './config';
import { ViewData } from './types';

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const config: ClusterConfig = {
    BACKEND_URL: 'localhost',
    PUBLIC_KEY_HEX: '00',
    CERTIFICATE_MODE: 'standard',
    LOCATIONS: [],
    name: 'Test',
    description: '',
};
const originalMatchMedia = window.matchMedia;
let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
    window.matchMedia = jest.fn<ReturnType<typeof window.matchMedia>, Parameters<typeof window.matchMedia>>().mockReturnValue({
        matches: false,
    } as MediaQueryList);
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
});

afterEach(async () => {
    await act(async () => root.unmount());
    container.remove();
    window.matchMedia = originalMatchMedia;
});

test.each([
    { name: 'no blocks', timestamps: [], expected: 'N/A' },
    { name: 'too few adjacent blocks', timestamps: [1000, 1000], expected: 'N/A' },
    { name: 'equal timestamps', timestamps: [1000, 1000, 1000], expected: '0ms' },
    { name: 'equal zero timestamps', timestamps: [0, 0, 0], expected: '0ms' },
    { name: 'mixed zero and positive deltas', timestamps: [1000, 1000, 1010], expected: '5ms' },
])('Block Time displays $expected for $name', async ({ timestamps, expected }: {
    timestamps: number[];
    expected: string;
}) => {
    const views: ViewData[] = timestamps.map((timestamp, index) => ({
        view: index + 1,
        status: 'finalized',
        startTime: timestamp,
        block: {
            leader: [],
            height: index + 1,
            timestamp,
            digest: [],
            parent: [],
        },
    }));

    await act(async () => {
        root.render(<StatsSection
            views={views}
            selectedCluster="local"
            onClusterChange={() => {}}
            configs={{ global: config, usa: config, local: config }}
        />);
    });

    expect(container.querySelector('.validator-metrics .stat-value')?.textContent).toBe(expected);
});
