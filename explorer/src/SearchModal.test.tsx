import { afterEach, beforeEach, expect, jest, test } from '@jest/globals';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';
import SearchModal from './SearchModal';
import { ClusterConfig } from './config';
import { BlockJs, SearchType } from './types';
import { parse_block, parse_finalized, parse_notarized } from './alto_types/alto_types.js';
import { hexToUint8Array } from './utils';

jest.mock('./alto_types/alto_types.js', () => {
    const { jest } = require('@jest/globals');
    return {
        __esModule: true,
        default: async () => {},
        parse_block: jest.fn(),
        parse_finalized: jest.fn().mockReturnValue(null),
        parse_notarized: jest.fn().mockReturnValue(null),
    };
});

Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

const digest = 'ab'.repeat(32);
const block: BlockJs = {
    leader: [],
    parent: [],
    height: 10,
    timestamp: 0,
    digest: Array.from(hexToUint8Array(digest)),
};
const originalFetch = globalThis.fetch;
let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    globalThis.fetch = jest.fn<ReturnType<typeof fetch>, Parameters<typeof fetch>>().mockResolvedValue({
        ok: true,
        arrayBuffer: async () => new ArrayBuffer(1),
    } as Response);
    jest.mocked(parse_block).mockReturnValue(block);
    jest.mocked(parse_finalized).mockReturnValue(null);
    jest.mocked(parse_notarized).mockReturnValue(null);
});

afterEach(async () => {
    await act(async () => root.unmount());
    container.remove();
    globalThis.fetch = originalFetch;
    jest.clearAllMocks();
    jest.restoreAllMocks();
});

async function search(query: string, type: SearchType = 'block') {
    const config: ClusterConfig = {
        BACKEND_URL: window.location.host,
        PUBLIC_KEY_HEX: '00',
        LOCATIONS: [],
        name: 'Test',
        description: '',
    };
    await act(async () => {
        root.render(<SearchModal isOpen onClose={() => {}} clusterConfig={config} />);
    });
    await act(async () => {
        const select = container.querySelector('select')!;
        select.value = type;
        select.dispatchEvent(new Event('change', { bubbles: true }));
        const input = container.querySelector('input')!;
        Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, query);
        input.dispatchEvent(new Event('input', { bubbles: true }));
    });
    await act(async () => {
        container.querySelector('form')!.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
    });
}

test('rejects a different block for a digest query', async () => {
    await search('cd' + digest.slice(2));

    expect(container.querySelector('.search-result-item')).toBeNull();
    expect(container.querySelector('.search-error')?.textContent).toContain('Block digest does not match query');
});

test.each([digest, digest.toUpperCase(), `0x${digest}`, `0x${digest.toUpperCase()}`])(
    'accepts the matching block for digest query %s',
    async query => {
        await search(query);

        expect(container.querySelector('.search-error')).toBeNull();
        expect(container.querySelector('.search-result-header')?.textContent).toContain('Block');
        expect(container.querySelectorAll('.search-result-item')).toHaveLength(1);
    },
);

const indexedSearches: SearchType[] = ['notarization', 'finalization', 'block'];

test.each(indexedSearches)('does not display a %s rejected by verification', async type => {
    await search('latest', type);

    expect(container.querySelector('.search-result-item')).toBeNull();
    expect(container.querySelector('.search-error')?.textContent).toContain(`Failed to parse ${type} data`);
    const publicKey = new Uint8Array([0]);
    const payload = new Uint8Array([0]);
    const parse = type === 'notarization' ? parse_notarized : parse_finalized;
    expect(parse).toHaveBeenCalledWith(publicKey, payload);
});

test('offers only certified artifact searches', async () => {
    await search('latest', 'finalization');

    const options = Array.from(container.querySelectorAll('option'), option => option.value);
    expect(options).toEqual(['notarization', 'finalization', 'block']);
});

test('labels the certified block by its certificate digest', async () => {
    returnArtifact('finalization');
    await search('latest', 'finalization');

    const keys = Array.from(container.querySelectorAll('.search-result-key'), key => key.textContent);
    expect(keys).toContain('certificate:');
    expect(keys).not.toContain('signature:');
});

function returnArtifact(type: SearchType) {
    const artifact = { view: 42, signature: [], block };
    const parse = type === 'notarization' ? parse_notarized : parse_finalized;
    jest.mocked(parse).mockReturnValue(artifact);
    return type === 'block' ? block.height : artifact.view;
}

test.each(indexedSearches)('rejects a %s for a different index', async type => {
    const index = returnArtifact(type);
    await search(String(index + 1), type);

    expect(container.querySelector('.search-result-item')).toBeNull();
    expect(container.querySelector('.search-error')?.textContent).toContain('Response does not match query');
});

test.each(indexedSearches)('accepts a %s for the requested index', async type => {
    const index = returnArtifact(type);
    await search(String(index), type);

    expect(container.querySelector('.search-error')).toBeNull();
    expect(container.querySelectorAll('.search-result-item')).toHaveLength(1);
});

test.each(indexedSearches)('accepts the latest %s', async type => {
    returnArtifact(type);
    await search('latest', type);

    expect(container.querySelector('.search-error')).toBeNull();
    expect(container.querySelectorAll('.search-result-item')).toHaveLength(1);
});

test.each([
    '', '12junk', '1.5', '-1', '1e3', 'Infinity',
    '-1..1', '1..-1', '1..2junk', '1.5..2', '1..2..3', '..2', '1..', '2..1',
    '9007199254740992', '9007199254740992..9007199254740992',
    '0..9007199254740992', '9007199254740991..9007199254740992',
])('rejects invalid numeric query %s without fetching', async query => {
    // A pending response prevents an invalid range from issuing repeated requests.
    jest.mocked(globalThis.fetch).mockImplementation(() => new Promise<Response>(() => {}));
    await search(query);

    expect(globalThis.fetch).not.toHaveBeenCalled();
    expect(container.querySelector('.search-error')?.textContent).toContain('Invalid query');
});

test.each([
    { start: 0, end: 100, count: 20 },
    { start: Number.MAX_SAFE_INTEGER - 1, end: Number.MAX_SAFE_INTEGER, count: 2 },
])('fetches only the bounded inclusive range $start..$end', async ({ start, end, count }) => {
    let height = start;
    jest.mocked(parse_finalized).mockImplementation(() => ({
        view: 42,
        signature: [],
        block: { ...block, height: height++ },
    }));
    await search(`${start}..${end}`);

    expect(container.querySelector('.search-error')).toBeNull();
    expect(container.querySelectorAll('.search-result-item')).toHaveLength(count);
    expect(jest.mocked(globalThis.fetch).mock.calls.map(([url]) => url)).toEqual(
        Array.from({ length: count }, (_, offset) =>
            `${window.location.origin}/block/${BigInt(start + offset).toString(16).padStart(16, '0')}`),
    );
});

test('does not display a repeated block for other heights in a range', async () => {
    returnArtifact('block');
    jest.spyOn(console, 'error').mockImplementation(() => {});
    await search('11..12');

    expect(globalThis.fetch).toHaveBeenCalledTimes(2);
    expect(container.querySelector('.search-result-item')).toBeNull();
    expect(container.querySelector('.search-error')?.textContent).toContain('No results found for range 11..12');
});
