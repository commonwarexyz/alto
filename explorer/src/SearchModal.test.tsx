import { afterEach, beforeEach, expect, jest, test } from '@jest/globals';
import { act } from 'react';
import { createRoot, Root } from 'react-dom/client';
import SearchModal from './SearchModal';
import { CertificateMode, ClusterConfig } from './config';
import { BlockJs, SearchType } from './types';
import { parse_block, parse_finalized, parse_notarized, parse_seed } from './alto_types/alto_types.js';
import { hexToUint8Array } from './utils';

jest.mock('./alto_types/alto_types.js', () => {
    const { jest } = require('@jest/globals');
    return {
        __esModule: true,
        default: async () => {},
        parse_block: jest.fn(),
        parse_finalized: jest.fn().mockReturnValue(null),
        parse_notarized: jest.fn().mockReturnValue(null),
        parse_seed: jest.fn().mockReturnValue(null),
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
});

afterEach(async () => {
    await act(async () => root.unmount());
    container.remove();
    globalThis.fetch = originalFetch;
    jest.clearAllMocks();
});

async function search(query: string, mode: CertificateMode, type: SearchType = 'block') {
    const config: ClusterConfig = {
        BACKEND_URL: window.location.host,
        PUBLIC_KEY_HEX: '00',
        CERTIFICATE_MODE: mode,
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

test.each(['standard', 'vrf'] as const)('rejects a different block for a digest query in %s mode', async mode => {
    await search('cd' + digest.slice(2), mode);

    expect(container.querySelector('.search-result-item')).toBeNull();
    expect(container.querySelector('.search-error')?.textContent).toContain('Block digest does not match query');
});

test.each([digest, digest.toUpperCase(), `0x${digest}`, `0x${digest.toUpperCase()}`])(
    'accepts the matching block for digest query %s',
    async query => {
        await search(query, 'standard');

        expect(container.querySelector('.search-error')).toBeNull();
        expect(container.querySelector('.search-result-header')?.textContent).toContain('Block');
        expect(container.querySelectorAll('.search-result-item')).toHaveLength(1);
    },
);

const rejectedSearches: [SearchType, CertificateMode][] = [
    ['notarization', 'standard'],
    ['finalization', 'standard'],
    ['block', 'standard'],
    ['seed', 'vrf'],
    ['notarization', 'vrf'],
    ['finalization', 'vrf'],
    ['block', 'vrf'],
];

test.each(rejectedSearches)('does not display a %s rejected by verification in %s mode', async (type, mode) => {
    await search('latest', mode, type);

    expect(container.querySelector('.search-result-item')).toBeNull();
    expect(container.querySelector('.search-error')?.textContent).toContain(`Failed to parse ${type} data`);
    const publicKey = new Uint8Array([0]);
    const payload = new Uint8Array([0]);
    if (type === 'seed') {
        expect(parse_seed).toHaveBeenCalledWith(publicKey, payload);
    } else {
        const parse = type === 'notarization' ? parse_notarized : parse_finalized;
        expect(parse).toHaveBeenCalledWith(publicKey, payload, mode === 'standard');
    }
});
