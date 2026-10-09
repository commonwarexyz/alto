import * as localConfig from './local_config';

export interface ClusterConfig {
    /** Indexer host and optional port, without a scheme. */
    BACKEND_URL: string;
    /** Hex-encoded participant set that verifies every certificate. */
    PUBLIC_KEY_HEX: string;
    // Validator locations and keys follow consensus public-key order.
    // Null locations keep unmapped validators in the committee.
    LOCATIONS: ([[number, number], string] | null)[];
    PARTICIPANTS?: string[];
    name: string;
    description: string;
}

declare global {
    interface Window {
        // Null selects standalone hosting after its configuration script loads.
        ALTO_DEPLOYMENT?: ClusterConfig | null;
    }
}

// An indexer serving the explorer supplies its network at /runtime-config.js. Standalone hosting
// (including `npm start`) uses the local configuration.
const clusterConfig: ClusterConfig = window.ALTO_DEPLOYMENT ?? {
    ...localConfig,
    name: 'Local Cluster',
    description: 'A local test cluster running on localhost.',
};

export const getClusterConfig = (): ClusterConfig => clusterConfig;

// An indexer on the page's host serves its API from the page's origin. Any other indexer is
// reached with the page's scheme, so a plain-http development server reaches a local indexer
// over http and an https page never requests mixed content.
export const getHttpBackendUrl = (backendUrl: string): string => {
    if (backendUrl === window.location.host) {
        return window.location.origin;
    }
    return `${window.location.protocol === 'https:' ? 'https' : 'http'}://${backendUrl}`;
};

export const getWebSocketBackendUrl = (backendUrl: string): string =>
    getHttpBackendUrl(backendUrl).replace(/^http/, 'ws');
