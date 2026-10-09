// View statuses
export type ViewStatus = "notarized" | "finalized" | "timed_out" | "unknown";

// Search types
export type SearchType = 'block' | 'notarization' | 'finalization';

// Block data
export interface BlockJs {
    leader: number[];
    height: number;
    /** Milliseconds since the Unix epoch. */
    timestamp: number;
    digest: number[];
    parent: number[];
}

// Verified notarized or finalized block. The signature is the SHA-256 digest of the encoded
// certificate.
export interface CertifiedBlockJs {
    view: number;
    signature: number[];
    block: BlockJs;
}

// View data for timeline display
export interface ViewData {
    view: number;
    location?: [number, number];
    locationName?: string;
    status: ViewStatus;
    startTime: number;
    notarizationTime?: number;
    finalizationTime?: number;
    block?: BlockJs;
    timeoutId?: NodeJS.Timeout;
    actualNotarizationLatency?: number;
    actualFinalizationLatency?: number;
}

// Type for search results
export type SearchResult = CertifiedBlockJs | BlockJs;

// Time constants
export const MS_PER_SECOND = 1000;
export const MS_PER_MINUTE = 60000;
export const MS_PER_HOUR = 3600000;
export const MS_PER_DAY = 86400000;
