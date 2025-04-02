// View statuses
export type ViewStatus = "growing" | "notarized" | "finalized" | "timed_out" | "unknown";

// Search types
export type SearchType = 'block' | 'notarization' | 'finalization' | 'seed';

// Block data
export interface BlockJs {
    height: number;
    timestamp: number;
    digest: Uint8Array;
    parent: Uint8Array;
}

// Seed (for leader election)
export interface SeedJs {
    view: number;
    signature: Uint8Array;
}

// Proof for notarizations and finalizations
export interface ProofJs {
    view: number;
    threshold?: number;
    signature?: Uint8Array;
}

// Notarized block
export interface NotarizedJs {
    proof: ProofJs;
    block: BlockJs;
}

// Finalized block
export interface FinalizedJs {
    proof: ProofJs;
    quorum: boolean;
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
    signature?: Uint8Array;
    block?: BlockJs;
    timeoutId?: NodeJS.Timeout;
    actualNotarizationLatency?: number;
    actualFinalizationLatency?: number;
}

// Type for search results
export type SearchResult = SeedJs | NotarizedJs | FinalizedJs | BlockJs | null;

// Time constants
export const MS_PER_SECOND = 1000;
export const MS_PER_MINUTE = 60000;
export const MS_PER_HOUR = 3600000;
export const MS_PER_DAY = 86400000;