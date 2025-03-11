export interface SeedJs {
    view: number;
    signature: Uint8Array;
}

export interface ProofJs {
    view: number;
    parent: number;
    payload: Uint8Array;
    signature: Uint8Array;
}

export interface BlockJs {
    parent: Uint8Array;
    height: number;
    timestamp: number;
    digest: Uint8Array;
}

export interface NotarizedJs {
    proof: ProofJs;
    block: BlockJs;
}

export interface FinalizedJs {
    proof: ProofJs;
    block: BlockJs;
}

// WASM module interface
interface AltoTypes {
    parse_seed: (publicKey: Uint8Array | null, bytes: Uint8Array) => SeedJs | null;
    parse_notarized: (publicKey: Uint8Array | null, bytes: Uint8Array) => NotarizedJs | null;
    parse_finalized: (publicKey: Uint8Array | null, bytes: Uint8Array) => FinalizedJs | null;
}

export declare const init: () => Promise<void>;
export declare const parse_seed: AltoTypes["parse_seed"];
export declare const parse_notarized: AltoTypes["parse_notarized"];
export declare const parse_finalized: AltoTypes["parse_finalized"];