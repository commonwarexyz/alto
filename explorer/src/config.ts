export const INDEXER_URL = "http://localhost:4000";
export const WS_URL = "ws://localhost:4000/consensus/ws";
export const PUBLIC_KEY_HEX = "976ab7efaef8a73690b9067690ac7541bc34f74b2543e8db16b5bf63aec487758ca98efdf5c9fcf1154941d8a8a1ec3d";

/**
 * Converts a hexadecimal string to a Uint8Array.
 * @param hex - The hexadecimal string to convert.
 * @returns A Uint8Array representation of the hex string.
 * @throws Error if the hex string has an odd length or contains invalid characters.
 */
function hexToUint8Array(hex: string): Uint8Array {
    if (hex.length % 2 !== 0) {
        throw new Error("Hex string must have an even length");
    }
    const bytes: number[] = [];
    for (let i = 0; i < hex.length; i += 2) {
        const byteStr = hex.substr(i, 2);
        const byte = parseInt(byteStr, 16);
        if (isNaN(byte)) {
            throw new Error(`Invalid hex character in string: ${byteStr}`);
        }
        bytes.push(byte);
    }
    return new Uint8Array(bytes);
}

// Export PUBLIC_KEY as a Uint8Array for use in the application
export const PUBLIC_KEY = hexToUint8Array(PUBLIC_KEY_HEX);