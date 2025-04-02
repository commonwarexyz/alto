import { MS_PER_SECOND, MS_PER_MINUTE, MS_PER_HOUR, MS_PER_DAY } from './types';

/**
 * Converts a hexadecimal string to a Uint8Array.
 * @param hex - The hexadecimal string to convert.
 * @returns A Uint8Array representation of the hex string.
 * @throws Error if the hex string has an odd length or contains invalid characters.
 */
export function hexToUint8Array(hex: string): Uint8Array {
    // Remove '0x' prefix if present
    if (hex.startsWith('0x')) {
        hex = hex.slice(2);
    }

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

/**
 * Converts a Uint8Array to a hex string (keeping up to len).
 * @param arr - The Uint8Array to convert
 * @param len - Max number of characters to keep (default: 8)
 * @returns A representation of the Uint8Array as a hex string.
 */
export function hexUint8Array(arr: Uint8Array | undefined, len: number = 8): string {
    if (!arr || arr.length === 0) return "";

    // Convert the entire array to hex
    const fullHex = Array.from(arr, (b) => b.toString(16).padStart(2, "0")).join("");

    // Get last characters of the hex string
    return fullHex.slice(-len);
}

/**
 * Format a timestamp age in milliseconds into a human-readable string
 * @param age - Age in milliseconds
 * @returns A formatted string representing the age
 */
export function formatAge(age: number): string {
    if (age < MS_PER_SECOND) {
        return `${age}ms`;
    } else if (age < MS_PER_MINUTE) {
        const seconds = age / MS_PER_SECOND;
        return `${seconds.toFixed(1)}s`;
    } else if (age < MS_PER_HOUR) {
        const minutes = age / MS_PER_MINUTE;
        return `${minutes.toFixed(1)}m`;
    } else if (age < MS_PER_DAY) {
        const hours = age / MS_PER_HOUR;
        return `${hours.toFixed(1)}h`;
    } else {
        const days = Math.floor(age / MS_PER_DAY);
        const remainingMs = age % MS_PER_DAY;
        const hours = Math.floor(remainingMs / MS_PER_HOUR);
        return `${days}d ${hours}h`;
    }
}