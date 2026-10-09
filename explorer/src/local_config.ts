// Standalone configuration for a local indexer (`npm start`). An indexer serving the explorer
// injects its own configuration instead. PUBLIC_KEY_HEX is a placeholder encoding an empty
// participant set, which verifies no certificates: replace it with the local network's identity
// (the hex participant set passed to the indexer as --identity).
export const BACKEND_URL = "localhost:8080";
export const PUBLIC_KEY_HEX = "00";
export const LOCATIONS: [[number, number], string][] = [];
