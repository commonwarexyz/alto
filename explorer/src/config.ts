// TODO: Replace this with the backend URL
export const BACKEND_URL = "ws://localhost:4000/consensus/ws";

// TODO: Replace this with the consensus threshold key
export const PUBLIC_KEY_HEX = "976ab7efaef8a73690b9067690ac7541bc34f74b2543e8db16b5bf63aec487758ca98efdf5c9fcf1154941d8a8a1ec3d";

// TODO: Replace this with an ordered list of validator locations (sorted by validator public key)
export const LOCATIONS: [[number, number], string][] = [
    [[37.7749, -122.4194], "San Francisco"],
    [[51.5074, -0.1278], "London"],
    [[35.6895, 139.6917], "Tokyo"],
    [[-33.8688, 151.2093], "Sydney"],
    [[55.7558, 37.6173], "Moscow"],
    [[-23.5505, -46.6333], "Sao Paulo"],
    [[28.6139, 77.2090], "New Delhi"],
    [[40.7128, -74.0060], "New York"],
    [[19.4326, -99.1332], "Mexico City"],
    [[31.2304, 121.4737], "Shanghai"],
];
