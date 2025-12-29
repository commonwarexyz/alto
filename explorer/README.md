# alto-explorer

Visualize `alto` activity.

## Status

`alto-explorer` is **ALPHA** software and is not yet recommended for production use. Developers should expect breaking changes and occasional instability.

## Modes

The alto explorer can run in two modes: **public** (for deployed clusters) and **local** (for local development).

### Public Mode (Default)

Public mode is used for deployed clusters (e.g., Global and USA clusters on AWS). It shows:
- A world map with validator locations
- A cluster dropdown to switch between clusters
- Full documentation about the deployed infrastructure

Populate `src/global_config.ts` and `src/usa_config.ts` with the cluster configurations:

```typescript
// Backend URL (without protocol - https:// is used automatically)
export const BACKEND_URL = "global.alto.example.com";

// Consensus threshold key (hex-encoded)
export const PUBLIC_KEY_HEX = "92b050b6...";

// Ordered list of validator locations (sorted by validator public key)
export const LOCATIONS: [[number, number], string][] = [
    [[37.7749, -122.4194], "San Francisco"],
    [[51.5074, -0.1278], "London"],
    // ...
];
```

You can generate these configurations using `setup explorer remote`:
```bash
cargo run --bin setup -- explorer --dir <config-dir> --backend-url <url> remote
```

To run in public mode:
```bash
npm start
# or explicitly:
REACT_APP_MODE=public npm start
```

### Local Mode

Local mode is used for local development with a local indexer. It shows:
- No map (since all validators are on localhost)
- No cluster dropdown
- Simplified documentation for local usage

Populate `src/local_config.ts`:

```typescript
// Backend URL (http:// is used automatically in local mode)
export const BACKEND_URL = "localhost:8080";

// Consensus threshold key (hex-encoded)
export const PUBLIC_KEY_HEX = "82f8a77b...";

// Empty locations array (map will be hidden)
export const LOCATIONS: [[number, number], string][] = [];
```

You can generate this configuration using `setup explorer local`:
```bash
cargo run --bin setup -- explorer --dir <config-dir> --backend-url <url> local
```

Then copy the generated `config.ts` to `src/local_config.ts`.

To run in local mode:
```bash
REACT_APP_MODE=local npm start
```

## Development

### Build the app

```bash
# Public mode (default)
npm run build

# Local mode
REACT_APP_MODE=local npm run build
```

_This will compile the WASM module from `alto-types` before building the React app._

### Run the production build

_Install `serve` if necessary: `npm install -g serve`._

```bash
serve -s build
```
