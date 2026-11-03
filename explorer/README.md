# alto-explorer

Visualize `alto` activity.

## Status

`alto-explorer` is **ALPHA** software and is not yet recommended for production use. Developers should expect breaking changes and occasional instability.

## Modes

The alto explorer can run in two modes: **public** (for deployed clusters) and **local** (for local development).

Run the `npm` commands in this guide from the `explorer/` directory after installing dependencies with `npm ci`.

When the indexer serves the explorer, `/runtime-config.js` supplies the mode, network key, leader mode, and validator keys and locations for that deployment. Requests use the same origin as the explorer page. This runtime configuration takes precedence over the static files and `REACT_APP_MODE`.

The leader mode selects the certificate format the explorer verifies: VRF certificates for `rotating-vrf`, and standard certificates for `stable` and `rotating-round-robin`.

For a standalone explorer, the bundled `runtime-config.js` selects the static configurations below. `REACT_APP_MODE` selects the mode and defaults to `public`. If the configuration script fails to load, startup stops with a refresh prompt.

### Public Mode (Default)

Public mode is used for deployed clusters (e.g., Global and USA clusters on AWS). It shows:
- A world map with validator locations
- A cluster dropdown listing Global and USA in the bundled standalone configuration, or only the serving deployment when an indexer serves the explorer
- Full documentation about the deployed infrastructure

The bundled Global and USA configurations use [exoware::relay](https://exoware.xyz), which streams consensus artifacts from those clusters to the browser.

For a standalone public explorer, populate `src/global_config.ts` and `src/usa_config.ts`:

```typescript
// Backend URL (without protocol - https:// is used automatically)
export const BACKEND_URL = "global.alto.example.com";

// Consensus threshold key (hex-encoded)
export const PUBLIC_KEY_HEX = "92b050b6...";

// Leader selection: stable, rotating-vrf, or rotating-round-robin
export const LEADER_MODE = "stable" as const;

// Validator public keys (hex, sorted) used to place certified block leaders on the map.
// Required for map placement in stable and rotating-round-robin networks.
export const PARTICIPANTS: string[] = [
    "0ba766c9...",
    "34bc98b6...",
    // ...
];

// Locations follow PARTICIPANTS order. Use null for an unmapped validator.
export const LOCATIONS: ([[number, number], string] | null)[] = [
    [[37.7749, -122.4194], "San Francisco"],
    [[51.5074, -0.1278], "London"],
    // ...
];
```

Generate each configuration from that deployment's output directory with `deploy explorer remote`, then copy the generated `config.ts` exports into the matching file:
```bash
cargo run --bin deploy -- explorer --dir <config-dir> --backend-url <url> remote
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

For a standalone local explorer, populate `src/local_config.ts`:

```typescript
// Backend URL (http:// is used automatically in local mode)
export const BACKEND_URL = "localhost:8080";

// Consensus threshold key (hex-encoded)
export const PUBLIC_KEY_HEX = "82f8a77b...";

// Leader selection: stable, rotating-vrf, or rotating-round-robin
export const LEADER_MODE = "stable" as const;

// Empty locations array (map will be hidden)
export const LOCATIONS: [[number, number], string][] = [];
```

You can generate this configuration using `deploy explorer local`:
```bash
cargo run --bin deploy -- explorer --dir <config-dir> --backend-url <url> local
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

_`npm run build` first compiles the WASM module from `alto-types` with `wasm-pack` and checks it, then builds the React app. On macOS, see [Build the Embedded Explorer](../deploy/README.md#build-the-embedded-explorer) for the required LLVM toolchain._

### Run the production build

_Install `serve` if necessary: `npm install -g serve`._

```bash
serve -s build
```
