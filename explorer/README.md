# alto-explorer

## Populate Configurations (Global and USA)

```typescript
// TODO: Replace this with the backend URL
export const BACKEND_URL = "localhost:4000";

// TODO: Replace this with the consensus threshold key
export const PUBLIC_KEY_HEX = "92b050b6fbe80695b5d56835e978918e37c8707a7fad09a01ae782d4c3170c9baa4c2c196b36eac6b78ceb210b287aeb0727ef1c60e48042142f7bcc8b6382305cd50c5a4542c44ec72a4de6640c194f8ef36bea1dbed168ab6fd8681d910d55";

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
```

## Run the app

```bash
npm start
```

## Build the app

```bash
npm run build
```

_This will compile the WASM module from `alto-types` before building the React app._

## Run the production app

_Install `serve` if necessary: `npm install -g serve`._

```bash
serve -s build
```