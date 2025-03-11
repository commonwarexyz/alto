# alto-explorer

## Specify consts

## Compile Alto-Types

```bash
cd ../types
wasm-pack build --release --target web
mv pkg/alto_types.js ../explorer/src/alto_types
mv pkg/alto_types_bg.wasm ../explorer/src/alto_types
cd ../explorer
```

## Run the app

```bash
npm start
```