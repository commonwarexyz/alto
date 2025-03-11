# alto-explorer

## Specify consts

## Compile Alto-Types

```bash
cd ../types
wasm-pack build --release --target web
mv pkg/alto_types.js ../explorer/public
mv pkg/alto_types_bg.wasm ../explorer/public
cd ../explorer
```

## Run the app

```bash
npm start
```