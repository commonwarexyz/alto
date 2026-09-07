import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const generatedDirectory = new URL("../src/alto_types/", import.meta.url);
const glue = await readFile(new URL("alto_types.js", generatedDirectory), "utf8");
const moduleUrl = `data:text/javascript;base64,${Buffer.from(glue).toString("base64")}`;
const altoTypes = await import(moduleUrl);
const binary = await readFile(new URL("alto_types_bg.wasm", generatedDirectory));
await altoTypes.default({ module_or_path: binary });

const fromHex = (hex) => Uint8Array.from(
  hex.match(/../g).map((byte) => Number.parseInt(byte, 16)),
);

const proposalSize = 39;
const signatureSize = 48;

const withoutSeedSignature = (encoded) => {
  const bytes = fromHex(encoded);
  return Uint8Array.from([
    ...bytes.slice(0, proposalSize + signatureSize),
    ...bytes.slice(proposalSize + 2 * signatureSize),
  ]);
};

const identity = fromHex(
  "90577f9eefba4e54f98aa6cda386718e82863d4989b7282f09446bdb408469dc" +
  "3dec0097ef3dc9359d88b04d700290e902628bc56fe9295ad92794114a65b2ee" +
  "1985767a7d230bd02d99080f6fcd1e47e5398e5ea9198b87614491543e25a5c0",
);

// These fixed, public network artifacts catch generated WASM that no longer
// decodes and verifies the certificate wire format used by the indexer.
const artifacts = [
  [
    "seed",
    altoTypes.parse_seed,
    "00e1d5568b2e909f067906d21308d8fc080507660e1c217926c172d8cd7ea12f" +
      "19b9c12f2b89a10e639b39c9a44b093c8b982cf1",
  ],
  [
    "notarization",
    altoTypes.parse_notarized,
    "00e8d556e7d55663ad946a51587556b3aad0f5605c82e5591301e8c59ed49b7d9c" +
      "aac7654180dc8d75ac0df306528f348898c6f4c10da01544647dfd23ab20f7091240" +
      "e5313cb07f3a45c8280950635b6fa19ae4f0bad296a2da6f7bba3d58e47357e3ce" +
      "f418a841f761929cc99ca271e6ff558f2db359f3a81c246834c1efe75e0b9fa4e4c" +
      "c0e00e8d55658c38457d86e3b5ad4b63d3141d549092af777ead207cdcdf2e020fb" +
      "e22f9471e7d556e85a83d7c80a460b11675d77464e010a20fa7b99adba13429abc" +
      "b8818e433c8de85a83d7c80a460b11675d77464e010a20fa7b99adba13429abcb8" +
      "818e433c8d88b307fae8f7a3ff3300",
  ],
  [
    "finalization",
    altoTypes.parse_finalized,
    "00b3d556b2d556b285175036544e0b5792fc8d12b7a5de8db5d1fe90477529c4e11" +
      "6db2f606d0c8a41e7f66520148f37f717a0f5f984e151f5202da84814d9934027f" +
      "d5b8968feb97dd54219a19ca35433d87d3d6477c4b1813f29d9feb783fd3a686c2" +
      "81f07d571d283363293f74f30e0fee389ca3ccda4b8dc63f5c6a75fb340ba8994f" +
      "3fb0d00b3d55658c38457d86e3b5ad4b63d3141d549092af777ead207cdcdf2e020" +
      "fbe22f9471b2d556279ae1cdb53c51e3a05017b414edd9891a4df11cce61cd2daac" +
      "d3a0e0b833d1a279ae1cdb53c51e3a05017b414edd9891a4df11cce61cd2daacd3" +
      "a0e0b833d1ad3b207e6e6f7a3ff3300",
  ],
];

// Expected explorer values for the fixed artifacts above
const expected = {
  "seed": {
    "view": 1420001,
    "signature": "8b2e909f067906d21308d8fc080507660e1c217926c172d8cd7ea12f19b9c12f2b89a10e639b39c9a44b093c8b982cf1"
  },
  "notarization": {
    "view": 1420008,
    "signature": "8d75ac0df306528f348898c6f4c10da01544647dfd23ab20f7091240e5313cb07f3a45c8280950635b6fa19ae4f0bad2",
    "block": {
      "leader": "58c38457d86e3b5ad4b63d3141d549092af777ead207cdcdf2e020fbe22f9471",
      "parent": "e85a83d7c80a460b11675d77464e010a20fa7b99adba13429abcb8818e433c8d",
      "height": 121224,
      "timestamp": 1786513323130,
      "digest": "63ad946a51587556b3aad0f5605c82e5591301e8c59ed49b7d9caac7654180dc"
    }
  },
  "finalization": {
    "view": 1419955,
    "signature": "8a41e7f66520148f37f717a0f5f984e151f5202da84814d9934027fd5b8968feb97dd54219a19ca35433d87d3d6477c4",
    "block": {
      "leader": "58c38457d86e3b5ad4b63d3141d549092af777ead207cdcdf2e020fbe22f9471",
      "parent": "279ae1cdb53c51e3a05017b414edd9891a4df11cce61cd2daacd3a0e0b833d1a",
      "height": 121171,
      "timestamp": 1786513322854,
      "digest": "b285175036544e0b5792fc8d12b7a5de8db5d1fe90477529c4e116db2f606d0c"
    }
  }
};

for (const [name, parse, encoded] of artifacts) {
  const fixture = expected[name];
  const value = {
    ...fixture,
    signature: Array.from(fromHex(fixture.signature)),
  };
  if (fixture.block) {
    value.block = {
      ...fixture.block,
      leader: Array.from(fromHex(fixture.block.leader)),
      parent: Array.from(fromHex(fixture.block.parent)),
      digest: Array.from(fromHex(fixture.block.digest)),
    };
  }
  assert.deepEqual(parse(identity, fromHex(encoded)), value, name);
  if (name !== "seed") {
    // Standard certificates omit the second, 48-byte seed signature
    assert.deepEqual(
      parse(identity, withoutSeedSignature(encoded), true),
      value,
      `standard ${name}`,
    );
    assert.deepEqual(altoTypes.parse_block(fromHex(encoded).slice(proposalSize + 2 * signatureSize)), value.block);
  }
}
