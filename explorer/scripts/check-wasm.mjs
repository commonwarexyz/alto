import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

// Checks the generated WASM against the deterministic FN-DSA fixture written by alto-types'
// explorer_fn_dsa_fixture_is_current test: a 4-participant set and certificates signed by
// participants 0, 2, and 3.
const generatedDirectory = new URL("../src/alto_types/", import.meta.url);
const glue = await readFile(new URL("alto_types.js", generatedDirectory), "utf8");
const moduleUrl = `data:text/javascript;base64,${Buffer.from(glue).toString("base64")}`;
const altoTypes = await import(moduleUrl);
const binary = await readFile(new URL("alto_types_bg.wasm", generatedDirectory));
await altoTypes.default({ module_or_path: binary });

const fromHex = (hex) => Uint8Array.from(
  hex.match(/../g).map((byte) => Number.parseInt(byte, 16)),
);

const expectedArtifact = (fixture) => ({
  ...fixture,
  signature: Array.from(fromHex(fixture.signature)),
  block: {
    ...fixture.block,
    leader: Array.from(fromHex(fixture.block.leader)),
    parent: Array.from(fromHex(fixture.block.parent)),
    digest: Array.from(fromHex(fixture.block.digest)),
  },
});

const invalidateByte = (bytes, offset) => {
  const altered = bytes.slice();
  altered[offset] ^= 0x20;
  return altered;
};

assert.deepEqual(
  Array.from(altoTypes.sha256(new TextEncoder().encode("abc"))),
  Array.from(fromHex("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")),
  "sha256",
);

const fixture = JSON.parse(await readFile(new URL("fn_dsa_fixture.json", import.meta.url), "utf8"));
const identity = fromHex(fixture.identity);
const keySize = 897;
assert.equal(identity[0], 4, "fixture has 4 participants");

// The first 3 participants, an encoding of a different participant set.
const otherIdentity = Uint8Array.from([3, ...identity.slice(1, 1 + 3 * keySize)]);

for (const [name, parse] of [
  ["notarization", altoTypes.parse_notarized],
  ["finalization", altoTypes.parse_finalized],
]) {
  const { bytes: encoded, ...expectedFields } = fixture[name];
  const bytes = fromHex(encoded);
  const value = expectedArtifact(expectedFields);
  assert.deepEqual(parse(identity, bytes), value, name);
  assert.equal(
    parse(identity, invalidateByte(bytes, fixture.signatureOffset + 100)),
    null,
    `rejects an incorrect ${name} signature`,
  );
  assert.equal(parse(otherIdentity, bytes), null, `rejects a ${name} for another participant set`);
  assert.deepEqual(altoTypes.parse_block(bytes.slice(fixture.blockOffset)), value.block, `${name} block`);
}
