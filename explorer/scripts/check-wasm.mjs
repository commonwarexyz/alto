import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

// The Rust fixtures cover noncontiguous quorums of 3/4 and 34/50 participants.
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

for (const [file, participantCount, signerCount] of [
  ["ellipsoidal_falcon_fixture.json", 4, 3],
  ["ellipsoidal_falcon_50_fixture.json", 50, 34],
]) {
  const fixture = JSON.parse(await readFile(new URL(file, import.meta.url), "utf8"));
  const identity = fromHex(fixture.identity);
  assert.equal(fixture.profile, "ellipsoidal-falcon-512");
  assert.equal(fixture.participantCount, participantCount);
  assert.equal(fixture.signerCount, signerCount);
  assert.equal(fixture.publicKeySize, 897);
  assert.equal(fixture.signatureSize, 512);
  assert.ok(fixture.participantCount < 128, "fixture count fits a one-byte varint");
  assert.equal(identity[0], fixture.participantCount);
  assert.equal(identity.length, 1 + fixture.participantCount * fixture.publicKeySize);
  assert.equal(
    fixture.signatureOffset + fixture.signerCount * fixture.signatureSize,
    fixture.blockOffset,
  );

  const otherCount = fixture.participantCount - 1;
  const otherIdentity = Uint8Array.from([
    otherCount,
    ...identity.slice(1, 1 + otherCount * fixture.publicKeySize),
  ]);

  for (const [name, parse] of [
    ["notarization", altoTypes.parse_notarized],
    ["finalization", altoTypes.parse_finalized],
  ]) {
    const { bytes: encoded, ...expectedFields } = fixture[name];
    const bytes = fromHex(encoded);
    const value = expectedArtifact(expectedFields);
    assert.deepEqual(parse(identity, bytes), value, `${file}: ${name}`);
    for (let signer = 0; signer < fixture.signerCount; signer += 1) {
      // Changing a salt byte preserves the encoding and requires signature verification to reject it.
      const offset = fixture.signatureOffset + signer * fixture.signatureSize + 1;
      assert.ok(offset < fixture.blockOffset);
      assert.equal(
        parse(identity, invalidateByte(bytes, offset)),
        null,
        `${file}: rejects an incorrect ${name} signature at signer ${signer}`,
      );
    }
    assert.equal(parse(otherIdentity, bytes), null, `rejects a ${name} for another participant set`);
    assert.equal(parse(identity, bytes.slice(0, -1)), null, `rejects a truncated ${name}`);
    assert.equal(parse(identity, Uint8Array.from([...bytes, 0])), null, `rejects trailing ${name} data`);
    assert.deepEqual(altoTypes.parse_block(bytes.slice(fixture.blockOffset)), value.block, `${name} block`);
  }
}
