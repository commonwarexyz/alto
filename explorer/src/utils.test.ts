import { expect, test } from "@jest/globals";
import { decodeVarintPrefix } from "./utils";

test.each([
  { bytes: [0x00], value: 0 },
  { bytes: [0x04, 0xaa], value: 4 },
  { bytes: [0x32], value: 50 },
  { bytes: [0x7f], value: 127 },
  { bytes: [0x80, 0x01], value: 128 },
  { bytes: [0xc8, 0x01, 0xff], value: 200 },
  { bytes: [0xff, 0xff, 0xff, 0xff, 0x0f], value: 0xffffffff },
])("decodes the varint prefix of $bytes as $value", ({ bytes, value }) => {
  expect(decodeVarintPrefix(new Uint8Array(bytes))).toBe(value);
});

test.each([
  { name: "empty", bytes: [] },
  { name: "truncated", bytes: [0xc8] },
  { name: "over 32 bits", bytes: [0xff, 0xff, 0xff, 0xff, 0x1f] },
  { name: "over five bytes", bytes: [0x80, 0x80, 0x80, 0x80, 0x80, 0x01] },
])("rejects a $name varint prefix", ({ bytes }) => {
  expect(decodeVarintPrefix(new Uint8Array(bytes))).toBeNull();
});
