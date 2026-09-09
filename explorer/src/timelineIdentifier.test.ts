import { expect, test } from "@jest/globals";
import {
  BLOCK_HASH_COLOR,
  getLeaderIndicator,
  getTimelineIdentifier,
  SEED_SIGNATURE_COLOR,
} from "./timelineIdentifier";

const seedSignature = new Uint8Array([0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe]);
const blockDigest = new Uint8Array([1, 2, 3, 4, 5, 6]);

test("labels election consistently using each mode's identifier color", () => {
  expect(getLeaderIndicator(true)).toEqual({
    label: "Elected",
    color: BLOCK_HASH_COLOR,
  });
  expect(getLeaderIndicator(false)).toEqual({
    label: "Elected",
    color: SEED_SIGNATURE_COLOR,
  });
});

test("shows the block hash for standard round-robin certificates", () => {
  expect(
    getTimelineIdentifier(true, seedSignature, blockDigest),
  ).toEqual({
    label: "Proposal",
    color: BLOCK_HASH_COLOR,
    value: "03040506",
    fullValue: "010203040506",
  });
});

test("shows the seed signature for VRF certificates", () => {
  expect(
    getTimelineIdentifier(false, seedSignature, blockDigest),
  ).toEqual({
    label: "Seed",
    color: SEED_SIGNATURE_COLOR,
    value: "beefcafe",
    fullValue: "deadbeefcafe",
  });
});
