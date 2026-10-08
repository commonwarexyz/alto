import { expect, test } from "@jest/globals";
import {
  BLOCK_HASH_COLOR,
  getTimelineIdentifier,
  LEADER_INDICATOR,
} from "./timelineIdentifier";

test("labels election with the proposal identifier color", () => {
  expect(LEADER_INDICATOR).toEqual({
    label: "Elected",
    color: BLOCK_HASH_COLOR,
  });
});

test("shows the block hash of the proposal", () => {
  expect(getTimelineIdentifier(new Uint8Array([1, 2, 3, 4, 5, 6]))).toEqual({
    label: "Proposal",
    color: BLOCK_HASH_COLOR,
    value: "03040506",
    fullValue: "010203040506",
  });
  expect(getTimelineIdentifier()).toEqual({
    label: "Proposal",
    color: BLOCK_HASH_COLOR,
    value: "",
    fullValue: "",
  });
});
