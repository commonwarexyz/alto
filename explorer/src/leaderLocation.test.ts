import { expect, test } from "@jest/globals";
import { resolveLeaderLocation } from "./leaderLocation";

const locations: [[number, number], string][] = [
  [[1, 2], "First"],
  [[3, 4], "Second"],
];

test("maps the certified block leader to its deployed location", () => {
  expect(
    resolveLeaderLocation(
      new Uint8Array([0xff, 0xab]),
      ["01ab", "ffab"],
      locations,
    ),
  ).toEqual({ location: [3, 4], locationName: "Second" });
});

test("does not infer a location when the certified leader is unknown", () => {
  expect(
    resolveLeaderLocation(
      new Uint8Array([0x00, 0xab]),
      ["01ab", "ffab"],
      locations,
    ),
  ).toBeUndefined();
});
