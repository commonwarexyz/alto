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

test("preserves participant indices across missing locations", () => {
  const participants = ["01", "02", "03"];
  const partialLocations: ([[number, number], string] | null)[] = [
    locations[0], null, locations[1],
  ];
  expect(resolveLeaderLocation([0x02], participants, partialLocations)).toBeUndefined();
  expect(resolveLeaderLocation([0x03], participants, partialLocations))
    .toEqual({ location: [3, 4], locationName: "Second" });
  expect(resolveLeaderLocation([0x01], participants, partialLocations))
    .toEqual({ location: [1, 2], locationName: "First" });
});
