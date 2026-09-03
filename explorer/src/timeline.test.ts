import { expect, test } from "@jest/globals";
import { scaleTimelineWidth } from "./timeline";

test("keeps sub-second consensus lifecycles within a one-second timeline", () => {
  expect(scaleTimelineWidth(500, 1000)).toBe(500);
  expect(scaleTimelineWidth(1000, 1000)).toBe(1000);
  expect(scaleTimelineWidth(1500, 1000)).toBe(1000);
  expect(scaleTimelineWidth(-1, 1000)).toBe(0);
});
