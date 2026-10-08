import { expect, test } from "@jest/globals";
import { getHttpBackendUrl, getWebSocketBackendUrl } from "./config";

test("reaches an indexer on the page's host through the page's origin", () => {
  expect(getHttpBackendUrl(window.location.host)).toBe(window.location.origin);
  expect(getWebSocketBackendUrl(window.location.host))
    .toBe(window.location.origin.replace(/^http/, "ws"));
});

test("reaches another indexer with the page's scheme", () => {
  expect(window.location.protocol).toBe("http:");
  expect(getHttpBackendUrl("localhost:8080")).toBe("http://localhost:8080");
  expect(getWebSocketBackendUrl("localhost:8080")).toBe("ws://localhost:8080");
});
