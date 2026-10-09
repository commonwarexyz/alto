import { afterAll, afterEach, beforeAll, expect, jest, test } from "@jest/globals";
import { parse_finalized, parse_notarized } from "./alto_types/alto_types.js";

jest.mock("./alto_types/alto_types.js", () => {
  const { jest } = require("@jest/globals");
  return {
    __esModule: true,
    default: async () => {},
    parse_notarized: jest.fn(),
    parse_finalized: jest.fn(),
  };
});

const worker = globalThis as unknown as DedicatedWorkerGlobalScope;
const originalOnMessage = worker.onmessage;
const parsers = [parse_notarized, parse_finalized];

beforeAll(() => {
  require("./consensusWorker");
});

afterEach(() => {
  jest.resetAllMocks();
  jest.restoreAllMocks();
});

afterAll(() => {
  worker.onmessage = originalOnMessage;
});

test.each([
  { name: "notarization", kind: 1 },
  { name: "finalization", kind: 2 },
])("replies to a rejected $name and delivers the next valid artifact", async ({ kind }) => {
  const publicKey = new Uint8Array([1, 2, 3]);
  const rejectedPayload = new Uint8Array([4]);
  const validPayload = new Uint8Array([5]);
  const artifact = {
    view: 7,
    signature: [8],
    block: { leader: [9], height: 6, timestamp: 10, digest: [11], parent: [12] },
  };
  const parser = jest.mocked(parsers[kind - 1]);
  parser.mockReturnValueOnce(null).mockReturnValueOnce(artifact);
  const postMessage = jest.spyOn(worker, "postMessage").mockImplementation(() => {});

  await worker.onmessage!.call(worker, new MessageEvent("message", {
    data: { kind, payload: rejectedPayload, publicKey },
  }));
  expect(postMessage).toHaveBeenCalledTimes(1);
  expect(postMessage).toHaveBeenNthCalledWith(1, { artifact: null, error: undefined });

  await worker.onmessage!.call(worker, new MessageEvent("message", {
    data: { kind, payload: validPayload, publicKey },
  }));
  expect(postMessage).toHaveBeenCalledTimes(2);
  expect(postMessage).toHaveBeenNthCalledWith(2, { artifact, error: undefined });
  expect(parser.mock.calls).toEqual([[publicKey, rejectedPayload], [publicKey, validPayload]]);
  parsers.forEach((candidate, index) => {
    expect(candidate).toHaveBeenCalledTimes(index === kind - 1 ? 2 : 0);
  });
});
