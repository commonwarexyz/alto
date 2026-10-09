import { hexUint8Array } from "./utils";

export const BLOCK_HASH_COLOR = "#9a3412";

export interface TimelineIdentifier {
  label: string;
  color: string;
  value: string;
  fullValue: string;
}

export interface LeaderIndicator {
  label: "Elected";
  color: string;
}

export const LEADER_INDICATOR: LeaderIndicator = {
  label: "Elected",
  color: BLOCK_HASH_COLOR,
};

// Stable leaders are known in advance, so each view is identified by its proposal digest.
export const getTimelineIdentifier = (
  blockDigest?: ArrayLike<number>,
): TimelineIdentifier => ({
  label: "Proposal",
  color: BLOCK_HASH_COLOR,
  value: hexUint8Array(blockDigest),
  fullValue: hexUint8Array(blockDigest, blockDigest ? blockDigest.length * 2 : 0),
});
