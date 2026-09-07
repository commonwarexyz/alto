import { hexUint8Array } from "./utils";

export const SEED_SIGNATURE_COLOR = "#0000eeff";
export const BLOCK_HASH_COLOR = "#9a3412";

export interface TimelineIdentifier {
  label: string;
  color: string;
  value: string;
  fullValue: string;
}

export interface LeaderIndicator {
  label: "Proposed" | "Seeded";
  color: string;
}

export const getLeaderIndicator = (
  standardCertificates: boolean,
): LeaderIndicator => ({
  label: standardCertificates ? "Proposed" : "Seeded",
  color: standardCertificates ? BLOCK_HASH_COLOR : SEED_SIGNATURE_COLOR,
});

export const getTimelineIdentifier = (
  standardCertificates: boolean,
  seedSignature?: ArrayLike<number>,
  blockDigest?: ArrayLike<number>,
): TimelineIdentifier => {
  const bytes = standardCertificates ? blockDigest : seedSignature;

  return {
    label: standardCertificates ? "Block hash" : "Seed signature",
    color: standardCertificates ? BLOCK_HASH_COLOR : SEED_SIGNATURE_COLOR,
    value: hexUint8Array(bytes),
    fullValue: hexUint8Array(bytes, bytes ? bytes.length * 2 : 0),
  };
};
