import { hexUint8Array } from "./utils";

export const SEED_SIGNATURE_COLOR = "#0000eeff";
export const BLOCK_HASH_COLOR = "#9a3412";

export interface TimelineIdentifier {
  label: string;
  color: string;
  value: string;
  fullValue: string;
}

export const getTimelineIdentifier = (
  standardCertificates: boolean,
  seedSignature?: Uint8Array,
  blockDigest?: Uint8Array,
): TimelineIdentifier => {
  const bytes = standardCertificates ? blockDigest : seedSignature;

  return {
    label: standardCertificates ? "Block hash" : "Seed signature",
    color: standardCertificates ? BLOCK_HASH_COLOR : SEED_SIGNATURE_COLOR,
    value: hexUint8Array(bytes),
    fullValue: hexUint8Array(bytes, bytes ? bytes.length * 2 : 0),
  };
};
