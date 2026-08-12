import { hexUint8Array } from "./utils";

export interface LeaderLocation {
  location: [number, number];
  locationName: string;
}

export const resolveLeaderLocation = (
  leader: Uint8Array | undefined,
  participants: string[] | undefined,
  locations: [[number, number], string][],
): LeaderLocation | undefined => {
  if (!leader || !participants || participants.length !== locations.length) {
    return undefined;
  }

  const publicKey = hexUint8Array(leader, leader.length * 2);
  const index = participants.indexOf(publicKey);
  if (index === -1) {
    return undefined;
  }

  return {
    location: locations[index][0],
    locationName: locations[index][1],
  };
};
