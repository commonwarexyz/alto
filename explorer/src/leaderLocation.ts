import { hexUint8Array } from "./utils";
import type { ClusterConfig } from "./config";

export interface LeaderLocation {
  location: [number, number];
  locationName: string;
}

export const resolveLeaderLocation = (
  leader: ArrayLike<number>,
  participants: string[] | undefined,
  locations: ClusterConfig['LOCATIONS'],
): LeaderLocation | undefined => {
  if (!participants || participants.length !== locations.length) {
    return undefined;
  }

  const publicKey = hexUint8Array(leader, leader.length * 2);
  const index = participants.indexOf(publicKey);
  const location = locations[index];
  return location ? { location: location[0], locationName: location[1] } : undefined;
};
