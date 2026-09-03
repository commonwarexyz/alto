const TIMELINE_DURATION_MS = 1_000;

export const scaleTimelineWidth = (latency: number, availableWidth: number): number =>
  Math.min(Math.max(latency, 0) / TIMELINE_DURATION_MS, 1) * availableWidth;
