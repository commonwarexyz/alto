use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

/// Sliding-window throughput tracker.
///
/// Records event timestamps and computes an events-per-second rate
/// over a configurable window. During startup (before the window is
/// full), the rate is computed over the actual elapsed time.
#[derive(Clone)]
pub struct Throughput {
    window: Duration,
    started_at: Option<SystemTime>,
    timestamps: VecDeque<SystemTime>,
}

impl Throughput {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            started_at: None,
            timestamps: VecDeque::new(),
        }
    }

    /// Record an event and return the current events-per-second rate.
    pub fn record(&mut self, now: SystemTime) -> f64 {
        // Lazily initialize start time on the first event
        let started_at = *self.started_at.get_or_insert(now);
        self.timestamps.push_back(now);

        // Evict timestamps that have fallen outside the sliding window
        let cutoff = now - self.window;
        while self.timestamps.front().is_some_and(|t| *t < cutoff) {
            self.timestamps.pop_front();
        }

        // Use actual elapsed time during startup, clamped to the window
        // once enough time has passed
        let elapsed = now
            .duration_since(started_at)
            .unwrap_or(Duration::ZERO)
            .min(self.window);
        if elapsed.is_zero() {
            0.0
        } else {
            self.timestamps.len() as f64 / elapsed.as_secs_f64()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_event_returns_zero() {
        let now = SystemTime::UNIX_EPOCH;
        let mut t = Throughput::new(Duration::from_secs(10));
        assert_eq!(t.record(now), 0.0);
    }

    #[test]
    fn rate_during_startup() {
        let start = SystemTime::UNIX_EPOCH;
        let mut t = Throughput::new(Duration::from_secs(30));

        t.record(start);
        let bps = t.record(start + Duration::from_secs(1));

        // 2 events in 1 second = 2.0 bps
        assert!((bps - 2.0).abs() < 0.01);
    }

    #[test]
    fn rate_after_window_full() {
        let start = SystemTime::UNIX_EPOCH;
        let window = Duration::from_secs(10);
        let mut t = Throughput::new(window);

        // Record 1 event per second for 20 seconds
        for i in 0..20 {
            t.record(start + Duration::from_secs(i));
        }

        // Window covers last 10 seconds: events at t=10..19 = 10 events / 10s = 1.0 bps
        let bps = t.record(start + Duration::from_secs(20));
        // 11 events in window (t=10..20 inclusive)
        assert!((bps - 1.1).abs() < 0.01);
    }

    #[test]
    fn old_events_are_pruned() {
        let start = SystemTime::UNIX_EPOCH;
        let window = Duration::from_secs(5);
        let mut t = Throughput::new(window);

        // Burst of 100 events at t=0
        for _ in 0..100 {
            t.record(start);
        }

        // At t=6, all old events should be pruned
        let bps = t.record(start + Duration::from_secs(6));
        // 1 event in 5 seconds (clamped window)
        assert!((bps - 0.2).abs() < 0.01);
    }
}
