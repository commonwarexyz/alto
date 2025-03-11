use alto_client::{IndexQuery, Query};
use alto_types::{Finalized, Notarized, Seed};
use commonware_cryptography::sha256::Digest;
use commonware_utils::{SizedSerialize, SystemTimeExt};
use std::time;
use tracing::info;

// Parse IndexQuery for seed, notarization, and finalization
pub fn parse_index_query(query: &str) -> Option<IndexQuery> {
    if query == "latest" {
        Some(IndexQuery::Latest)
    } else if let Ok(index) = query.parse::<u64>() {
        Some(IndexQuery::Index(index))
    } else {
        None
    }
}

// Parse Query for block
pub fn parse_query(query: &str) -> Option<Query> {
    if query == "latest" {
        Some(Query::Latest)
    } else if let Ok(index) = query.parse::<u64>() {
        Some(Query::Index(index))
    } else {
        let bytes = commonware_utils::from_hex(query).expect("Failed to decode hex");
        let digest: [u8; Digest::SERIALIZED_LEN] = bytes.try_into().expect("Invalid digest length");
        Some(Query::Digest(digest.into()))
    }
}

const MS_PER_SECOND: u64 = 1000;
const MS_PER_HOUR: u64 = 3_600_000; // 60 * 60 * 1000
const MS_PER_DAY: u64 = 86_400_000; // 24 * 3_600_000

/// Formats the age in milliseconds into a human-readable string.
/// - Less than 1 second: "Xms" (e.g., "500ms")
/// - Less than 1 hour: "X.Ys" (e.g., "5.0s")
/// - Less than 1 day: "X.Yh" (e.g., "19.1h")
/// - Otherwise: "Xd Yh" (e.g., "2d 3h")
pub fn format_age(age: u64) -> String {
    if age < MS_PER_SECOND {
        format!("{}ms", age)
    } else if age < MS_PER_HOUR {
        let seconds = age as f64 / MS_PER_SECOND as f64;
        format!("{:.1}s", seconds)
    } else if age < MS_PER_DAY {
        let hours = age as f64 / MS_PER_HOUR as f64;
        format!("{:.1}h", hours)
    } else {
        let days = age / MS_PER_DAY;
        let remaining_ms = age % MS_PER_DAY;
        let hours = remaining_ms / MS_PER_HOUR;
        format!("{}d {}h", days, hours)
    }
}

pub fn log_seed(seed: Seed) {
    info!(view = seed.view, signature = ?seed.signature, "seed");
}

pub fn log_notarization(notarized: Notarized) {
    let now = time::SystemTime::now().epoch_millis();
    let age_ms = now.saturating_sub(notarized.block.timestamp);
    let age_str = format_age(age_ms);
    info!(
        view = notarized.proof.view,
        height = notarized.block.height,
        timestamp = notarized.block.timestamp,
        age = %age_str,
        digest = ?notarized.block.digest(),
        "notarized"
    );
}

pub fn log_finalization(finalized: Finalized) {
    let now = time::SystemTime::now().epoch_millis();
    let age_ms = now.saturating_sub(finalized.block.timestamp);
    let age_str = format_age(age_ms);
    info!(
        view = finalized.proof.view,
        height = finalized.block.height,
        timestamp = finalized.block.timestamp,
        age = %age_str,
        digest = ?finalized.block.digest(),
        "finalized"
    );
}

pub fn log_block(block: alto_types::Block) {
    let now = time::SystemTime::now().epoch_millis();
    let age_ms = now.saturating_sub(block.timestamp);
    let age_str = format_age(age_ms);
    info!(
        height = block.height,
        timestamp = block.timestamp,
        age = %age_str,
        digest = ?block.digest(),
        "block"
    );
}

pub fn log_latency(start: time::Instant) {
    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_millis();
    let elapsed_str = format_age(elapsed_ms as u64);
    info!(elapsed = %elapsed_str, "latency");
}
