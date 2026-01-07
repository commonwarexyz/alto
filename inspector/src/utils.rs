use alto_client::{IndexQuery, Query};
use alto_types::{Finalized, Notarized, Seed};
use commonware_codec::DecodeExt;
use commonware_consensus::Viewable;
use commonware_cryptography::{sha256::Digest, Digestible};
use commonware_utils::SystemTimeExt;
use std::time;
use tracing::{debug, info};

// Define enums for query kinds
pub enum IndexQueryKind {
    Single(IndexQuery),
    Range(u64, u64),
}

pub enum QueryKind {
    Single(Query),
    Range(u64, u64),
}

// Parse IndexQuery for seed, notarization, and finalization
pub fn parse_index_query(query: &str) -> Option<IndexQueryKind> {
    if query == "latest" {
        Some(IndexQueryKind::Single(IndexQuery::Latest))
    } else if let Some((start, end)) = parse_range(query) {
        Some(IndexQueryKind::Range(start, end))
    } else if let Ok(index) = query.parse::<u64>() {
        Some(IndexQueryKind::Single(IndexQuery::Index(index)))
    } else {
        None
    }
}

// Parse Query for block
pub fn parse_query(query: &str) -> Option<QueryKind> {
    if query == "latest" {
        Some(QueryKind::Single(Query::Latest))
    } else if let Some((start, end)) = parse_range(query) {
        Some(QueryKind::Range(start, end))
    } else if let Ok(index) = query.parse::<u64>() {
        Some(QueryKind::Single(Query::Index(index)))
    } else {
        let bytes = commonware_utils::from_hex(query)?;
        let digest = Digest::decode(bytes.as_ref()).ok()?;
        Some(QueryKind::Single(Query::Digest(digest)))
    }
}

// Helper function to parse range queries
fn parse_range(query: &str) -> Option<(u64, u64)> {
    let parts: Vec<&str> = query.split("..").collect();
    if parts.len() == 2 {
        let start = parts[0].parse::<u64>().ok()?;
        let end = parts[1].parse::<u64>().ok()?;
        if start <= end {
            Some((start, end))
        } else {
            None
        }
    } else {
        None
    }
}

// Existing logging functions remain unchanged
const MS_PER_SECOND: u64 = 1000;
const MS_PER_HOUR: u64 = 3_600_000;
const MS_PER_DAY: u64 = 86_400_000;

pub fn format_age(age: u64) -> String {
    if age < MS_PER_SECOND {
        format!("{age}ms")
    } else if age < MS_PER_HOUR {
        let seconds = age as f64 / MS_PER_SECOND as f64;
        format!("{seconds:.1}s")
    } else if age < MS_PER_DAY {
        let hours = age as f64 / MS_PER_HOUR as f64;
        format!("{hours:.1}h")
    } else {
        let days = age / MS_PER_DAY;
        let remaining_ms = age % MS_PER_DAY;
        let hours = remaining_ms / MS_PER_HOUR;
        format!("{days}d {hours}h")
    }
}

pub fn log_seed(seed: Seed) {
    info!(view = %seed.view(), signature = ?seed.signature, "seed");
}

pub fn log_notarization(notarized: Notarized) {
    let now = time::SystemTime::now().epoch_millis();
    let age_ms = now.saturating_sub(notarized.block.timestamp);
    let age_str = format_age(age_ms);
    info!(
        view = %notarized.proof.view(),
        height = %notarized.block.height,
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
        view = %finalized.proof.view(),
        height = %finalized.block.height,
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
        height = %block.height,
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
    debug!(elapsed = %elapsed_str, "latency");
}
