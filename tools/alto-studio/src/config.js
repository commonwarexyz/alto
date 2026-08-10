/**
 * Commonware Alto Reference Blockchain Node Configuration
 */

export const ALTO_CONFIG = {
  node: {
    name: 'Alto Reference Blockchain Node',
    language: 'Rust (tokio / async runtime)',
    library: 'Commonware Primitives Monorepo',
    consensusEngine: 'consensus::simplex (Sub-second BFT)',
    p2pProtocol: 'commonware-p2p (Authenticated & Encrypted)',
  },
  crates: [
    { name: 'alto-chain', description: 'Core blockchain state transition machine & block headers.' },
    { name: 'alto-validator', description: 'BFT consensus participant & VRF leader block proposer.' },
    { name: 'alto-client', description: 'RPC client & transaction broadcaster.' },
    { name: 'alto-indexer', description: 'High-speed event & transaction indexer.' },
  ],
  networkMetrics: {
    activePeers: 36,
    targetBlockTimeMs: 250,
    currentHeight: 148520,
    networkBandwidthMbSec: 14.8,
  },
};
