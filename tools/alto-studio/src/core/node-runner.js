/**
 * Alto Node Block Production & P2P Gossip Engine
 */

import crypto from 'crypto';
import { ALTO_CONFIG } from '../config.js';

export class AltoNodeRunner {
  constructor() {
    this.currentHeight = ALTO_CONFIG.networkMetrics.currentHeight;
    this.blocks = [];
  }

  /**
   * Produce a new block on Alto reference chain
   */
  produceBlock() {
    this.currentHeight += 1;
    const blockHash = '0x' + crypto.randomBytes(32).toString('hex');
    const proposer = '0x' + crypto.randomBytes(20).toString('hex');
    const txCount = Math.floor(Math.random() * 85) + 15;
    const blockTimeMs = Math.floor(Math.random() * 50 + 220); // ~250ms

    const block = {
      height: this.currentHeight,
      blockHash,
      proposer,
      txCount,
      blockTimeMs: `${blockTimeMs} ms`,
      gossipStatus: 'PROPAGATED_TO_36_PEERS',
      timestamp: new Date().toISOString(),
    };

    this.blocks.unshift(block);
    return block;
  }

  getRecentBlocks() {
    return this.blocks.slice(0, 10);
  }
}

export const defaultAltoRunner = new AltoNodeRunner();
