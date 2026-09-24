/**
 * Alto Node Unit Tests
 */

import { defaultAltoRunner } from '../src/core/node-runner.js';

async function runNodeTests() {
  console.log('Testing Commonware Alto Reference Blockchain Node Runner...');

  // 1. Produce Block
  const block = defaultAltoRunner.produceBlock();
  if (!block.blockHash || !block.blockTimeMs) {
    throw new Error('Alto block production failed');
  }

  console.log(`✅ Alto Reference Node Block Produced (#${block.height} @ ${block.blockTimeMs})!`);
}

runNodeTests().catch(e => {
  console.error('❌ Node Test Failed:', e);
  process.exit(1);
});
