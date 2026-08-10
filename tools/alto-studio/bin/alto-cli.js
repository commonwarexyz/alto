#!/usr/bin/env node

/**
 * Commonware Alto CLI
 */

import { defaultAltoRunner } from '../src/core/node-runner.js';
import { ALTO_CONFIG } from '../src/config.js';

const args = process.argv.slice(2);
const command = args[0] || 'help';

async function main() {
  switch (command.toLowerCase()) {
    case 'crates': {
      console.log('\n🎶 Commonware Alto Rust Crates:');
      ALTO_CONFIG.crates.forEach(c => {
        console.log(`  • [${c.name}]`);
        console.log(`    Description: ${c.description}\n`);
      });
      break;
    }

    case 'produce': {
      console.log('\n⚡ Producing Reference Block on Alto Node (~250ms)...');
      const block = defaultAltoRunner.produceBlock();
      console.log(`  Height:       #${block.height}`);
      console.log(`  Block Hash:   ${block.blockHash}`);
      console.log(`  Transactions: ${block.txCount}`);
      console.log(`  Block Time:   ${block.blockTimeMs}`);
      console.log(`  P2P Gossip:   ${block.gossipStatus}\n`);
      break;
    }

    case 'studio': {
      console.log('\n🌐 Launching Alto Studio on :3420...');
      await import('../src/server/app.js');
      break;
    }

    default: {
      console.log(`
╔══════════════════════════════════════════════════════════════════╗
║               🎶 COMMONWARE ALTO NODE CLI                        ║
║     Reference Blockchain Implementation & Benchmark Suite        ║
╚══════════════════════════════════════════════════════════════════╝

Commands:
  alto-cli crates                      List Alto reference Rust crates
  alto-cli produce                     Produce block on Alto reference chain
  alto-cli studio                      Launch Interactive Web Studio on :3420
      `);
      break;
    }
  }
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
