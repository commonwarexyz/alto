/**
 * Alto Node Web Studio Server
 */

import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import { ALTO_CONFIG } from '../config.js';
import { defaultAltoRunner } from '../core/node-runner.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const WEB_ROOT = path.join(__dirname, '../../web');

const app = express();
const PORT = process.env.PORT || 3420;

app.use(cors());
app.use(express.json());
app.use(express.static(WEB_ROOT));

// 1. Get Node Info & Crates
app.get('/api/config', (req, res) => {
  res.json({
    node: ALTO_CONFIG.node,
    crates: ALTO_CONFIG.crates,
    metrics: ALTO_CONFIG.networkMetrics,
  });
});

// 2. Produce Alto Block
app.post('/api/node/produce', (req, res) => {
  const block = defaultAltoRunner.produceBlock();
  res.json({ success: true, block });
});

// 3. Get Chain Blocks
app.get('/api/node/blocks', (req, res) => {
  res.json(defaultAltoRunner.getRecentBlocks());
});

if (process.env.NODE_ENV !== 'test') {
  app.listen(PORT, () => {
    console.log(`\n======================================================`);
    console.log(`🎶 Commonware Alto Reference Blockchain Studio Running!`);
    console.log(`🌐 Web Dashboard: http://localhost:${PORT}`);
    console.log(`⚡ Reference Implementation: Sub-Second Simplex Consensus`);
    console.log(`======================================================\n`);
  });
}

export default app;
