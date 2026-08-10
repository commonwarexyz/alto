/**
 * Commonware Alto Studio Client Logic
 */

let isAutoProducing = false;
let autoInterval = null;

document.addEventListener('DOMContentLoaded', () => {
  initTabs();
  loadConfig();
  initListeners();
});

function initTabs() {
  const tabs = document.querySelectorAll('.nav-tab');
  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.nav-tab').forEach(t => t.classList.toggle('active', t === tab));
      document.querySelectorAll('.tab-pane').forEach(p => p.classList.toggle('active', p.id === `tab-${tab.dataset.tab}`));
    });
  });
}

async function loadConfig() {
  try {
    const res = await fetch('/api/config');
    const data = await res.json();

    document.getElementById('header-height').textContent = `Height: #${data.metrics.currentHeight.toLocaleString()}`;

    const grid = document.getElementById('crates-container');
    grid.innerHTML = '';

    data.crates.forEach(c => {
      const card = document.createElement('div');
      card.className = 'crate-card';
      card.innerHTML = `
        <div class="crate-title">${c.name}</div>
        <div class="crate-desc">${c.description}</div>
      `;
      grid.appendChild(card);
    });
  } catch (e) {
    console.error(e);
  }
}

function initListeners() {
  document.getElementById('btn-produce-block').addEventListener('click', produceBlock);

  const autoBtn = document.getElementById('btn-toggle-auto');
  autoBtn.addEventListener('click', () => {
    if (isAutoProducing) {
      clearInterval(autoInterval);
      isAutoProducing = false;
      autoBtn.textContent = '▶️ Start Auto-Block Production (4 blocks/sec)';
      autoBtn.className = 'btn btn-gradient btn-lg';
    } else {
      isAutoProducing = true;
      autoBtn.textContent = '⏸️ Pause Alto Block Production';
      autoBtn.className = 'btn btn-secondary btn-lg';
      produceBlock();
      autoInterval = setInterval(produceBlock, 250); // 250ms per block!
    }
  });
}

async function produceBlock() {
  try {
    const res = await fetch('/api/node/produce', { method: 'POST' });
    const data = await res.json();
    if (data.success) {
      appendBlockRow(data.block);
    }
  } catch (e) {
    console.warn(e);
  }
}

function appendBlockRow(block) {
  const container = document.getElementById('blocks-container');
  const empty = container.querySelector('.empty-state');
  if (empty) container.innerHTML = '';

  const row = document.createElement('div');
  row.className = 'ledger-row';
  row.innerHTML = `
    <div>
      <div style="font-weight: 700; color: #fff;">Block #${block.height.toLocaleString()}</div>
      <div class="mono text-muted" style="font-size: 0.72rem;">Proposer: ${block.proposer.slice(0, 14)}...</div>
    </div>
    <div style="text-align: right;">
      <div style="color: #ea580c; font-weight: 700; font-family: var(--font-mono);">${block.blockTimeMs} Block Time</div>
      <div class="mono text-muted" style="font-size: 0.72rem;">${block.txCount} txs • ${block.gossipStatus}</div>
    </div>
  `;
  container.insertBefore(row, container.firstChild);
}
