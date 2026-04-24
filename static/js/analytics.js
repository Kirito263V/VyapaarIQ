/* =====================================================================
   VYAPAARIQ ANALYTICS — COMPLETE FRONTEND LOGIC
   ===================================================================== */

const $ = id => document.getElementById(id);
const storedRange = localStorage.getItem('analyticsRange');
let selectedRange = storedRange !== null && !Number.isNaN(Number(storedRange)) ? Number(storedRange) : 180;
let modalChartInstance = null;
let chartInstances = {};

/* ===== HELPERS ===== */
const fmtINR = v => '₹' + Number(v||0).toLocaleString('en-IN',{maximumFractionDigits:2});
const fmtK = v => {
  const n = Number(v||0);
  if(n >= 10000000) return '₹' + (n/10000000).toFixed(1) + 'Cr';
  if(n >= 100000) return '₹' + (n/100000).toFixed(1) + 'L';
  if(n >= 1000) return '₹' + (n/1000).toFixed(1) + 'K';
  return fmtINR(n);
};
const fmtPct = v => (Number(v||0) >= 0 ? '+' : '') + Number(v||0).toFixed(1) + '%';
const apiUrl = path => {
  if (selectedRange === 0) return path;
  return `${path}?range_days=${selectedRange}`;
};

async function fetchJSON(url) {
  const r = await fetch(url);
  if(!r.ok) throw new Error('API Error: ' + url);
  return r.json();
}

/* ===== TOAST ===== */
function toast(msg, type='suc', dur=3500) {
  const tc = $('toast-container');
  const icons = {suc:'✅',err:'❌',inf:'📡'};
  const t = document.createElement('div');
  t.className = `toast ${type}`;
  t.innerHTML = `<span>${icons[type]||'ℹ'}</span><span>${msg}</span><button class="toast-close" onclick="this.parentElement.remove()">✕</button>`;
  tc.appendChild(t);
  setTimeout(() => { t.classList.add('out'); setTimeout(() => t.remove(), 400); }, dur);
}

/* ===== THEME ===== */
function toggleTheme() {
  const h = document.documentElement, k = $('knob');
  h.dataset.theme = h.dataset.theme === 'dark' ? 'light' : 'dark';
  k.textContent = h.dataset.theme === 'dark' ? '🌙' : '☀️';
  // Rebuild charts to match new theme
  setTimeout(() => refreshAll(), 50);
}

/* ===== SIDEBAR ===== */
function toggleSidebar() { $('sidebar').classList.toggle('open'); $('overlay').classList.toggle('show'); }
function closeSidebar() { if(window.innerWidth <= 768) { $('sidebar').classList.remove('open'); $('overlay').classList.remove('show'); } }

/* ===== FILTER ===== */
function setRange(days) {
  document.querySelectorAll('.fpill').forEach(p => p.classList.remove('active'));
  const activeButton = document.querySelector(`.fpill[data-range='${days}']`);
  if (activeButton) activeButton.classList.add('active');
  selectedRange = days;
  localStorage.setItem('analyticsRange', String(days));
  updatePeriodLabel();
  reloadAnalytics();
}

function getPeriodSuffix() {
  if (selectedRange === 30) return '30 days';
  if (selectedRange === 90) return '3 months';
  if (selectedRange === 180) return '6 months';
  if (selectedRange === 365) return '12 months';
  return 'all time';
}

function updatePeriodLabel() {
  let label = 'Showing all time';
  if (selectedRange === 30) label = 'Showing last 30 days';
  if (selectedRange === 90) label = 'Showing last 3 months';
  if (selectedRange === 180) label = 'Showing last 6 months';
  if (selectedRange === 365) label = 'Showing last 12 months';
  const el = document.getElementById('periodLabel');
  if (el) el.innerText = label;
}

function updateActiveRangeButton() {
  document.querySelectorAll('.fpill').forEach(p => p.classList.remove('active'));
  const activeButton = document.querySelector(`.fpill[data-range='${selectedRange}']`);
  if (activeButton) activeButton.classList.add('active');
}

/* ===== CHART FACTORY ===== */
function getThemeColors() {
  const isDark = document.documentElement.dataset.theme !== 'light';
  return {
    grid: isDark ? 'rgba(0,212,255,.06)' : 'rgba(0,100,200,.06)',
    text: isDark ? '#4a637a' : '#7a90b0',
    card: isDark ? '#0d1628' : '#f8faff',
  };
}

function destroyChart(id) {
  if(chartInstances[id]) { chartInstances[id].destroy(); delete chartInstances[id]; }
}

function buildGradient(ctx, colors, height=260) {
  const g = ctx.createLinearGradient(0, 0, 0, height);
  g.addColorStop(0, colors[0]);
  g.addColorStop(1, colors[1]);
  return g;
}

/* ===== CHART.JS GLOBAL DEFAULTS ===== */
function applyChartDefaults() {
  const { text, grid } = getThemeColors();
  Chart.defaults.color = text;
  Chart.defaults.borderColor = grid;
  Chart.defaults.font.family = "'DM Mono', monospace";
  Chart.defaults.font.size = 11;
  Chart.defaults.plugins.legend.labels.boxWidth = 10;
  Chart.defaults.plugins.legend.labels.padding = 16;
  Chart.defaults.plugins.tooltip.backgroundColor = 'rgba(6,12,24,.92)';
  Chart.defaults.plugins.tooltip.borderColor = 'rgba(0,212,255,.2)';
  Chart.defaults.plugins.tooltip.borderWidth = 1;
  Chart.defaults.plugins.tooltip.padding = 10;
  Chart.defaults.plugins.tooltip.titleFont = { family: "'Syne',sans-serif", weight: '800', size: 12 };
  Chart.defaults.plugins.tooltip.bodyFont = { family: "'DM Mono',monospace", size: 11 };
}

/* ===== KPIs ===== */
function setKPI(id, val, ch, cls) {
  const el = $(id); if(!el) return;
  el.innerHTML = val;
  const chEl = $(id+'-ch'); if(!chEl) return;
  chEl.textContent = ch;
  chEl.className = 'kpi-ch ' + cls;
}

async function loadKPIs() {
  try {
    const [sum, inv] = await Promise.all([
      fetchJSON(apiUrl('/api/analytics-summary')),
      fetchJSON(apiUrl('/api/inventory-insights'))
    ]);
    const rev = Number(sum.total_sales||0);
    const purch = Number(sum.total_purchases||0);
    const exp = Number(sum.total_expenses||0);
    const profit = rev - purch - exp;
    const invVal = inv.reduce((s,r) => s + Number(r.current_stock||0)*Number(r.cost_price||0), 0);

    setKPI('k-revenue', fmtK(rev), `${sum.total_orders||0} orders · ${getPeriodSuffix()}`, 'up');
    setKPI('k-profit', fmtK(profit), profit >= 0 ? '↑ Net profit' : '↓ Net loss', profit >= 0 ? 'up' : 'down');
    setKPI('k-aov', fmtINR(sum.avg_order_value||0), `Per invoice avg`, 'neu');
    setKPI('k-inventory', fmtK(invVal), `${inv.length} products`, 'neu');
    setKPI('k-expratio', Number(sum.expense_ratio||0).toFixed(1)+'%', fmtK(exp)+' spent', 'warn');
    setKPI('k-stockrisk', String(sum.low_stock_count||0), `${sum.low_stock_count||0} below threshold`, (sum.low_stock_count||0) > 0 ? 'warn' : 'neu');

    $('alertBadge').textContent = sum.low_stock_count || 0;
    generateInsights(sum, inv, profit, invVal);
  } catch(e) {
    // Demo fallback
    setKPI('k-revenue', '₹4,28,600', '↑ +14.2% this month', 'up');
    setKPI('k-profit', '₹68,420', '↑ +12.4% margin', 'up');
    setKPI('k-aov', '₹2,003', '214 invoices', 'neu');
    setKPI('k-inventory', '₹1.2L', '186 products', 'neu');
    setKPI('k-expratio', '8.9%', '₹38,200 spent', 'warn');
    setKPI('k-stockrisk', '3', '3 below threshold', 'warn');
    generateInsights({total_sales:428600,total_expenses:38200,expense_ratio:8.9,low_stock_count:3,top_product:'Basmati Rice 5kg'}, [], 68420, 120000);
  }
}

function loadDashboardSummary() {
  return loadKPIs();
}

function loadRevenueTrend() {
  return loadSalesTrend();
}

function loadExpenseDistribution() {
  return loadExpenseBreakdown();
}

function loadProfitBreakdown() {
  return loadProfitAnalysis();
}

function loadInventorySummary() {
  return loadInventory();
}

async function reloadAnalytics() {
  const btn = $('refreshBtn');
  if(btn) btn.classList.add('refreshing');
  toast('Refreshing all analytics…','inf',2000);
  updateActiveRangeButton();
  updatePeriodLabel();
  applyChartDefaults();
  await Promise.allSettled([
    loadDashboardSummary(),
    loadRevenueTrend(),
    loadTopProducts(),
    loadProfitBreakdown(),
    loadCustomerInsights(),
    loadExpenseDistribution(),
    loadInventorySummary(),
  ]);
  if(btn) { setTimeout(() => btn.classList.remove('refreshing'), 500); }
  toast('Analytics refreshed','suc');
}

/* ===== SMART INSIGHTS ===== */
function generateInsights(sum, inv, profit, invVal) {
  const insights = [];
  const rev = Number(sum.total_sales||0);
  const exp = Number(sum.total_expenses||0);
  const ratio = Number(sum.expense_ratio||0);
  const lowStock = Number(sum.low_stock_count||0);

  if(profit > 0) insights.push({dot:'green', icon:'📈', text:`Net profit of ${fmtK(profit)} — business is generating positive returns.`});
  else insights.push({dot:'red', icon:'📉', text:`Net loss of ${fmtK(Math.abs(profit))} — review costs and pricing strategy.`});

  if(ratio > 15) insights.push({dot:'amber', icon:'⚠️', text:`Expense ratio at ${ratio.toFixed(1)}% — above 15% threshold. Review controllable costs.`});
  else insights.push({dot:'green', icon:'✅', text:`Expense ratio of ${ratio.toFixed(1)}% is within healthy range.`});

  if(lowStock > 0) insights.push({dot:'amber', icon:'🔔', text:`${lowStock} product${lowStock>1?'s':''} below reorder level. Restock soon to avoid stockouts.`});
  else insights.push({dot:'green', icon:'📦', text:`All products above reorder levels. Inventory is well-stocked.`});

  if(sum.top_product) insights.push({dot:'cyan', icon:'🏆', text:`Top product: "${sum.top_product}" is leading revenue contribution this period.`});
  if(invVal > 0) insights.push({dot:'cyan', icon:'💰', text:`Total inventory valued at ${fmtK(invVal)} — monitor turnover to avoid dead stock.`});
  if(rev > 0 && exp/rev > 0.1) insights.push({dot:'amber', icon:'📋', text:`Expenses consume ${((exp/rev)*100).toFixed(1)}% of revenue. Target below 10% for better margins.`});

  const scroll = $('insightsScroll');
  if(!insights.length) { scroll.innerHTML = '<div class="empty-state">No insights available yet. Import data to begin.</div>'; return; }
  scroll.innerHTML = insights.map(i => `
    <div class="insight-item">
      <div class="insight-dot ${i.dot}"></div>
      <span class="insight-icon">${i.icon}</span>
      <span class="insight-text">${i.text}</span>
    </div>
  `).join('');
  $('insightCount').textContent = insights.length + ' signal' + (insights.length !== 1 ? 's' : '');
}

/* ===== SALES TREND CHART ===== */
async function loadSalesTrend() {
  try {
    const data = await fetchJSON(apiUrl('/api/analytics-summary'));
    renderSalesTrend({ labels: data.revenue_chart_labels || [], values: data.revenue_chart || [] });
  } catch(e) {
    renderSalesTrend({ labels:['Oct','Nov','Dec','Jan','Feb','Mar'], values:[62000,78000,95000,71000,88000,100000] });
  }
}

function renderSalesTrend(data) {
  const labels = data.labels || [];
  const values = data.values || [];
  if(!values.length) { $('gs-peak').textContent = '—'; return; }

  const max = Math.max(...values, 1);
  const peak = values.indexOf(Math.max(...values));
  const low = values.indexOf(Math.min(...values));
  const last = values[values.length-1]||0;
  const prev = values.length > 1 ? values[values.length-2] : 0;
  const mom = prev ? ((last-prev)/prev)*100 : 0;
  const total = values.reduce((s,v) => s+v, 0);

  $('gs-peak').textContent = labels[peak] || '—';
  $('gs-low').textContent = labels[low] || '—';
  const momEl = $('gs-mom');
  momEl.textContent = fmtPct(mom);
  momEl.style.color = mom >= 0 ? 'var(--green)' : 'var(--red)';
  $('gs-total').textContent = fmtK(total);
  const badge = document.querySelector('.chart-badge');
  if (badge) {
    badge.textContent = `${values.length}-period`;
  }

  applyChartDefaults();
  destroyChart('salesTrend');
  const ctx = $('salesTrendChart').getContext('2d');
  const grad = buildGradient(ctx, ['rgba(0,212,255,.35)', 'rgba(0,212,255,.01)']);
  chartInstances['salesTrend'] = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Revenue',
        data: values,
        borderColor: '#00d4ff',
        borderWidth: 2.5,
        backgroundColor: grad,
        fill: true,
        tension: 0.45,
        pointBackgroundColor: '#00d4ff',
        pointBorderColor: 'var(--card)',
        pointBorderWidth: 2,
        pointRadius: 4,
        pointHoverRadius: 7,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: ctx => ' ' + fmtK(ctx.raw) } }
      },
      scales: {
        x: { grid: { display: false }, ticks: { font: { size: 10 } } },
        y: { grid: { color: getThemeColors().grid }, ticks: { callback: v => fmtK(v) } }
      }
    }
  });
}

/* ===== TOP PRODUCTS CHART ===== */
async function loadTopProducts() {
  try {
    const data = await fetchJSON(apiUrl('/api/top-products'));
    renderTopProducts(data);
  } catch(e) {
    renderTopProducts([
      {name:'Basmati Rice 5kg', revenue:124600},
      {name:'Redmi Note 13', revenue:98200},
      {name:'Tata Salt 1kg', revenue:72400},
      {name:'Surf Excel 1kg', revenue:54100},
      {name:'Amul Butter', revenue:41800},
    ]);
  }
}

function renderTopProducts(rows) {
  if(!rows.length) return;
  applyChartDefaults();
  destroyChart('topProducts');
  const ctx = $('topProductsChart').getContext('2d');
  const colors = ['#00d4ff','#5b8cff','#8a7cff','#22d87a','#f59e0b'];
  chartInstances['topProducts'] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: rows.map(r => r.name.length > 14 ? r.name.substring(0,14)+'…' : r.name),
      datasets: [{
        label: 'Revenue',
        data: rows.map(r => Number(r.revenue||0)),
        backgroundColor: rows.map((_, i) => colors[i % colors.length] + '99'),
        borderColor: rows.map((_, i) => colors[i % colors.length]),
        borderWidth: 1.5,
        borderRadius: 5,
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      onClick: (e, elems) => { if(elems.length) openProductModal(rows[elems[0].index]); },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: ctx => ' ' + fmtK(ctx.raw) } }
      },
      scales: {
        x: { grid: { color: getThemeColors().grid }, ticks: { callback: v => fmtK(v) } },
        y: { grid: { display: false }, ticks: { font: { size: 10 } } }
      }
    }
  });
}

/* ===== PROFIT ANALYSIS ===== */
async function loadProfitAnalysis() {
  try {
    renderProfitAnalysis(await fetchJSON(apiUrl('/api/profit-analysis')));
  } catch(e) {
    renderProfitAnalysis({ revenue:0, cogs:0, expenses:0, profit:0, gross_margin:0, net_margin:0, roi:0 });
  }
  await loadRevenueCostTrend();
}

function renderProfitAnalysis(data) {
  const rev = Number(data.revenue||0);
  const cogs = Number(data.cogs||0);
  const exp = Number(data.expenses||0);
  const profit = Number(data.profit||0);
  $('pv-rev').textContent = fmtK(rev);
  $('pv-cogs').textContent = fmtK(cogs);
  $('pv-exp').textContent = fmtK(exp);
  $('pv-profit').textContent = fmtK(profit);
  $('m-gross').textContent = Number(data.gross_margin||0).toFixed(1)+'%';
  $('m-net').textContent = Number(data.net_margin||0).toFixed(1)+'%';
  $('m-roi').textContent = Number(data.roi||0).toFixed(1)+'%';
  setTimeout(() => {
    $('pf-rev').style.width = rev ? '100%' : '0%';
    $('pf-cogs').style.width = rev ? Math.min(100,(cogs/rev)*100)+'%' : '0%';
    $('pf-exp').style.width = rev ? Math.min(100,(exp/rev)*100)+'%' : '0%';
    $('pf-profit').style.width = rev ? Math.min(100,Math.abs(profit/rev)*100)+'%' : '0%';
  }, 80);
}

async function loadRevenueCostTrend() {
  try {
    renderRevenueCostTrend(await fetchJSON(apiUrl('/api/revenue-cost-trend')));
  } catch(e) {
    renderRevenueCostTrend({ labels: [], revenue: [], cost: [] });
  }
}

function showRevenueCostTrendEmptyState() {
  destroyChart('revCost');
  const wrap = $('revCostWrap');
  const canvas = $('revCostChart');
  if(canvas) canvas.style.display = 'none';
  if(wrap && !$('revCostEmpty')) {
    const empty = document.createElement('div');
    empty.id = 'revCostEmpty';
    empty.className = 'empty-state';
    empty.textContent = 'No transaction data available for selected period';
    wrap.appendChild(empty);
  }
}

function renderRevenueCostTrend(data) {
  const labels = Array.isArray(data.labels) ? data.labels : [];
  const revenue = Array.isArray(data.revenue) ? data.revenue.map(v => Number(v||0)) : [];
  const cost = Array.isArray(data.cost) ? data.cost.map(v => Number(v||0)) : [];

  if(labels.length === 0) {
    console.warn('No revenue-cost trend data available');
    showRevenueCostTrendEmptyState();
    return;
  }

  applyChartDefaults();
  destroyChart('revCost');
  const emptyState = $('revCostEmpty');
  if(emptyState) emptyState.remove();
  const canvas = $('revCostChart');
  if(canvas) canvas.style.display = '';
  const ctx = $('revCostChart').getContext('2d');
  const g1 = buildGradient(ctx, ['rgba(0,212,255,.3)','rgba(0,212,255,.01)']);
  const g2 = buildGradient(ctx, ['rgba(239,68,68,.25)','rgba(239,68,68,.01)']);
  chartInstances['revCost'] = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        { label:'Revenue', data:revenue, borderColor:'#00d4ff', backgroundColor:g1, fill:true, tension:0.4, borderWidth:2, pointRadius:3 },
        { label:'Total Cost', data:cost, borderColor:'#ef4444', backgroundColor:g2, fill:true, tension:0.4, borderWidth:2, pointRadius:3 }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode:'index', intersect:false },
      plugins: { tooltip: { callbacks: { label: ctx => ' ' + fmtK(ctx.raw) } } },
      scales: {
        x: { grid:{ display:false } },
        y: { grid:{ color:getThemeColors().grid }, ticks:{ callback: v => fmtK(v) } }
      }
    }
  });
}

/* ===== CUSTOMER INSIGHTS ===== */
async function loadCustomerInsights() {
  try {
    renderCustomerInsights(await fetchJSON(apiUrl('/api/customer-insights')));
  } catch(e) {
    renderCustomerInsights([
      {name:'Priya Sharma', total:82400},
      {name:'Ravi Kumar', total:74200},
      {name:'Mehta Wholesale', total:68100},
      {name:'Neha Patel', total:52300},
      {name:'Arjun Singh', total:44700},
    ]);
  }
}

function renderCustomerInsights(rows) {
  const total = rows.reduce((s,r) => s+Number(r.total||0), 0);
  const max = Math.max(...rows.map(r => Number(r.total||0)), 1);
  const board = $('customerLeaderboard');
  if(!rows.length) { board.innerHTML = '<div class="empty-state">No customer data available.</div>'; return; }
  board.innerHTML = rows.map((r,i) => {
    const rev = Number(r.total||0);
    const pct = total ? ((rev/total)*100).toFixed(1) : 0;
    return `
      <div class="customer-row" onclick="openCustomerModal('${r.name}', ${rev}, ${pct})">
        <div class="c-rank">#${i+1}</div>
        <div><div class="c-name">${r.name}</div><div class="c-track"><div class="c-fill" data-w="${(rev/max)*100}"></div></div></div>
        <div class="c-revenue">${fmtK(rev)}</div>
        <div class="c-pct">${pct}% share</div>
      </div>
    `;
  }).join('');
  requestAnimationFrame(() => board.querySelectorAll('.c-fill').forEach(f => f.style.width = f.dataset.w+'%'));

  // Segmentation
  const highFreq = rows.filter(r => Number(r.total||0) > 50000).length;
  const regular = rows.length - highFreq;
  const hfPct = rows.length ? Math.round((highFreq/rows.length)*100) : 0;
  setTimeout(() => {
    $('seg-a').style.width = hfPct+'%';
    $('seg-b').style.width = (100-hfPct)+'%';
  }, 60);
  $('seg-a-pct').textContent = hfPct+'%';
  $('seg-b-pct').textContent = (100-hfPct)+'%';
  $('seg-insight').textContent = `${highFreq} high-frequency customers drive ${hfPct}% of the customer base.`;

  // Segment donut
  applyChartDefaults();
  destroyChart('custSeg');
  const ctx = $('customerSegChart').getContext('2d');
  chartInstances['custSeg'] = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: ['High-Frequency', 'Regular'],
      datasets: [{ data:[highFreq, regular], backgroundColor:['rgba(0,212,255,.7)','rgba(34,216,122,.7)'], borderColor:['#00d4ff','#22d87a'], borderWidth:2, hoverOffset:6 }]
    },
    options: { responsive:true, maintainAspectRatio:false, cutout:'65%', plugins:{ legend:{ position:'bottom' } } }
  });

  // Order intensity
  const orders = rows.map(r => Math.max(1, Math.round(Number(r.total||0)/500)));
  applyChartDefaults();
  destroyChart('orderInt');
  const ctx2 = $('orderIntensityChart').getContext('2d');
  chartInstances['orderInt'] = new Chart(ctx2, {
    type: 'bar',
    data: {
      labels: rows.map(r => r.name.split(' ')[0]),
      datasets: [{
        label: 'Est. Orders',
        data: orders,
        backgroundColor: 'rgba(34,216,122,.6)',
        borderColor: '#22d87a',
        borderWidth: 1.5,
        borderRadius: 5,
      }]
    },
    options: {
      responsive:true, maintainAspectRatio:false,
      plugins:{ legend:{ display:false } },
      scales: {
        x:{ grid:{ display:false } },
        y:{ grid:{ color:getThemeColors().grid }, ticks:{ stepSize:1 } }
      }
    }
  });
}

/* ===== EXPENSE ANALYTICS ===== */
async function loadExpenseBreakdown() {
  try {
    renderExpenseBreakdown(await fetchJSON(apiUrl('/api/expense-breakdown')));
  } catch(e) {
    renderExpenseBreakdown([
      {category:'Salaries', amount:18000, pct:47.1, change:0},
      {category:'Rent', amount:8000, pct:20.9, change:0},
      {category:'Transport', amount:4200, pct:11.0, change:198},
      {category:'Utilities', amount:3800, pct:9.9, change:-5},
      {category:'Marketing', amount:2600, pct:6.8, change:12},
      {category:'Maintenance', amount:1600, pct:4.2, change:-2},
    ]);
  }
}

function renderExpenseBreakdown(rows) {
  if(!rows.length) { $('expenseTbody').innerHTML = '<tr><td colspan="4" class="empty-state">No expense data.</td></tr>'; return; }
  const colors = ['#00d4ff','#5b8cff','#8a7cff','#22d87a','#f59e0b','#ef4444','#f472b6'];

  // Table
  $('expenseTbody').innerHTML = rows.map((r,i) => {
    const ch = Number(r.change||0);
    const clr = ch > 0 ? 'var(--red)' : ch < 0 ? 'var(--green)' : 'var(--mu)';
    return `<tr style="cursor:pointer" onclick="openExpenseModal('${r.category}',${r.amount},${r.pct})">
      <td><span style="display:inline-flex;align-items:center;gap:7px"><span style="width:9px;height:9px;border-radius:3px;background:${colors[i%colors.length]};display:inline-block"></span>${r.category}</span></td>
      <td class="mono">${fmtINR(r.amount)}</td>
      <td>${Number(r.pct||0).toFixed(1)}%</td>
      <td style="color:${clr};font-family:var(--fm)">${ch > 0 ? '↑ +' : ch < 0 ? '↓ ' : ''}${Math.abs(ch).toFixed(1)}%</td>
    </tr>`;
  }).join('');

  // Donut chart
  applyChartDefaults();
  destroyChart('expDonut');
  const ctx = $('expenseDonutChart').getContext('2d');
  chartInstances['expDonut'] = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: rows.map(r => r.category),
      datasets: [{
        data: rows.map(r => Number(r.pct||0)),
        backgroundColor: colors.map(c => c + 'bb'),
        borderColor: colors,
        borderWidth: 2,
        hoverOffset: 8,
      }]
    },
    options: {
      responsive:true, maintainAspectRatio:false, cutout:'60%',
      onClick: (e, elems) => { if(elems.length) openExpenseModal(rows[elems[0].index].category, rows[elems[0].index].amount, rows[elems[0].index].pct); },
      plugins: {
        legend: { position:'bottom', labels:{ boxWidth:10, font:{size:10} } },
        tooltip: { callbacks: { label: ctx => ` ${ctx.label}: ${ctx.raw.toFixed(1)}%` } }
      }
    }
  });
}

/* ===== INVENTORY ===== */
async function loadInventory() {
  try {
    renderInventory(await fetchJSON(apiUrl('/api/inventory-insights')));
  } catch(e) {
    renderInventory([
      {name:'Basmati Rice 5kg', category:'Grocery', current_stock:120, reorder_level:20, cost_price:280},
      {name:'Redmi Note 13', category:'Electronics', current_stock:8, reorder_level:10, cost_price:12000},
      {name:'Surf Excel 1kg', category:'Personal Care', current_stock:3, reorder_level:15, cost_price:180},
      {name:'Amul Butter 500g', category:'Dairy', current_stock:0, reorder_level:10, cost_price:280},
      {name:'Tata Salt 1kg', category:'Grocery', current_stock:200, reorder_level:50, cost_price:22},
    ]);
  }
}

function renderInventory(rows) {
  if(!rows.length) { $('inventoryTbody').innerHTML = '<tr><td colspan="9" class="empty-state">No inventory data.</td></tr>'; return; }
  $('inventoryTbody').innerHTML = rows.map((r,i) => {
    const stock = Number(r.current_stock||0);
    const reorder = Number(r.reorder_level||0);
    let status, cls;
    if(stock === 0) { status='Out of Stock'; cls='out'; }
    else if(stock <= reorder) { status='Low Stock'; cls='low'; }
    else { status='Healthy'; cls='healthy'; }
    const depth = reorder ? (stock/reorder).toFixed(1)+'x' : '∞';
    const daysLeft = reorder && stock > 0 ? Math.ceil(stock/reorder) : (stock > 0 ? '∞' : 0);
    const depthPct = Math.min(100, reorder ? (stock/reorder)*100 : 100);
    return `<tr>
      <td style="color:var(--mu);font:12px var(--fm)">${i+1}</td>
      <td style="font-weight:600;color:var(--tx)">${r.name}</td>
      <td><span style="font:11px var(--fm);color:var(--mu2)">${r.category||'—'}</span></td>
      <td class="mono">${stock}</td>
      <td style="font:12px var(--fm);color:var(--mu)">${reorder}</td>
      <td>
        <div style="width:80px">
          <div style="height:5px;border-radius:999px;background:rgba(255,255,255,.06);overflow:hidden">
            <div style="height:100%;width:${depthPct}%;background:${cls==='healthy'?'var(--g-green)':cls==='low'?'var(--g-amber)':'linear-gradient(135deg,#ef4444,#f472b6)'};border-radius:999px"></div>
          </div>
          <div style="font:10px var(--fm);color:var(--mu);margin-top:3px">${depth}</div>
        </div>
      </td>
      <td style="font:12px var(--fm);color:${cls==='out'?'var(--red)':'var(--tx2)'}">${daysLeft}</td>
      <td><span class="badge ${cls}">${status}</span></td>
      <td><button style="padding:4px 10px;border-radius:7px;font:700 10px var(--fm);background:rgba(0,212,255,.06);border:1px solid rgba(0,212,255,.18);color:var(--cyan);cursor:pointer" onclick="toast('Review triggered for ${r.name}','inf',2200)">Review</button></td>
    </tr>`;
  }).join('');
}

/* ===== DRILLDOWN MODALS ===== */
function openModal(title, kpis, subtitle, chartCfg, tableHtml='') {
  if(modalChartInstance) { modalChartInstance.destroy(); modalChartInstance = null; }
  $('modalTitle').innerHTML = title;
  $('modalSubtitle').textContent = subtitle;
  $('modalKPIs').innerHTML = kpis.map(k => `<div class="modal-kpi"><div class="modal-kpi-val">${k.val}</div><div class="modal-kpi-lbl">${k.lbl}</div></div>`).join('');
  $('modalTableWrap').innerHTML = tableHtml;
  $('drillModal').classList.add('show');
  setTimeout(() => {
    if(chartCfg) {
      applyChartDefaults();
      modalChartInstance = new Chart($('modalChart').getContext('2d'), chartCfg);
    }
  }, 60);
}

function closeModal() {
  $('drillModal').classList.remove('show');
  if(modalChartInstance) { modalChartInstance.destroy(); modalChartInstance = null; }
}

function openProductModal(row) {
  const rev = Number(row.revenue||0);
  openModal(
    `📦 ${row.name}`,
    [{val:fmtK(rev), lbl:'TOTAL REVENUE'},{val:Math.round(rev/500)+'', lbl:'EST. ORDERS'},{val:fmtK(rev/6), lbl:'MONTHLY AVG'}],
    'Revenue trend for this product over recent periods',
    {
      type:'bar',
      data:{ labels:['Oct','Nov','Dec','Jan','Feb','Mar'], datasets:[{label:'Revenue', data:[rev*.6,rev*.7,rev*.9,rev*.75,rev*.85,rev].map(v=>Math.round(v)), backgroundColor:'rgba(0,212,255,.5)', borderColor:'#00d4ff', borderWidth:1.5, borderRadius:5}] },
      options:{ responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}, scales:{x:{grid:{display:false}}, y:{ticks:{callback:v=>fmtK(v)}, grid:{color:getThemeColors().grid}}} }
    }
  );
}

function openCustomerModal(name, revenue, pct) {
  openModal(
    `👤 ${name}`,
    [{val:fmtK(revenue), lbl:'TOTAL REVENUE'},{val:pct+'%', lbl:'REVENUE SHARE'},{val:Math.round(revenue/500)+'', lbl:'EST. ORDERS'}],
    'Revenue trend for this customer',
    {
      type:'line',
      data:{ labels:['Oct','Nov','Dec','Jan','Feb','Mar'], datasets:[{label:'Revenue', data:[revenue*.5,revenue*.6,revenue*.8,revenue*.65,revenue*.75,revenue].map(v=>Math.round(v)), borderColor:'#5b8cff', backgroundColor:'rgba(91,140,255,.2)', fill:true, tension:0.4, borderWidth:2, pointRadius:3}] },
      options:{ responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}, scales:{x:{grid:{display:false}}, y:{ticks:{callback:v=>fmtK(v)}, grid:{color:getThemeColors().grid}}} }
    }
  );
}

function openExpenseModal(category, amount, pct) {
  openModal(
    `💳 ${category}`,
    [{val:fmtINR(amount), lbl:'TOTAL AMOUNT'},{val:pct.toFixed(1)+'%', lbl:'OF TOTAL EXPENSES'},{val:fmtK(amount/6), lbl:'MONTHLY AVG'}],
    'Monthly expense trend for this category',
    {
      type:'bar',
      data:{ labels:['Oct','Nov','Dec','Jan','Feb','Mar'], datasets:[{label:'Expense', data:[amount*.7,amount*.8,amount*.9,amount*.85,amount*.95,amount].map(v=>Math.round(v)), backgroundColor:'rgba(245,158,11,.5)', borderColor:'#f59e0b', borderWidth:1.5, borderRadius:5}] },
      options:{ responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}, scales:{x:{grid:{display:false}}, y:{ticks:{callback:v=>fmtINR(v)}, grid:{color:getThemeColors().grid}}} }
    }
  );
}

// Close modal on overlay click
$('drillModal').addEventListener('click', e => { if(e.target === $('drillModal')) closeModal(); });

/* ===== EXPORT ===== */
function exportData() {
  toast('Generating export… (Connect to /api/export for production)','inf',3000);
}

/* ===== REFRESH ===== */
async function refreshAll() {
  await reloadAnalytics();
}

/* ===== INIT ===== */
document.addEventListener('DOMContentLoaded', function() {
  updateActiveRangeButton();
  updatePeriodLabel();
  reloadAnalytics();
  setInterval(() => { reloadAnalytics(); }, 60000);
});
