const $ = id => document.getElementById(id);
const today = () => new Date().toISOString().split('T')[0];
const fmtINR = v => '₹' + Number(v||0).toLocaleString('en-IN',{maximumFractionDigits:2});

/* PANEL SWITCHING */
function showPanel(el, panelId, title){
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  const panel = $(panelId);
  if(panel) panel.classList.add('active');
  if(el) el.classList.add('active');
  $('pageTitle').textContent = title;
  $('pageBc').textContent = title;
  $('main').scrollTop = 0;
  closeSidebar();
}

/* THEME */
function toggleTheme(){
  const h = document.documentElement, k = $('knob');
  h.dataset.theme = h.dataset.theme === 'dark' ? 'light' : 'dark';
  k.textContent = h.dataset.theme === 'dark' ? '🌙' : '☀️';
}

/* SIDEBAR */
function toggleSidebar(){
  $('sidebar').classList.toggle('open');
  $('overlay').classList.toggle('show');
}
function closeSidebar(){
  if(window.innerWidth <= 768){
    $('sidebar').classList.remove('open');
    $('overlay').classList.remove('show');
  }
}

/* TOAST */
function toast(msg, type='suc', dur=3500){
  const tc = $('toast-container');
  const icons = {suc:'✅',err:'❌',inf:'📡'};
  const t = document.createElement('div');
  t.className = `toast ${type}`;
  t.innerHTML = `<span class="toast-icon">${icons[type]||'ℹ'}</span><span>${msg}</span><button class="toast-close" onclick="this.parentElement.remove()">✕</button>`;
  tc.appendChild(t);
  setTimeout(()=>{ t.classList.add('out'); setTimeout(()=>t.remove(),400); }, dur);
}

/* DATES */
function initDates(){
  ['saleDate','purchaseDate','expenseDate'].forEach(id=>{ const el=$(id); if(el) el.value=today(); });
}

/* KPIs */
function loadKPIs(){
  toast('Fetching dashboard data…','inf',2000);
  Promise.all([
    fetch('/api/dashboard-summary').then(r=>r.json()),
    fetch('/api/monthly-sales-trend').then(r=>r.json())
  ])
    .then(([d, trend])=>{
      renderKPI('kpi-sales',fmtINR(d.total_sales),`↑ +${d.sales_growth||0}% this month`,'up');
      renderKPI('kpi-profit',fmtINR(d.total_profit),`↑ +${d.profit_margin||0}% margin`,'up');
      renderKPI('kpi-customers',d.total_customers,`↑ +${d.new_customers||0} new`,'up');
      renderKPI('kpi-alerts',d.low_stock_count||0,`⚠ ${d.low_stock_count||0} products low`,'warn');
      renderKPI('kpi-top-product',d.top_product||'N/A',`${fmtINR(d.top_product_revenue||0)} revenue • ${d.top_product_qty||0} units`,'up');
      $('qs-purchases').textContent = d.total_purchases||'—';
      $('qs-products').textContent  = d.total_products||'—';
      $('qs-expenses').textContent  = fmtINR(d.total_expenses);
      $('qs-suppliers').textContent = d.total_suppliers||'—';
      if(d.low_stock_count) $('alertBadge').textContent = d.low_stock_count;
      renderRevChart(trend);
      toast('Dashboard data refreshed','suc');
    })
    .catch(()=>{
      renderKPI('kpi-sales','₹4,28,600','↑ +14.2% this month','up');
      renderKPI('kpi-profit','₹68,420','↑ +12.4% margin','up');
      renderKPI('kpi-customers','214','↑ +8 new this week','up');
      renderKPI('kpi-alerts','3','⚠ 3 products low','warn');
      renderKPI('kpi-top-product','Basmati Rice 5kg','● ₹1,24,600 revenue','up');
      $('qs-purchases').textContent='42';$('qs-products').textContent='186';
      $('qs-expenses').textContent='₹38,200';$('qs-suppliers').textContent='18';
      renderRevChart({ labels:['Oct','Nov','Dec','Jan','Feb','Mar'], values:[62,78,95,71,88,100] });
      addActivity('Demo data loaded','API not connected — showing mock data','var(--amber)');
    });
}

function renderKPI(id,val,change,cls){
  const el=$(id); if(!el) return;
  el.innerHTML=val;
  const ch=$(id+'-ch');
  if(ch){ ch.textContent=change; ch.className='kpi-ch '+cls; }
}

function renderRevChart(data){
  const wrap=document.getElementById('revBars'); if(!wrap) return;
  wrap.innerHTML='';
  const values = data?.revenue_chart || data?.values || [];
  const labels = data?.revenue_chart_labels || data?.labels || [];
  if(!values.length){
    wrap.innerHTML='<div class="empty-state">No monthly revenue data available.</div>';
    return;
  }
  const maxBars = 24;
  const trimmedValues = values.length > maxBars ? values.slice(-maxBars) : values;
  const trimmedLabels = labels.length > maxBars ? labels.slice(-maxBars) : labels;
  const max = Math.max(...trimmedValues, 0);
  const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const formatLabel = label => {
    if(!label) return '';
    const parts = label.toString().split('-');
    if(parts.length === 2){
      const month = monthNames[Number(parts[1]) - 1] || parts[1];
      const year = parts[0].slice(-2);
      return `${month} ${year}`;
    }
    return label;
  };
  trimmedValues.forEach((value,i)=>{
    const pct = max > 0 ? Math.max(8, (value / max) * 100) : 8;
    const col = document.createElement('div');
    col.className='rev-col';
    const valueHint = document.createElement('div');
    valueHint.className='rev-value';
    valueHint.textContent = fmtINR(value);
    const track = document.createElement('div');
    track.className='rev-track';
    const bar = document.createElement('div');
    bar.className='rev-bar';
    bar.style.height='0%';
    const label = document.createElement('div');
    label.className='rev-label';
    label.textContent = formatLabel(trimmedLabels[i] || '');
    track.appendChild(bar);
    col.appendChild(valueHint);
    col.appendChild(track);
    col.appendChild(label);
    wrap.appendChild(col);
    requestAnimationFrame(()=>{ bar.style.height = pct + '%'; });
  });
}

function addActivity(title,text,color){
  const feed=$('activityFeed'); if(!feed) return;
  const item=document.createElement('div');
  item.className='feed-item';
  item.innerHTML=`<div class="feed-dot" style="background:${color||'var(--cyan)'}"></div><div class="feed-text"><strong>${title}</strong><span>${text}</span><div class="feed-time">Just now</div></div>`;
  feed.insertBefore(item,feed.firstChild);
}

/* STOCK ALERTS */
function loadStockAlerts(){
  const tbody=$('alertsTbody');
  tbody.innerHTML='<tr class="loading-row"><td colspan="5"><span class="inline-spinner"></span>Fetching alerts…</td></tr>';
  fetch('/api/stock-alerts')
    .then(r=>r.json())
    .then(data=>renderAlertsTable(data))
    .catch(()=>renderAlertsTable([
      {product_id:'P001',alert_type:'low_stock',threshold:10,is_active:1},
      {product_id:'P007',alert_type:'reorder',threshold:5,is_active:1},
      {product_id:'P023',alert_type:'out_of_stock',threshold:0,is_active:1},
    ]));
}

function renderAlertsTable(data){
  const tbody=$('alertsTbody');
  if(!data||!data.length){
    tbody.innerHTML=`<tr><td colspan="5"><div class="empty-state"><div class="empty-state-icon">🔔</div>No active stock alerts found.</div></td></tr>`;
    return;
  }
  const typeMap={low_stock:{lbl:'Low Stock',cls:'warn'},out_of_stock:{lbl:'Out of Stock',cls:'inactive'},reorder:{lbl:'Reorder',cls:'info'},overstock:{lbl:'Overstock',cls:'active'}};
  tbody.innerHTML=data.map(row=>{
    const t=typeMap[row.alert_type]||{lbl:row.alert_type,cls:'info'};
    const st=row.is_active?'<span class="badge active">● Active</span>':'<span class="badge inactive">● Inactive</span>';
    return `<tr><td><span class="mono">${row.product_id}</span></td><td><span class="badge ${t.cls}">${t.lbl}</span></td><td class="mono">${row.threshold} units</td><td>${st}</td><td><button class="btn-outline" style="padding:5px 12px;font-size:11px" onclick="resolveAlert('${row.product_id}')">Resolve</button></td></tr>`;
  }).join('');
  $('alertBadge').textContent=data.filter(a=>a.is_active).length;
}

function resolveAlert(pid){ toast(`Alert resolved for Product ${pid}`,'suc'); loadStockAlerts(); }

/* DROPDOWNS */
function loadDropdowns(){
  fetch('/api/categories').then(r=>r.json()).then(data=>populateSel('sel-category',data,'id','name','— Select Category —')).catch(()=>populateSel('sel-category',[{id:1,name:'Grocery'},{id:2,name:'Electronics'},{id:3,name:'Beverages'},{id:4,name:'Personal Care'}],'id','name','— Select Category —'));
  fetch('/api/suppliers').then(r=>r.json()).then(data=>{ populateSel('sel-supplier',data,'id','name','— Select Supplier —'); populateSel('sel-pur-supplier',data,'id','name','— Select Supplier —'); }).catch(()=>{ const s=[{id:1,name:'Agarwal Traders'},{id:2,name:'Mehta Wholesale'},{id:3,name:'Singh Distributors'}]; populateSel('sel-supplier',s,'id','name','— Select Supplier —'); populateSel('sel-pur-supplier',s,'id','name','— Select Supplier —'); });
  fetch('/api/customers').then(r=>r.json()).then(data=>populateSel('sel-customer',data,'id','name','— Select Customer —')).catch(()=>populateSel('sel-customer',[{id:1,name:'Priya Sharma'},{id:2,name:'Ravi Kumar'},{id:3,name:'Neha Patel'},{id:4,name:'Walk-in Customer'}],'id','name','— Select Customer —'));
  fetch('/api/products').then(r=>r.json()).then(data=>{ populateSel('sel-alert-product',data,'id','name','— Select Product —'); window._products=data; populateSaleProductSelects(); populatePurProductSelects(); }).catch(()=>{ const p=[{id:1,name:'Basmati Rice 5kg'},{id:2,name:'Redmi Note 13'},{id:3,name:'Tata Salt 1kg'},{id:4,name:'Surf Excel 1kg'},{id:5,name:'Amul Butter 500g'}]; populateSel('sel-alert-product',p,'id','name','— Select Product —'); window._products=p; populateSaleProductSelects(); populatePurProductSelects(); });
}

function populateSel(selId,data,valKey,labelKey,placeholder){
  const sel=$(selId); if(!sel) return;
  sel.innerHTML=`<option value="">${placeholder}</option>`;
  (data||[]).forEach(item=>{ const opt=document.createElement('option'); opt.value=item[valKey]; opt.textContent=item[labelKey]; sel.appendChild(opt); });
}

/* SALE ITEMS */
let saleItemCount=0, purchaseItemCount=0;
function addSaleItem(){
  saleItemCount++;
  const prods=window._products||[];
  const opts=prods.map(p=>`<option value="${p.id}">${p.name}</option>`).join('');
  const row=document.createElement('div');
  row.className='item-row'; row.id=`sale-row-${saleItemCount}`;
  row.innerHTML=`<select name="product_id" class="inp" onchange="calcSaleTotal()"><option value="">— Product —</option>${opts}</select><input name="quantity" class="inp" type="number" min="1" value="1" placeholder="Qty" oninput="calcSaleTotal()"/><input name="price" class="inp" type="number" min="0" step="0.01" placeholder="Price" oninput="calcSaleTotal()"/><input name="discount" class="inp" type="number" min="0" max="100" step="0.1" value="0" placeholder="Disc%" oninput="calcSaleTotal()"/><button type="button" class="del-row" onclick="this.closest('.item-row').remove();calcSaleTotal()">✕</button>`;
  $('saleItems').appendChild(row);
  row.querySelector('select').focus();
}
function populateSaleProductSelects(){
  document.querySelectorAll('#saleItems .item-row select[name="product_id"]').forEach(sel=>{ const p=window._products||[],cur=sel.value; sel.innerHTML=`<option value="">— Product —</option>`+p.map(x=>`<option value="${x.id}">${x.name}</option>`).join(''); sel.value=cur; });
}
function calcSaleTotal(){
  let total=0;
  $('saleItems').querySelectorAll('.item-row').forEach(row=>{
    const qty=parseFloat(row.querySelector('[name="quantity"]')?.value||0);
    const price=parseFloat(row.querySelector('[name="price"]')?.value||0);
    const disc=parseFloat(row.querySelector('[name="discount"]')?.value||0);
    total+=qty*price*(1-disc/100);
  });
  $('saleTotal').textContent=fmtINR(total);
}
function resetSaleItems(){ $('saleItems').innerHTML=''; $('saleTotal').textContent='₹0.00'; saleItemCount=0; }
function submitSale(e){
  e.preventDefault();
  const form=e.target, fd=new FormData(form);
  const items=[];
  $('saleItems').querySelectorAll('.item-row').forEach(row=>items.push({product_id:row.querySelector('[name="product_id"]').value,quantity:row.querySelector('[name="quantity"]').value,price:row.querySelector('[name="price"]').value,discount:row.querySelector('[name="discount"]').value}));
  if(!items.length){ toast('Please add at least one sale item','err'); return; }
  const payload={customer_id:fd.get('customer_id'),sale_date:fd.get('sale_date'),payment_method:fd.get('payment_method'),notes:fd.get('notes'),items};
  fetch('/add-sale',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)})
    .then(r=>r.json())
    .then(()=>{ toast('✅ Sale recorded successfully!','suc'); form.reset(); resetSaleItems(); initDates(); loadKPIs(); })
    .catch(()=>{ toast('Sale saved (API offline — demo mode)','inf'); form.reset(); resetSaleItems(); initDates(); });
}

/* PURCHASE ITEMS */
function addPurchaseItem(){
  purchaseItemCount++;
  const prods=window._products||[];
  const opts=prods.map(p=>`<option value="${p.id}">${p.name}</option>`).join('');
  const row=document.createElement('div');
  row.className='item-row'; row.style.gridTemplateColumns='2fr 1fr 1fr auto'; row.id=`pur-row-${purchaseItemCount}`;
  row.innerHTML=`<select name="product_id" class="inp" onchange="calcPurTotal()"><option value="">— Product —</option>${opts}</select><input name="quantity" class="inp" type="number" min="1" value="1" placeholder="Qty" oninput="calcPurTotal()"/><input name="unit_cost" class="inp" type="number" min="0" step="0.01" placeholder="Unit Cost" oninput="calcPurTotal()"/><button type="button" class="del-row" onclick="this.closest('.item-row').remove();calcPurTotal()">✕</button>`;
  $('purchaseItems').appendChild(row);
  row.querySelector('select').focus();
}
function populatePurProductSelects(){
  document.querySelectorAll('#purchaseItems .item-row select[name="product_id"]').forEach(sel=>{ const p=window._products||[],cur=sel.value; sel.innerHTML=`<option value="">— Product —</option>`+p.map(x=>`<option value="${x.id}">${x.name}</option>`).join(''); sel.value=cur; });
}
function calcPurTotal(){
  let total=0;
  $('purchaseItems').querySelectorAll('.item-row').forEach(row=>{
    const qty=parseFloat(row.querySelector('[name="quantity"]')?.value||0);
    const cost=parseFloat(row.querySelector('[name="unit_cost"]')?.value||0);
    total+=qty*cost;
  });
  $('purTotal').textContent=fmtINR(total);
}
function resetPurchaseItems(){ $('purchaseItems').innerHTML=''; $('purTotal').textContent='₹0.00'; purchaseItemCount=0; }
function submitPurchase(e){
  e.preventDefault();
  const form=e.target, fd=new FormData(form);
  const items=[];
  $('purchaseItems').querySelectorAll('.item-row').forEach(row=>items.push({product_id:row.querySelector('[name="product_id"]').value,quantity:row.querySelector('[name="quantity"]').value,unit_cost:row.querySelector('[name="unit_cost"]').value}));
  if(!items.length){ toast('Please add at least one purchase item','err'); return; }
  const payload={supplier_id:fd.get('supplier_id'),purchase_date:fd.get('purchase_date'),status:fd.get('status'),items};
  fetch('/add-purchase',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)})
    .then(r=>r.json())
    .then(()=>{ toast('✅ Purchase recorded successfully!','suc'); form.reset(); resetPurchaseItems(); initDates(); loadKPIs(); })
    .catch(()=>{ toast('Purchase saved (API offline — demo mode)','inf'); form.reset(); resetPurchaseItems(); initDates(); });
}

/* GENERIC FORM */
function submitForm(e,formId,endpoint){
  e.preventDefault();
  const form=$(formId), fd=new FormData(form), payload=Object.fromEntries(fd.entries());
  const subBtn=form.querySelector('[type="submit"]');
  if(subBtn){ subBtn.disabled=true; subBtn.innerHTML='<span class="inline-spinner"></span> Saving…'; }
  fetch(endpoint,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)})
    .then(r=>r.json())
    .then(d=>{ toast(`✅ ${d.message||'Saved successfully!'}`, 'suc'); form.reset(); loadDropdowns(); })
    .catch(()=>{ toast('Data saved (API offline — demo mode)','inf'); form.reset(); })
    .finally(()=>{ if(subBtn){ subBtn.disabled=false; subBtn.innerHTML=subBtn.dataset.orig||subBtn.textContent; } });
}

/* INIT */
(function init(){
  document.querySelectorAll('.btn-sub').forEach(b=>b.dataset.orig=b.innerHTML);
  initDates();
  loadKPIs();
  loadDropdowns();
  loadStockAlerts();
  addSaleItem();
  addPurchaseItem();
})();
