/**
 * ThermalMatch — Corporate B2B Dashboard Controller (INR ₹ Edition)
 * Powered by Antigravity framework, Amazon Kinesis Data Streams, and Google Gemini AI.
 */

// Application State
const state = {
  facilities: [],
  matches: [],
  shards: [],
  summary: null,
  selectedZone: 'all',
  selectedGrade: 'all',
  searchQuery: '',
  selectedRouteIndex: 0,
  currentViewMode: 'table', // 'table' or 'map'
  isQueryingGemini: false
};

// Map Projection Bounds (Regional Industrial Corridor)
const MAP_BOUNDS = {
  minLat: 18.670,
  maxLat: 18.785,
  minLng: 73.700,
  maxLng: 73.850,
  width: 1200,
  height: 600,
  padding: 80
};

// Currency Formatter for Indian Rupees (₹)
function formatINR(amount, compact = false) {
  if (!amount || isNaN(amount)) return '₹0';
  const val = Math.round(amount);
  
  if (compact) {
    if (val >= 10000000) {
      return `₹${(val / 10000000).toFixed(2)} Cr`;
    } else if (val >= 100000) {
      return `₹${(val / 100000).toFixed(2)} Lakh`;
    }
  }

  // Indian number comma format
  const str = val.toString();
  const lastThree = str.substring(str.length - 3);
  const otherNumbers = str.substring(0, str.length - 3);
  if (otherNumbers !== '') {
    return '₹' + otherNumbers.replace(/\B(?=(\d{2})+(?!\d))/g, ',') + ',' + lastThree;
  }
  return '₹' + lastThree;
}

// Project GPS coordinates to SVG canvas
function projectGpsToSvg(lat, lng) {
  const xRatio = (lng - MAP_BOUNDS.minLng) / (MAP_BOUNDS.maxLng - MAP_BOUNDS.minLng);
  const yRatio = (lat - MAP_BOUNDS.minLat) / (MAP_BOUNDS.maxLat - MAP_BOUNDS.minLat);
  const x = MAP_BOUNDS.padding + xRatio * (MAP_BOUNDS.width - 2 * MAP_BOUNDS.padding);
  const y = (MAP_BOUNDS.height - MAP_BOUNDS.padding) - yRatio * (MAP_BOUNDS.height - 2 * MAP_BOUNDS.padding);
  return { x: Math.round(x), y: Math.round(y) };
}

// =========================================================================
// 1. INITIALIZATION & LIVE TICKERS
// =========================================================================

document.addEventListener('DOMContentLoaded', async () => {
  initTheme();
  initUserSession();
  startLiveClock();
  renderMapBackground();
  await loadInitialData();
  startLiveTelemetryPolling();
});

function startLiveClock() {
  function updateTime() {
    const clockEl = document.getElementById('live-clock-text');
    if (!clockEl) return;
    const now = new Date();
    let hours = now.getHours();
    const minutes = now.getMinutes().toString().padStart(2, '0');
    const ampm = hours >= 12 ? 'PM' : 'AM';
    hours = hours % 12;
    hours = hours ? hours : 12;
    clockEl.textContent = `${hours}:${minutes} ${ampm}`;
  }
  updateTime();
  setInterval(updateTime, 1000);
}

async function loadInitialData() {
  try {
    const [facRes, matchesRes, summaryRes, shardsRes] = await Promise.all([
      fetch('/api/facilities'),
      fetch('/api/matches?limit=15'),
      fetch('/api/summary'),
      fetch('/api/kinesis/stream')
    ]);

    state.facilities = await facRes.json();
    state.matches = await matchesRes.json();
    state.summary = await summaryRes.json();
    state.shards = await shardsRes.json();

    applyGlobalFilter();
  } catch (err) {
    console.error('Error loading initial dashboard data:', err);
  }
}

function startLiveTelemetryPolling() {
  setInterval(async () => {
    try {
      const [sumRes, shardsRes, liveRes] = await Promise.all([
        fetch('/api/summary'),
        fetch('/api/kinesis/stream'),
        fetch('/api/kinesis/telemetry/live')
      ]);

      const sumData = await sumRes.json();
      const shardsData = await shardsRes.json();
      const liveData = await liveRes.json();

      state.summary = sumData;
      state.shards = shardsData;

      updateFacilityTemperatures(liveData);
      if (state.selectedGrade === 'all' && state.selectedZone === 'all' && !state.searchQuery) {
        updateMetricCards();
      }
    } catch (e) {
      console.warn('Live telemetry polling tick:', e);
    }
  }, 2500);
}

function updateFacilityTemperatures(liveRecords) {
  liveRecords.forEach(rec => {
    const fac = state.facilities.find(f => f.id === rec.facility_id);
    if (fac) {
      fac.temperature_c = rec.exhaust_temp_c;
      const badgeText = document.getElementById(`temp-badge-text-${fac.id}`);
      if (badgeText) {
        badgeText.textContent = `${rec.exhaust_temp_c.toFixed(1)}°C`;
      }
    }
  });
}

// =========================================================================
// 2. EXCHANGE PIPELINE REPOSITORY & REACTIVE FILTERS (INR ₹)
// =========================================================================

// High-fidelity industrial pipeline pairings in Indian Rupees (₹)
const INDUSTRIAL_PIPELINES = [
  {
    id: 'pipe-chakan-talegaon',
    source: 'Company A',
    sourceId: 'PROD-01',
    sourceType: 'Data Center Liquid Cooling',
    sourceTemp: 82.0,
    sink: 'Company F',
    sinkId: 'CONS-01',
    sinkType: 'Greenhouse Canopy Heating',
    sinkTemp: 62.0,
    zone: 'Regional Tech to Agro Belt',
    zoneKey: 'Tech Sector',
    distance: '1.85 km (DN150)',
    tempProfile: '82°C ➔ 62°C',
    deliveredTemp: '79.8°C',
    flux: '5.2 MWt',
    fluxMWt: 5.2,
    annualSavingsInr: 36338400, // ₹3.63 Cr
    hourlySavingsInr: 41500,
    paybackYrs: '4.38 Yrs',
    tco2eYr: 7842,
    grade: 'low' // Consumer 62°C is Low Grade (< 75°C)
  },
  {
    id: 'pipe-bhosari-chakan',
    source: 'Company B',
    sourceId: 'PROD-02',
    sourceType: 'Induction Arc Melting',
    sourceTemp: 260.0,
    sink: 'Company H',
    sinkId: 'CONS-03',
    sinkType: 'Municipal Hydronic Loop',
    sinkTemp: 78.0,
    zone: 'Regional Smelting to City Loop',
    zoneKey: 'Smelting Sector',
    distance: '3.40 km (DN300)',
    tempProfile: '260°C ➔ 78°C',
    deliveredTemp: '254.2°C',
    flux: '24.5 MWt',
    fluxMWt: 24.5,
    annualSavingsInr: 142850000, // ₹14.28 Cr
    hourlySavingsInr: 163000,
    paybackYrs: '3.12 Yrs',
    tco2eYr: 34120,
    grade: 'high' // Arc Furnace Steam 260°C is High Grade (> 75°C)
  },
  {
    id: 'pipe-pimpri-agro',
    source: 'Company C',
    sourceId: 'PROD-03',
    sourceType: 'Melting Tank Flue Gas',
    sourceTemp: 195.0,
    sink: 'Company G',
    sinkId: 'CONS-02',
    sinkType: 'Agricultural Grain Drying',
    sinkTemp: 74.0,
    zone: 'Regional Manufacturing to Agro Hub',
    zoneKey: 'Manufacturing Hub',
    distance: '2.15 km (DN200)',
    tempProfile: '195°C ➔ 74°C',
    deliveredTemp: '190.5°C',
    flux: '11.8 MWt',
    fluxMWt: 11.8,
    annualSavingsInr: 78540000, // ₹7.85 Cr
    hourlySavingsInr: 89700,
    paybackYrs: '3.65 Yrs',
    tco2eYr: 18450,
    grade: 'low' // Consumer drying temp 74°C is Low Grade (< 75°C)
  },
  {
    id: 'pipe-kurkumbh-chemical',
    source: 'Company D',
    sourceId: 'PROD-04',
    sourceType: 'Fractionation Reboiler',
    sourceTemp: 145.0,
    sink: 'Company I',
    sinkId: 'CONS-04',
    sinkType: 'Feedstock Catalytic Pre-Heating',
    sinkTemp: 130.0,
    zone: 'Regional Chemical Industrial Belt',
    zoneKey: 'Chemical Belt',
    distance: '1.45 km (DN250)',
    tempProfile: '145°C ➔ 130°C',
    deliveredTemp: '142.1°C',
    flux: '28.5 MWt',
    fluxMWt: 28.5,
    annualSavingsInr: 184500000, // ₹18.45 Cr
    hourlySavingsInr: 210600,
    paybackYrs: '2.84 Yrs',
    tco2eYr: 41820,
    grade: 'high' // Chemical pre-heating 130°C is High Grade (> 75°C)
  },
  {
    id: 'pipe-talegaon-paper',
    source: 'Company E',
    sourceId: 'PROD-05',
    sourceType: 'Mash Wash Enthalpy Recovery',
    sink: 'Company J',
    sinkId: 'CONS-05',
    sinkType: 'Paper Drying Tunnel',
    sinkTemp: 115.0,
    zone: 'Regional Agro to Packaging Mills',
    zoneKey: 'Agro Belt',
    distance: '1.20 km (DN125)',
    tempProfile: '94°C ➔ 115°C',
    deliveredTemp: '91.8°C',
    flux: '4.8 MWt',
    fluxMWt: 4.8,
    annualSavingsInr: 26712000, // ₹2.67 Cr
    hourlySavingsInr: 30500,
    paybackYrs: '4.62 Yrs',
    tco2eYr: 6314,
    grade: 'low' // Biological mash condensation heat recovery
  }
];

function getFilteredPipelines() {
  return INDUSTRIAL_PIPELINES.filter(p => {
    if (state.selectedZone !== 'all' && !p.zone.includes(state.selectedZone) && p.zoneKey !== state.selectedZone) return false;
    if (state.selectedGrade !== 'all' && p.grade !== state.selectedGrade) return false;
    if (state.searchQuery) {
      const q = state.searchQuery.toLowerCase();
      const match = p.source.toLowerCase().includes(q) ||
                    p.sink.toLowerCase().includes(q) ||
                    p.zone.toLowerCase().includes(q) ||
                    p.tempProfile.toLowerCase().includes(q);
      if (!match) return false;
    }
    return true;
  });
}

function applyGlobalFilter() {
  renderExchangeDirectoryTable();
  renderMapElements();
  updateHourlyChart();
  updateMetricCards();
  renderQuickSightCharts();
}

function updateHourlyChart() {
  const activePipes = getFilteredPipelines();
  const totalFlux = activePipes.reduce((acc, p) => acc + p.fluxMWt, 0);
  const timeLabels = ['12:00 PM', '12:15 PM', '12:30 PM', '12:45 PM', '1:00 PM', '1:15 PM', '1:30 PM', '1:45 PM', '2:00 PM', '2:15 PM', '2:30 PM', '2:45 PM', '3:00 PM'];
  
  let values = [];
  let calloutText = '';

  if (state.selectedGrade === 'low') {
    // Low Grade (< 75°C): agricultural & greenhouse recovery profile
    values = [7.5, 9.2, 11.0, 12.8, 14.5, 16.8, 18.5, 20.2, 21.8, 20.0, 17.4, 14.2, 12.0];
    calloutText = `Live: Low-grade agricultural dispatch peak at 2:00 PM (${totalFlux.toFixed(1)} MWt)`;
  } else if (state.selectedGrade === 'high') {
    // High Grade (> 75°C): metallurgy & chemical steam recovery profile
    values = [18.0, 22.0, 26.5, 30.5, 34.0, 39.5, 44.0, 48.5, 53.0, 49.0, 43.5, 35.0, 31.0];
    calloutText = `Live: High-grade metallurgy & steam dispatch peak at 2:00 PM (${totalFlux.toFixed(1)} MWt)`;
  } else {
    // All Temperature Grades
    values = [28, 34, 40, 45, 50, 58, 64, 72, 82, 75, 66, 54, 48];
    calloutText = `Live: Optimal district thermal dispatch window at 2:00 PM (82.0 MWt Peak)`;
  }

  // If a specific zone is selected, scale values proportionally
  if (state.selectedZone !== 'all') {
    const scaleFactor = Math.max(0.2, totalFlux / 74.8);
    values = values.map(v => Math.max(1.5, Math.round(v * scaleFactor * 10) / 10));
    calloutText = `Live: ${state.selectedZone} thermal dispatch peak at 2:00 PM (${Math.max(...values).toFixed(1)} MWt)`;
  }

  const maxVal = Math.max(...values, 1);
  const track = document.getElementById('hourly-bars-track');
  if (track) {
    track.innerHTML = values.map((val, idx) => {
      const isPeak = idx === 8;
      const isLow = val < (maxVal * 0.45);
      const heightPct = Math.min(100, Math.round((val / maxVal) * 94));
      const pillClass = isPeak ? 'bar-pill peak' : (isLow ? 'bar-pill low' : 'bar-pill');
      return `
        <div class="bar-col" title="${timeLabels[idx]}: ${val.toFixed(1)} MWt ${isPeak ? '(Peak Recovery)' : ''}">
          <div class="${pillClass}" style="height: ${heightPct}%; transition: height 0.4s cubic-bezier(0.16, 1, 0.3, 1);"></div>
        </div>
      `;
    }).join('');
  }

  const calloutEl = document.getElementById('chart-recovery-callout-text');
  if (calloutEl) {
    calloutEl.textContent = calloutText;
  }
}

function updateMetricCards() {
  const activePipes = getFilteredPipelines();
  const totalFlux = activePipes.reduce((acc, p) => acc + p.fluxMWt, 0);
  const totalHourlySavings = activePipes.reduce((acc, p) => acc + p.hourlySavingsInr, 0);
  const totalAnnualSavings = activePipes.reduce((acc, p) => acc + p.annualSavingsInr, 0);
  const totalTco2 = activePipes.reduce((acc, p) => acc + p.tco2eYr, 0);

  const activeProducerIds = new Set(activePipes.map(p => p.sourceId));
  const activeConsumerIds = new Set(activePipes.map(p => p.sinkId));

  // 1. Donut Number
  const donutNumEl = document.getElementById('donut-number');
  if (donutNumEl) {
    donutNumEl.textContent = totalFlux > 0 ? totalFlux.toFixed(1) : '0.0';
  }

  // 2. Donut Progress Ring
  const ringEl = document.getElementById('donut-progress-ring');
  if (ringEl) {
    const circumference = 2 * Math.PI * 32; // ~201
    const pct = totalFlux > 0 ? Math.min(100, (totalFlux / 75.0) * 100) : 10;
    const offset = circumference - (pct / 100) * circumference;
    ringEl.style.strokeDasharray = `${circumference}`;
    ringEl.style.strokeDashoffset = `${offset}`;
  }

  // 3. Hourly Displacement Callout
  const hourlyCalloutEl = document.getElementById('live-hourly-savings-callout');
  if (hourlyCalloutEl) {
    const fuelType = state.selectedGrade === 'low' ? 'agricultural boiler' : (state.selectedGrade === 'high' ? 'metallurgical & chemical' : 'natural gas & coal');
    hourlyCalloutEl.textContent = `Displacing ${formatINR(totalHourlySavings)} / hr in ${fuelType} fuel costs`;
  }

  // 4. Producers & Consumers Status Counts
  const prodText = document.getElementById('status-text-producers');
  if (prodText) {
    const prodTempRange = state.selectedGrade === 'low' ? '82°C – 94°C' : (state.selectedGrade === 'high' ? '145°C – 260°C' : '82°C – 260°C');
    prodText.innerHTML = `<strong>${activeProducerIds.size}/5 Producers</strong> active (${prodTempRange})`;
  }

  const consText = document.getElementById('status-text-consumers');
  if (consText) {
    const consTempRange = state.selectedGrade === 'low' ? '62°C – 74°C' : (state.selectedGrade === 'high' ? '78°C – 130°C' : '62°C – 130°C');
    consText.innerHTML = `<strong>${activeConsumerIds.size}/5 Consumers</strong> active (${consTempRange})`;
  }

  // 5. Capacity Badge Pill
  const capBadge = document.getElementById('capacity-badge-pill');
  if (capBadge) {
    if (state.selectedGrade === 'low') {
      capBadge.textContent = 'Low-Grade (< 75°C) Active';
      capBadge.style.background = '#e6f8f0';
      capBadge.style.color = '#00a86b';
    } else if (state.selectedGrade === 'high') {
      capBadge.textContent = 'High-Grade (> 75°C) Active';
      capBadge.style.background = '#fff4eb';
      capBadge.style.color = '#f97316';
    } else {
      capBadge.textContent = 'Capacity Available';
      capBadge.style.background = '#e6f8f0';
      capBadge.style.color = '#00a86b';
    }
  }

  // 6. Savings Card Heading & Desc
  const alertHeading = document.getElementById('alert-savings-heading');
  const alertDesc = document.getElementById('alert-savings-desc');
  if (alertHeading) {
    alertHeading.textContent = state.selectedGrade === 'low' ? 'Low-Grade Canopy Savings Active!' : (state.selectedGrade === 'high' ? 'High-Enthalpy Steam Savings Active!' : 'Peak Savings Active!');
  }
  if (alertDesc) {
    alertDesc.innerHTML = `Recovering ${state.selectedGrade === 'low' ? 'low-grade waste heat' : (state.selectedGrade === 'high' ? 'high-grade steam' : 'waste heat')} saves up to <strong>${formatINR(totalAnnualSavings, true)} / yr</strong> across ${activePipes.length} active connection${activePipes.length === 1 ? '' : 's'}.`;
  }

  // 7. ESG Carbon Card
  const ecoDesc = document.getElementById('eco-carbon-desc');
  if (ecoDesc) {
    const creditVal = totalAnnualSavings > 0 ? totalAnnualSavings * 1.45 : 684000000;
    ecoDesc.innerHTML = `<strong>${totalTco2.toLocaleString()} tCO₂e / yr</strong> avoided. Eligible for <strong>${formatINR(creditVal, true)}</strong> in verified carbon credits under the Green Heat Protocol.`;
  }

  // 8. Map Floating Filter Pill
  const mapPillText = document.getElementById('map-filter-pill-text');
  if (mapPillText) {
    const gradeName = state.selectedGrade === 'low' ? 'Low Grade (< 75°C)' : (state.selectedGrade === 'high' ? 'High Grade (> 75°C)' : 'All Temperature Grades');
    const zoneName = state.selectedZone !== 'all' ? ` · ${state.selectedZone}` : '';
    mapPillText.textContent = `${gradeName}${zoneName} · ${activePipes.length} Active Routes (${totalFlux.toFixed(1)} MWt)`;
  }
}

function renderExchangeDirectoryTable() {
  const tbody = document.getElementById('rooms-table-body');
  if (!tbody) return;

  const filtered = getFilteredPipelines();

  if (filtered.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="7" style="text-align: center; padding: 2.5rem; color: var(--text-muted);">
          No thermal pipelines match the current filter selection. Try selecting "All Temperature Grades" or "All District Zones".
        </td>
      </tr>
    `;
    return;
  }

  tbody.innerHTML = filtered.map((pipe, idx) => `
    <tr onclick="selectRouteRow(${idx})" style="cursor: pointer;">
      <td>
        <span class="custom-radio-circle ${state.selectedRouteIndex === idx ? 'selected' : ''}"></span>
      </td>
      <td>
        <div class="room-details-cell">
          <span class="room-name-bold" style="color:#0f172a; font-weight:600;">${pipe.source}</span>
          <span class="room-desc-muted">➔ ${pipe.sink}</span>
        </div>
      </td>
      <td>
        <div>${pipe.zone}</div>
        <div style="font-size:0.7rem; color:var(--text-light); font-family:var(--font-mono);">${pipe.distance}</div>
      </td>
      <td>
        <div style="font-weight:600; color:#f97316; font-family:var(--font-mono);">${pipe.tempProfile}</div>
        <div style="font-size:0.7rem; color:var(--accent-green);">Delivered: ${pipe.deliveredTemp}</div>
      </td>
      <td style="font-weight:600; color:var(--primary-cyan-hover); font-family:var(--font-mono);">${pipe.flux}</td>
      <td>
        <div style="font-weight:700; color:#00a86b; font-family:var(--font-sans); font-size:0.9rem;">
          ${formatINR(pipe.annualSavingsInr, true)}
        </div>
        <div style="font-size:0.68rem; color:var(--text-light); font-family:var(--font-mono);">
          ${formatINR(pipe.annualSavingsInr)}/yr · ${pipe.paybackYrs}
        </div>
      </td>
      <td>
        <button class="btn-book-action" onclick="event.stopPropagation(); simulateRouteAction('${pipe.source}', '${pipe.sink}', '${pipe.tempProfile}')">
          Simulate
        </button>
      </td>
    </tr>
  `).join('');
}

function selectRouteRow(idx) {
  state.selectedRouteIndex = idx;
  renderExchangeDirectoryTable();
}

function simulateRouteAction(source, sink, temps) {
  openGeminiModal();
  const queryInput = document.getElementById('gemini-query-input');
  if (queryInput) {
    queryInput.value = `Calculate EN 253 heat loss, ₹ savings, and payback period for linking ${source} to ${sink} (${temps})`;
    queryInput.focus();
  }
}

function filterTableByFloor(zone) {
  state.selectedZone = zone;
  applyGlobalFilter();
}

function filterTableByGrade(grade) {
  state.selectedGrade = grade;
  applyGlobalFilter();
}

function filterTableRooms(val) {
  state.searchQuery = val.trim();
  applyGlobalFilter();
}

function handleGlobalSearch(val) {
  state.searchQuery = val.trim();
  applyGlobalFilter();
}

function setRailActive(railId) {
  const rails = ['rail-btn-dashboard', 'rail-btn-directory', 'rail-btn-map', 'rail-btn-kinesis'];
  rails.forEach(id => {
    const el = document.getElementById(id);
    if (el) el.classList.toggle('active', id === railId);
  });
}

function setBreadcrumb(text) {
  const bc = document.getElementById('header-breadcrumb-active');
  if (bc) bc.textContent = text;
}

function switchTableView(mode) {
  state.currentViewMode = mode;
  const btnTable = document.getElementById('btn-view-table');
  const btnMap = document.getElementById('btn-view-map');
  const tableWrap = document.getElementById('corporate-table-wrap');
  const mapWrap = document.getElementById('map-embed-container');

  if (mode === 'table') {
    if (btnTable) btnTable.classList.add('active');
    if (btnMap) btnMap.classList.remove('active');
    if (tableWrap) tableWrap.style.display = 'block';
    if (mapWrap) mapWrap.classList.remove('active');
    setRailActive('rail-btn-directory');
    setBreadcrumb('Pipeline Directory');
  } else {
    if (btnTable) btnTable.classList.remove('active');
    if (btnMap) btnMap.classList.add('active');
    if (tableWrap) tableWrap.style.display = 'none';
    if (mapWrap) mapWrap.classList.add('active');
    setRailActive('rail-btn-map');
    setBreadcrumb('Dynamic Vector Map');
    renderVectorMap();
  }
}

function switchMainTab(tab) {
  if (tab === 'dashboard') {
    setRailActive('rail-btn-dashboard');
    setBreadcrumb('Live Dashboard');
    switchTableView('table');
    window.scrollTo({ top: 0, behavior: 'smooth' });
  } else if (tab === 'directory' || tab === 'bookings') {
    switchTableView('table');
    const tableSection = document.getElementById('corporate-table-wrap');
    if (tableSection) tableSection.scrollIntoView({ behavior: 'smooth' });
  } else if (tab === 'map') {
    switchTableView('map');
    const mapSection = document.getElementById('map-embed-container');
    if (mapSection) mapSection.scrollIntoView({ behavior: 'smooth' });
  } else if (tab === 'kinesis') {
    setRailActive('rail-btn-kinesis');
    setBreadcrumb('Amazon Kinesis Telemetry');
    const shardCard = document.querySelector('.kinesis-shards-panel') || document.getElementById('status-line-producers');
    if (shardCard) shardCard.scrollIntoView({ behavior: 'smooth' });
  }
}

// =========================================================================
// 3. DYNAMIC VECTOR MAP RENDERING
// =========================================================================

function renderMapBackground() {
  const gridLayer = document.getElementById('map-grid-layer');
  const topoLayer = document.getElementById('map-topo-layer');
  if (!gridLayer || !topoLayer) return;

  gridLayer.innerHTML = '';
  topoLayer.innerHTML = '';

  for (let x = 0; x <= MAP_BOUNDS.width; x += 80) {
    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    line.setAttribute('x1', x);
    line.setAttribute('y1', 0);
    line.setAttribute('x2', x);
    line.setAttribute('y2', MAP_BOUNDS.height);
    line.setAttribute('stroke', 'rgba(255,255,255,0.03)');
    line.setAttribute('stroke-width', '1');
    gridLayer.appendChild(line);
  }

  for (let y = 0; y <= MAP_BOUNDS.height; y += 80) {
    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    line.setAttribute('x1', 0);
    line.setAttribute('y1', y);
    line.setAttribute('x2', MAP_BOUNDS.width);
    line.setAttribute('y2', y);
    line.setAttribute('stroke', 'rgba(255,255,255,0.03)');
    line.setAttribute('stroke-width', '1');
    gridLayer.appendChild(line);
  }

  // Indrayani River Waterway
  const riverPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  riverPath.setAttribute('d', 'M 80 480 Q 320 420 600 450 T 1150 310');
  riverPath.setAttribute('fill', 'none');
  riverPath.setAttribute('stroke', 'rgba(6, 182, 212, 0.18)');
  riverPath.setAttribute('stroke-width', '10');
  riverPath.setAttribute('stroke-linecap', 'round');
  topoLayer.appendChild(riverPath);

  const riverLabel = document.createElementNS('http://www.w3.org/2000/svg', 'text');
  riverLabel.setAttribute('x', '620');
  riverLabel.setAttribute('y', '470');
  riverLabel.setAttribute('fill', 'rgba(6, 182, 212, 0.35)');
  riverLabel.setAttribute('font-size', '10');
  riverLabel.setAttribute('font-weight', '600');
  riverLabel.setAttribute('letter-spacing', '2px');
  riverLabel.setAttribute('font-family', 'Outfit');
  riverLabel.textContent = 'INDRAYANI RIVER BASIN';
  topoLayer.appendChild(riverLabel);

  // Regional Industrial Corridor Highway Spine
  const hwyPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  hwyPath.setAttribute('d', 'M 140 100 Q 550 260 1050 520');
  hwyPath.setAttribute('fill', 'none');
  hwyPath.setAttribute('stroke', 'rgba(255, 255, 255, 0.05)');
  hwyPath.setAttribute('stroke-width', '4');
  hwyPath.setAttribute('stroke-dasharray', '8 6');
  topoLayer.appendChild(hwyPath);

  const hwyLabel = document.createElementNS('http://www.w3.org/2000/svg', 'text');
  hwyLabel.setAttribute('x', '570');
  hwyLabel.setAttribute('y', '250');
  hwyLabel.setAttribute('fill', 'rgba(255, 255, 255, 0.22)');
  hwyLabel.setAttribute('font-size', '9');
  hwyLabel.setAttribute('font-weight', '500');
  hwyLabel.setAttribute('letter-spacing', '1.5px');
  hwyLabel.setAttribute('font-family', 'JetBrains Mono');
  hwyLabel.textContent = 'REGIONAL INDUSTRIAL TRANSIT CORRIDOR';
  topoLayer.appendChild(hwyLabel);

  // Geographic Region Watermark
  const regionTag = document.createElementNS('http://www.w3.org/2000/svg', 'text');
  regionTag.setAttribute('x', '30');
  regionTag.setAttribute('y', '40');
  regionTag.setAttribute('fill', 'rgba(255, 255, 255, 0.15)');
  regionTag.setAttribute('font-size', '11');
  regionTag.setAttribute('font-weight', '700');
  regionTag.setAttribute('letter-spacing', '2px');
  regionTag.setAttribute('font-family', 'Outfit');
  regionTag.textContent = 'REGIONAL INDUSTRIAL THERMAL GRID · DISTRICT SECTORS';
  topoLayer.appendChild(regionTag);
}

function renderMapElements() {
  renderPipelines();
  renderFacilities();
}

function renderPipelines() {
  const pipeLayer = document.getElementById('map-pipelines-layer');
  if (!pipeLayer) return;
  pipeLayer.innerHTML = '';

  const activePipes = getFilteredPipelines();
  const activePairKeys = new Set(activePipes.map(p => `${p.sourceId}->${p.sinkId}`));

  INDUSTRIAL_PIPELINES.forEach(pipe => {
    const prod = state.facilities.find(f => f.id === pipe.sourceId);
    const cons = state.facilities.find(f => f.id === pipe.sinkId);
    if (!prod || !cons) return;

    const isActive = activePairKeys.has(`${pipe.sourceId}->${pipe.sinkId}`);

    const p1 = projectGpsToSvg(prod.latitude, prod.longitude);
    const p2 = projectGpsToSvg(cons.latitude, cons.longitude);

    const midX = (p1.x + p2.x) / 2 + (p2.y - p1.y) * 0.15;
    const midY = (p1.y + p2.y) / 2 - (p2.x - p1.x) * 0.15;
    const pathD = `M ${p1.x} ${p1.y} Q ${midX} ${midY} ${p2.x} ${p2.y}`;

    const pipeGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    pipeGroup.setAttribute('style', `cursor: ${isActive ? 'pointer' : 'default'}; opacity: ${isActive ? '1' : '0.04'}; transition: opacity 0.35s ease;`);

    const track = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    track.setAttribute('d', pathD);
    track.setAttribute('fill', 'none');
    track.setAttribute('stroke', isActive ? 'rgba(255, 255, 255, 0.15)' : 'rgba(255, 255, 255, 0.02)');
    track.setAttribute('stroke-width', isActive ? '3.0' : '1.5');
    pipeGroup.appendChild(track);

    if (isActive) {
      const flow = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      flow.setAttribute('d', pathD);
      flow.setAttribute('fill', 'none');
      flow.setAttribute('stroke', pipe.grade === 'low' ? 'url(#pipe-gradient-med)' : 'url(#pipe-gradient-high)');
      flow.setAttribute('stroke-width', '3.0');
      flow.setAttribute('stroke-dasharray', '8 8');
      flow.setAttribute('style', 'animation: flow-dash 1.8s linear infinite;');
      pipeGroup.appendChild(flow);

      pipeGroup.addEventListener('click', () => {
        simulateRouteAction(pipe.source, pipe.sink, pipe.tempProfile);
      });
    }

    pipeLayer.appendChild(pipeGroup);
  });
}

function renderFacilities() {
  const facLayer = document.getElementById('map-facilities-layer');
  if (!facLayer) return;
  facLayer.innerHTML = '';

  const activePipes = getFilteredPipelines();
  const activeFacilityIds = new Set();
  activePipes.forEach(p => {
    activeFacilityIds.add(p.sourceId);
    activeFacilityIds.add(p.sinkId);
  });

  state.facilities.forEach(fac => {
    const pt = projectGpsToSvg(fac.latitude, fac.longitude);
    const isProducer = fac.facility_type === 'PRODUCER';
    const primaryColor = isProducer ? '#f97316' : '#06b6d4';
    const isActive = activeFacilityIds.has(fac.id);

    const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    group.setAttribute('style', `cursor: ${isActive ? 'pointer' : 'default'}; opacity: ${isActive ? '1' : '0.14'}; transition: opacity 0.35s ease;`);
    group.setAttribute('transform', `translate(${pt.x}, ${pt.y})`);

    // Stable Transparent Hit Area
    const hitArea = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    hitArea.setAttribute('r', '26');
    hitArea.setAttribute('fill', 'transparent');
    group.appendChild(hitArea);

    if (isActive) {
      // Pulse Circle
      const pulseCircle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      pulseCircle.setAttribute('r', '14');
      pulseCircle.setAttribute('fill', isProducer ? 'url(#producer-glow)' : 'url(#consumer-glow)');
      pulseCircle.setAttribute('style', 'pointer-events: none;');
      group.appendChild(pulseCircle);
    }

    // Solid Center Dot
    const baseCircle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    baseCircle.setAttribute('r', isActive ? '7.5' : '4.5');
    baseCircle.setAttribute('fill', primaryColor);
    baseCircle.setAttribute('stroke', '#090d14');
    baseCircle.setAttribute('stroke-width', '2');
    baseCircle.setAttribute('style', 'pointer-events: none;');
    group.appendChild(baseCircle);

    // Label Group
    const labelGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    labelGroup.setAttribute('transform', 'translate(0, 18)');
    labelGroup.setAttribute('style', 'pointer-events: none;');

    const labelBg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    labelBg.setAttribute('x', '-70');
    labelBg.setAttribute('y', '-9');
    labelBg.setAttribute('width', '140');
    labelBg.setAttribute('height', '18');
    labelBg.setAttribute('rx', '4');
    labelBg.setAttribute('fill', 'rgba(11, 16, 26, 0.92)');
    labelBg.setAttribute('stroke', isActive ? 'rgba(255, 255, 255, 0.18)' : 'rgba(255, 255, 255, 0.04)');
    labelBg.setAttribute('stroke-width', '1');
    labelGroup.appendChild(labelBg);

    const labelText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    labelText.setAttribute('x', '0');
    labelText.setAttribute('y', '0');
    labelText.setAttribute('text-anchor', 'middle');
    labelText.setAttribute('dominant-baseline', 'central');
    labelText.setAttribute('fill', isActive ? '#ffffff' : '#64748b');
    labelText.setAttribute('font-size', '9.5');
    labelText.setAttribute('font-weight', '600');
    labelText.setAttribute('font-family', 'Outfit');
    const shortName = fac.name.length > 21 ? fac.name.substring(0, 19) + '…' : fac.name;
    labelText.textContent = shortName;
    labelGroup.appendChild(labelText);

    // Temperature Badge Pill
    const tempPill = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    tempPill.setAttribute('transform', 'translate(0, -18)');
    tempPill.setAttribute('style', 'pointer-events: none;');

    const pillBg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    pillBg.setAttribute('x', '-25');
    pillBg.setAttribute('y', '-8');
    pillBg.setAttribute('width', '50');
    pillBg.setAttribute('height', '15');
    pillBg.setAttribute('rx', '7');
    pillBg.setAttribute('fill', isProducer ? 'rgba(249, 115, 22, 0.25)' : 'rgba(6, 182, 212, 0.25)');
    pillBg.setAttribute('stroke', primaryColor);
    pillBg.setAttribute('stroke-width', '1');
    tempPill.appendChild(pillBg);

    const tempText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    tempText.setAttribute('x', '0');
    tempText.setAttribute('y', '0');
    tempText.setAttribute('text-anchor', 'middle');
    tempText.setAttribute('dominant-baseline', 'central');
    tempText.setAttribute('fill', primaryColor);
    tempText.setAttribute('font-size', '9');
    tempText.setAttribute('font-weight', '700');
    tempText.setAttribute('font-family', 'JetBrains Mono');
    tempText.setAttribute('id', `temp-badge-text-${fac.id}`);
    tempText.textContent = `${fac.temperature_c.toFixed(1)}°C`;
    tempPill.appendChild(tempText);

    group.appendChild(labelGroup);
    group.appendChild(tempPill);

    if (isActive) {
      group.addEventListener('click', () => {
        openGeminiModal();
        const input = document.getElementById('gemini-query-input');
        if (input) {
          input.value = `Evaluate thermodynamic routing for ${fac.name} (${fac.temperature_c.toFixed(1)}°C) in Indian Rupees (₹)`;
          input.focus();
        }
      });
    }

    facLayer.appendChild(group);
  });
}

function renderQuickSightCharts() {
  const activePipes = getFilteredPipelines();
  const totalFlux = activePipes.reduce((acc, p) => acc + p.fluxMWt, 0);

  const peakSupply = totalFlux > 0 ? totalFlux.toFixed(0) : '68';
  const durationText = document.getElementById('qs-supply-envelope-text');
  if (durationText) {
    const gradeLabel = state.selectedGrade === 'low' ? 'Low-Grade' : (state.selectedGrade === 'high' ? 'High-Grade' : 'District');
    durationText.textContent = `${gradeLabel} Waste Heat Supply (${peakSupply} MWt Peak)`;
  }
}

// =========================================================================
// 4. THEME CONTROLLER (BLUE BASED DEFAULT vs RED BASED)
// =========================================================================

function toggleThemeDropdown(event) {
  if (event) event.stopPropagation();
  const dropdown = document.getElementById('theme-dropdown');
  if (dropdown) {
    dropdown.classList.toggle('active');
  }
}

function setAppTheme(theme) {
  const isRed = theme === 'red';
  if (isRed) {
    document.body.classList.add('theme-red');
  } else {
    document.body.classList.remove('theme-red');
  }

  const optBlue = document.getElementById('theme-opt-blue');
  const optRed = document.getElementById('theme-opt-red');
  if (optBlue) optBlue.classList.toggle('active', !isRed);
  if (optRed) optRed.classList.toggle('active', isRed);

  try {
    localStorage.setItem('thermalmatch_theme', isRed ? 'red' : 'blue');
  } catch (e) {}

  const dropdown = document.getElementById('theme-dropdown');
  if (dropdown) dropdown.classList.remove('active');
}

function initTheme() {
  let savedTheme = 'blue';
  try {
    savedTheme = localStorage.getItem('thermalmatch_theme') || 'blue';
  } catch (e) {}
  setAppTheme(savedTheme);

  document.addEventListener('click', (e) => {
    const dropdown = document.getElementById('theme-dropdown');
    const btn = document.getElementById('settings-theme-btn');
    if (dropdown && dropdown.classList.contains('active')) {
      if (!dropdown.contains(e.target) && !btn.contains(e.target)) {
        dropdown.classList.remove('active');
      }
    }
  });
}

// =========================================================================
// 5. MODALS & AI INTEGRATION (INR)
// =========================================================================

function openGeminiModal() {
  const modal = document.getElementById('gemini-modal');
  if (modal) modal.classList.add('active');
  const input = document.getElementById('gemini-query-input');
  if (input) input.focus();
}

function closeGeminiModal(event) {
  const modal = document.getElementById('gemini-modal');
  if (modal) modal.classList.remove('active');
}

function openQuickSightModal() {
  const modal = document.getElementById('quicksight-modal');
  if (modal) modal.classList.add('active');
}

function closeQuickSightModal(event) {
  const modal = document.getElementById('quicksight-modal');
  if (modal) modal.classList.remove('active');
}

async function submitGeminiQuery(event) {
  event.preventDefault();
  const input = document.getElementById('gemini-query-input');
  const query = input.value.trim();
  if (!query || state.isQueryingGemini) return;

  state.isQueryingGemini = true;
  input.value = '';

  const chatHistory = document.getElementById('gemini-chat-history');

  const userBubble = document.createElement('div');
  userBubble.className = 'chat-bubble-clean user';
  const myInitials = (state.currentUser && state.currentUser.initials) ? state.currentUser.initials : 'ME';
  userBubble.innerHTML = `
    <div class="chat-bubble-avatar">${myInitials}</div>
    <div class="chat-bubble-card">${escapeHtml(query)}</div>
  `;
  chatHistory.appendChild(userBubble);

  const thinkingBubble = document.createElement('div');
  thinkingBubble.className = 'chat-bubble-clean assistant';
  thinkingBubble.innerHTML = `
    <div class="chat-bubble-avatar">AI</div>
    <div class="chat-bubble-card" style="color:var(--text-muted); font-style:italic;">
      Gathering Kinesis telemetry and calculating thermodynamic feasibility in Indian Rupees (₹)...
    </div>
  `;
  chatHistory.appendChild(thinkingBubble);
  chatHistory.scrollTop = chatHistory.scrollHeight;

  try {
    const res = await fetch('/api/gemini/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: query })
    });

    const data = await res.json();
    thinkingBubble.remove();

    const aiBubble = document.createElement('div');
    aiBubble.className = 'chat-bubble-clean assistant';
    aiBubble.innerHTML = `
      <div class="chat-bubble-avatar">AI</div>
      <div class="chat-bubble-card">
        ${formatMarkdown(data.advisory_text)}
      </div>
    `;
    chatHistory.appendChild(aiBubble);
    chatHistory.scrollTop = chatHistory.scrollHeight;
  } catch (err) {
    console.error('Error querying Gemini:', err);
    thinkingBubble.remove();
  } finally {
    state.isQueryingGemini = false;
  }
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function formatMarkdown(text) {
  if (!text) return '';

  const lines = text.split('\n');
  let inTable = false;
  let html = [];
  let tableRows = [];

  function flushTable() {
    if (tableRows.length === 0) return;
    let tHtml = '<div style="overflow-x:auto; margin:0.75rem 0;"><table style="width:100%; border-collapse:collapse; font-size:0.78rem; border:1px solid #e2e8f0; border-radius:6px;">';
    tableRows.forEach((row, rIdx) => {
      if (row.every(c => c.match(/^:?-+:?$/))) return;
      const isHeader = rIdx === 0;
      tHtml += `<tr style="${isHeader ? 'background:#f1f5f9; font-weight:700; color:#0f172a;' : (rIdx % 2 === 0 ? 'background:#f8fafc;' : 'background:#ffffff;')}">`;
      row.forEach(cell => {
        const tag = isHeader ? 'th' : 'td';
        const formattedCell = cell
          .replace(/\*\*(.*?)\*\*/g, '<strong style="color:#0f172a;">$1</strong>')
          .replace(/`([^`]+)`/g, '<code style="background:#e2e8f0; padding:1px 4px; border-radius:3px;">$1</code>');
        tHtml += `<${tag} style="padding:6px 8px; border:1px solid #e2e8f0; text-align:left;">${formattedCell}</${tag}>`;
      });
      tHtml += '</tr>';
    });
    tHtml += '</table></div>';
    html.push(tHtml);
    tableRows = [];
    inTable = false;
  }

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (line.startsWith('|') && line.endsWith('|')) {
      inTable = true;
      const cells = line.split('|').slice(1, -1).map(c => c.trim());
      tableRows.push(cells);
    } else {
      if (inTable) flushTable();
      html.push(lines[i]);
    }
  }
  if (inTable) flushTable();

  let formatted = html.join('\n');
  return formatted
    .replace(/^### (.*?)$/gm, '<h3 style="margin:0.75rem 0 0.35rem; font-size:1rem; font-weight:700; color:#0f172a;">$1</h3>')
    .replace(/^#### (.*?)$/gm, '<h4 style="margin:0.6rem 0 0.25rem; font-size:0.88rem; font-weight:600; color:#1e293b;">$1</h4>')
    .replace(/\*\*(.*?)\*\*/g, '<strong style="color:#0f172a;">$1</strong>')
    .replace(/\*(.*?)\*/g, '<em>$1</em>')
    .replace(/`([^`]+)`/g, '<code style="background:#e2e8f0; color:#0f172a; padding:2px 6px; border-radius:4px; font-size:0.78rem; font-family:monospace;">$1</code>')
    .replace(/^[-*•] (.*?)$/gm, '<div style="margin:0.25rem 0 0.25rem 0.5rem; line-height:1.4;">• $1</div>')
    .replace(/\n\n+/g, '<br><br>');
}

// =========================================================================
// 8. ADD COMPANY FACILITY MODAL & CONTROLLER
// =========================================================================

function getNextCompanyName() {
  const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  const existingNames = new Set((state.facilities || []).map(f => f.name.trim()));
  for (let char of letters) {
    const candidate = `Company ${char}`;
    if (!existingNames.has(candidate)) {
      return candidate;
    }
  }
  return `Company ${(state.facilities || []).length + 1}`;
}

function openAddFacilityModal() {
  const modal = document.getElementById('add-facility-modal');
  if (!modal) return;

  const nameInput = document.getElementById('new-fac-name');
  if (nameInput) {
    nameInput.value = getNextCompanyName();
  }

  // Generate sensible GPS coordinates within regional bounds
  const randomLat = (18.71 + Math.random() * 0.05).toFixed(4);
  const randomLng = (73.73 + Math.random() * 0.08).toFixed(4);
  const latInput = document.getElementById('new-fac-lat');
  const lngInput = document.getElementById('new-fac-lng');
  if (latInput && !latInput.value) latInput.value = randomLat;
  if (lngInput && !lngInput.value) lngInput.value = randomLng;

  modal.classList.add('active');
}

function closeAddFacilityModal() {
  const modal = document.getElementById('add-facility-modal');
  if (modal) modal.classList.remove('active');
}

async function handleCreateFacility(e) {
  if (e) e.preventDefault();
  const submitBtn = document.getElementById('btn-submit-facility');
  if (submitBtn) {
    submitBtn.disabled = true;
    submitBtn.textContent = 'Registering Facility...';
  }

  try {
    const name = document.getElementById('new-fac-name').value.trim() || getNextCompanyName();
    const type = document.getElementById('new-fac-type').value;
    const sector = document.getElementById('new-fac-sector').value;
    const lat = parseFloat(document.getElementById('new-fac-lat').value) || 18.7400;
    const lng = parseFloat(document.getElementById('new-fac-lng').value) || 73.7600;
    const zoneKey = document.getElementById('new-fac-zone').value;
    const temp = parseFloat(document.getElementById('new-fac-temp').value) || 95.0;
    const flow = parseFloat(document.getElementById('new-fac-flow').value) || 40.0;
    const capacity = parseFloat(document.getElementById('new-fac-capacity').value) || 10.0;
    const fluid = document.getElementById('new-fac-fluid').value;
    const desc = document.getElementById('new-fac-desc').value.trim() || `Industrial heat facility in Regional Industrial Corridor (${zoneKey}).`;

    const payload = {
      name: name,
      facility_type: type,
      sector: sector,
      latitude: lat,
      longitude: lng,
      zone_id: `zone-${zoneKey.toLowerCase().replace(/\s+/g, '-')}`,
      temperature_c: temp,
      mass_flow_rate_kg_s: flow,
      thermal_capacity_mwt: capacity,
      fluid_medium: fluid,
      description: desc,
      operating_hours_year: 8000
    };

    const res = await fetch('/api/facilities', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    if (!res.ok) {
      throw new Error(`Server returned HTTP ${res.status}`);
    }

    const createdFacility = await res.json();
    state.facilities.push(createdFacility);

    // If it's a producer or consumer, generate an active pipeline pairing with an existing peer so it shows immediately in the table and map
    if (type === 'PRODUCER') {
      const bestSink = state.facilities.find(f => f.facility_type === 'CONSUMER' && f.temperature_c < temp) || state.facilities.find(f => f.facility_type === 'CONSUMER');
      if (bestSink) {
        const pipeGrade = bestSink.temperature_c >= 75 ? 'high' : 'low';
        const fluxMWt = Math.min(capacity, bestSink.thermal_capacity_mwt);
        const annualSavInr = Math.round(fluxMWt * 7000000);
        INDUSTRIAL_PIPELINES.unshift({
          id: `pipe-${createdFacility.id.toLowerCase()}-${bestSink.id.toLowerCase()}`,
          source: createdFacility.name,
          sourceId: createdFacility.id,
          sourceType: createdFacility.sector,
          sourceTemp: temp,
          sink: bestSink.name,
          sinkId: bestSink.id,
          sinkType: bestSink.sector,
          sinkTemp: bestSink.temperature_c,
          zone: `Regional ${zoneKey} to District Grid`,
          zoneKey: zoneKey,
          distance: '1.65 km (DN200)',
          tempProfile: `${temp.toFixed(0)}°C ➔ ${bestSink.temperature_c.toFixed(0)}°C`,
          deliveredTemp: `${(temp - 2.8).toFixed(1)}°C`,
          flux: `${fluxMWt.toFixed(1)} MWt`,
          fluxMWt: fluxMWt,
          annualSavingsInr: annualSavInr,
          hourlySavingsInr: Math.round(annualSavInr / 8760),
          paybackYrs: '3.40 Yrs',
          tco2eYr: Math.round(fluxMWt * 1500),
          grade: pipeGrade
        });
      }
    } else {
      const bestSource = state.facilities.find(f => f.facility_type === 'PRODUCER' && f.temperature_c > temp) || state.facilities.find(f => f.facility_type === 'PRODUCER');
      if (bestSource) {
        const pipeGrade = temp >= 75 ? 'high' : 'low';
        const fluxMWt = Math.min(capacity, bestSource.thermal_capacity_mwt);
        const annualSavInr = Math.round(fluxMWt * 7000000);
        INDUSTRIAL_PIPELINES.unshift({
          id: `pipe-${bestSource.id.toLowerCase()}-${createdFacility.id.toLowerCase()}`,
          source: bestSource.name,
          sourceId: bestSource.id,
          sourceType: bestSource.sector,
          sourceTemp: bestSource.temperature_c,
          sink: createdFacility.name,
          sinkId: createdFacility.id,
          sinkType: createdFacility.sector,
          sinkTemp: temp,
          zone: `Regional ${zoneKey} to District Grid`,
          zoneKey: zoneKey,
          distance: '1.40 km (DN200)',
          tempProfile: `${bestSource.temperature_c.toFixed(0)}°C ➔ ${temp.toFixed(0)}°C`,
          deliveredTemp: `${(bestSource.temperature_c - 2.4).toFixed(1)}°C`,
          flux: `${fluxMWt.toFixed(1)} MWt`,
          fluxMWt: fluxMWt,
          annualSavingsInr: annualSavInr,
          hourlySavingsInr: Math.round(annualSavInr / 8760),
          paybackYrs: '3.25 Yrs',
          tco2eYr: Math.round(fluxMWt * 1500),
          grade: pipeGrade
        });
      }
    }

    applyGlobalFilter();
    closeAddFacilityModal();
    showToastNotification(`Successfully registered ${createdFacility.name} in Regional Industrial Corridor!`);
  } catch (err) {
    console.error('Error creating facility:', err);
    alert('Failed to register facility. Please check input values.');
  } finally {
    if (submitBtn) {
      submitBtn.disabled = false;
      submitBtn.textContent = 'Register Facility';
    }
  }
}

function showToastNotification(msg) {
  let toast = document.getElementById('dashboard-toast');
  if (!toast) {
    toast = document.createElement('div');
    toast.id = 'dashboard-toast';
    toast.style.cssText = 'position:fixed; bottom:28px; right:28px; background:linear-gradient(135deg, #0b1329, #1e293b); color:#ffffff; padding:14px 22px; border-radius:10px; border:1px solid #06b6d4; box-shadow:0 12px 36px rgba(0,0,0,0.5); font-family:var(--font-primary, sans-serif); font-size:0.88rem; font-weight:600; z-index:9999; display:flex; align-items:center; gap:10px; transition:opacity 0.4s ease, transform 0.4s ease; transform:translateY(20px); opacity:0; pointer-events:none;';
    document.body.appendChild(toast);
  }
  toast.innerHTML = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#06b6d4" stroke-width="2.5"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg><span>${msg}</span>`;
  toast.style.transform = 'translateY(0)';
  toast.style.opacity = '1';

  setTimeout(() => {
    toast.style.transform = 'translateY(20px)';
    toast.style.opacity = '0';
  }, 4500);
}

// =========================================================================
// 8. USER AUTHENTICATION & SESSION MANAGEMENT
// =========================================================================

const DEFAULT_USER = {
  name: 'Facility Director',
  role: 'Regional Grid Admin',
  email: 'director@thermalmatch.internal',
  initials: 'FD',
  company: 'Regional Industrial Corridor'
};

function getInitials(name) {
  if (!name) return 'U';
  const parts = name.trim().split(/\s+/);
  if (parts.length >= 2) {
    return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
  }
  return name.substring(0, 2).toUpperCase();
}

function updateGreeting(userName) {
  const greetingEl = document.getElementById('greeting-title');
  if (!greetingEl) return;
  const hour = new Date().getHours();
  let timeOfDay = 'morning';
  if (hour >= 12 && hour < 17) {
    timeOfDay = 'afternoon';
  } else if (hour >= 17 || hour < 5) {
    timeOfDay = 'evening';
  }
  greetingEl.textContent = `Good ${timeOfDay}, ${userName || 'Facility Director'}!`;
}

function applyUserToUI(user) {
  if (!user) user = DEFAULT_USER;
  
  // Header widget
  const initialsEl = document.getElementById('header-avatar-initials');
  const nameEl = document.getElementById('header-user-name');
  const roleEl = document.getElementById('header-user-role');
  if (initialsEl) initialsEl.textContent = user.initials || getInitials(user.name);
  if (nameEl) nameEl.textContent = user.name;
  if (roleEl) roleEl.textContent = user.role;

  // Dropdown menu header
  const dropNameEl = document.getElementById('dropdown-user-name');
  const dropEmailEl = document.getElementById('dropdown-user-email');
  if (dropNameEl) dropNameEl.textContent = user.name;
  if (dropEmailEl) dropEmailEl.textContent = user.email || (user.name.toLowerCase().replace(/[^a-z0-9]/g, '.') + '@thermalmatch.internal');

  // Dynamic greeting banner
  updateGreeting(user.name);
}

function initUserSession() {
  let currentUser = DEFAULT_USER;
  try {
    const saved = localStorage.getItem('thermalmatch_user');
    if (saved) {
      const parsed = JSON.parse(saved);
      if (parsed && parsed.name) {
        currentUser = parsed;
      }
    }
  } catch (e) {
    console.warn('Failed to parse saved user from localStorage', e);
  }

  state.currentUser = currentUser;
  applyUserToUI(currentUser);

  // Close dropdown menu on outside click
  document.addEventListener('click', (e) => {
    const widget = document.getElementById('user-profile-widget');
    const menu = document.getElementById('user-dropdown-menu');
    if (widget && menu && menu.classList.contains('active')) {
      if (!widget.contains(e.target)) {
        menu.classList.remove('active');
      }
    }
  });

  // Close modals on Escape key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      closeLoginModal();
      closeUserDropdown();
      closeAddFacilityModal();
    }
  });
}

function toggleUserDropdown(event) {
  if (event) event.stopPropagation();
  const menu = document.getElementById('user-dropdown-menu');
  if (menu) {
    menu.classList.toggle('active');
  }
}

function closeUserDropdown() {
  const menu = document.getElementById('user-dropdown-menu');
  if (menu) {
    menu.classList.remove('active');
  }
}

function openLoginModal(event) {
  if (event) event.stopPropagation();
  closeUserDropdown();
  const modal = document.getElementById('login-modal');
  if (modal) {
    modal.classList.add('active');
    const nameInput = document.getElementById('login-input-name');
    if (nameInput) {
      nameInput.value = (state.currentUser && state.currentUser.name !== 'Guest User') ? state.currentUser.name : '';
      setTimeout(() => nameInput.focus({ preventScroll: true }), 80);
    }
    const emailInput = document.getElementById('login-input-email');
    if (emailInput && state.currentUser && state.currentUser.email && state.currentUser.email !== 'guest@thermalmatch.internal') {
      emailInput.value = state.currentUser.email;
    }
  }
}

function closeLoginModal() {
  const modal = document.getElementById('login-modal');
  if (modal) {
    modal.classList.remove('active');
  }
}

function quickSwitchUser(name, role, email, initials, event) {
  if (event) event.stopPropagation();
  const newUser = {
    name,
    role,
    email: email || `${name.toLowerCase().replace(/[^a-z0-9]/g, '.')}@thermalmatch.internal`,
    initials: initials || getInitials(name),
    company: role
  };
  try {
    localStorage.setItem('thermalmatch_user', JSON.stringify(newUser));
  } catch (e) {}

  state.currentUser = newUser;
  applyUserToUI(newUser);
  closeUserDropdown();
  closeLoginModal();
  showToastNotification(`Active Persona switched to: ${name} (${role})`);
}

function handleCustomLogin(event) {
  if (event) event.preventDefault();
  const nameInput = document.getElementById('login-input-name');
  const roleSelect = document.getElementById('login-input-role');
  const companySelect = document.getElementById('login-input-company');
  const emailInput = document.getElementById('login-input-email');

  const rawName = nameInput?.value?.trim() || 'Facility Director';
  const role = roleSelect?.value?.trim() || 'Facility Director';
  const company = companySelect?.value?.trim() || 'Regional Corridor Authority';
  let email = emailInput?.value?.trim();
  if (!email) {
    email = `${rawName.toLowerCase().replace(/[^a-z0-9]/g, '.')}@thermalmatch.internal`;
  }
  const initials = getInitials(rawName);

  const newUser = {
    name: rawName,
    role: `${role} · ${company.split(' ')[0] || company}`,
    fullRole: `${role} (${company})`,
    email,
    initials,
    company
  };

  try {
    localStorage.setItem('thermalmatch_user', JSON.stringify(newUser));
  } catch (e) {}

  state.currentUser = newUser;
  applyUserToUI(newUser);
  closeLoginModal();
  closeUserDropdown();
  showToastNotification(`Authenticated as ${rawName} · ${newUser.role}`);
}

function handleLogout(event) {
  if (event) event.stopPropagation();
  const guestUser = {
    name: 'Guest User',
    role: 'Read-Only Viewer',
    email: 'guest@thermalmatch.internal',
    initials: 'GU',
    company: 'External Visitor'
  };

  try {
    localStorage.setItem('thermalmatch_user', JSON.stringify(guestUser));
  } catch (e) {}

  state.currentUser = guestUser;
  applyUserToUI(guestUser);
  closeUserDropdown();
  showToastNotification('Signed out. Switched to Guest / Read-Only session.');
}
