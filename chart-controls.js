function initChartControls(wrapId) {
  const wrap = document.getElementById(wrapId);
  if (!wrap || wrap.dataset.controlsInit) return;
  wrap.dataset.controlsInit = '1';

  const svg = wrap.querySelector('svg');
  const scrollEl = wrap.querySelector('.chart-scroll') || wrap;

  const toolbar = document.createElement('div');
  toolbar.className = 'chart-toolbar';
  toolbar.innerHTML = `
    <button type="button" class="chart-btn" data-action="zoom-out" title="Zoom out">&minus;</button>
    <button type="button" class="chart-btn chart-zoom-label" data-action="zoom-reset" title="Reset zoom">100%</button>
    <button type="button" class="chart-btn" data-action="zoom-in" title="Zoom in">+</button>
    <button type="button" class="chart-btn" data-action="fullscreen" title="Toggle fullscreen">&#x26F6;</button>
  `;
  wrap.insertBefore(toolbar, wrap.firstChild);

  let zoom = 1;

  function applyZoom() {
    const vb = svg.viewBox.baseVal;
    if (zoom === 1 || !vb || !vb.width) {
      svg.style.width = '';
      svg.style.height = '';
    } else {
      const baseWidth = scrollEl.clientWidth || wrap.clientWidth;
      const w = baseWidth * zoom;
      const h = w * vb.height / vb.width;
      svg.style.width = w + 'px';
      svg.style.height = h + 'px';
    }
    toolbar.querySelector('[data-action="zoom-reset"]').textContent = Math.round(zoom * 100) + '%';
  }

  toolbar.addEventListener('click', (e) => {
    const action = e.target.dataset.action;
    if (!action) return;
    if (action === 'zoom-in') zoom = Math.min(4, zoom + 0.25);
    else if (action === 'zoom-out') zoom = Math.max(1, zoom - 0.25);
    else if (action === 'zoom-reset') zoom = 1;
    else if (action === 'fullscreen') {
      if (!document.fullscreenElement) wrap.requestFullscreen();
      else document.exitFullscreen();
      return;
    }
    applyZoom();
  });

  document.addEventListener('fullscreenchange', () => {
    requestAnimationFrame(applyZoom);
  });
  window.addEventListener('resize', () => requestAnimationFrame(applyZoom));
  new MutationObserver(() => requestAnimationFrame(applyZoom)).observe(svg, { attributes: true, attributeFilter: ['viewBox'] });

  applyZoom();
}
