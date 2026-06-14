(function () {
  const KEY = 'f1cleanair_prefs';
  const DEFAULT_ACCENT = '#00C8FF';

  const prefs = JSON.parse(localStorage.getItem(KEY) || '{}');
  if (!prefs.favoriteTeams) prefs.favoriteTeams = [];
  if (!prefs.favoriteDrivers) prefs.favoriteDrivers = [];

  function applyAccent() {
    document.documentElement.style.setProperty('--accent', prefs.accent || DEFAULT_ACCENT);
  }
  applyAccent();

  function save() {
    localStorage.setItem(KEY, JSON.stringify(prefs));
    document.dispatchEvent(new CustomEvent('f1prefs-change', { detail: prefs }));
  }

  function toggle(list, value) {
    const idx = list.indexOf(value);
    if (idx >= 0) list.splice(idx, 1);
    else list.push(value);
  }

  window.F1Prefs = prefs;

  function buildPanel(teams) {
    const nav = document.querySelector('nav');
    if (!nav) return;

    const wrap = document.createElement('div');
    wrap.className = 'prefs';
    wrap.innerHTML = `
      <button id="prefs-btn" class="settings-btn" aria-label="Preferences">&#127912;</button>
      <div class="settings-panel prefs-panel" id="prefs-panel">
        <div class="prefs-section">
          <div class="prefs-title">Theme color</div>
          <div class="swatches" id="swatches"></div>
        </div>
        <div class="prefs-section">
          <div class="prefs-title">Favorite team</div>
          <div class="chips" id="team-chips"></div>
        </div>
        <div class="prefs-section">
          <div class="prefs-title">Favorite driver</div>
          <div class="chips" id="driver-chips"></div>
        </div>
      </div>
    `;
    nav.appendChild(wrap);

    document.getElementById('prefs-btn').addEventListener('click', () => {
      document.getElementById('prefs-panel').classList.toggle('open');
    });

    const swatches = document.getElementById('swatches');
    const teamChips = document.getElementById('team-chips');
    const driverChips = document.getElementById('driver-chips');

    const defaultSwatch = document.createElement('button');
    defaultSwatch.className = 'swatch';
    defaultSwatch.style.background = DEFAULT_ACCENT;
    defaultSwatch.title = 'Default';
    defaultSwatch.addEventListener('click', () => {
      prefs.accent = DEFAULT_ACCENT;
      applyAccent();
      save();
    });
    swatches.appendChild(defaultSwatch);

    teams.forEach((t) => {
      const color = '#' + (t.team_colour || '00C8FF');

      const sw = document.createElement('button');
      sw.className = 'swatch';
      sw.style.background = color;
      sw.title = t.team_name;
      sw.addEventListener('click', () => {
        prefs.accent = color;
        applyAccent();
        save();
      });
      swatches.appendChild(sw);

      const chip = document.createElement('button');
      chip.className = 'chip' + (prefs.favoriteTeams.includes(t.team_name) ? ' active' : '');
      chip.textContent = t.team_name;
      chip.addEventListener('click', () => {
        toggle(prefs.favoriteTeams, t.team_name);
        chip.classList.toggle('active');
        save();
      });
      teamChips.appendChild(chip);

      t.drivers.forEach((d) => {
        if (!d.tla) return;
        const dchip = document.createElement('button');
        dchip.className = 'chip' + (prefs.favoriteDrivers.includes(d.tla) ? ' active' : '');
        dchip.textContent = d.tla;
        dchip.addEventListener('click', () => {
          toggle(prefs.favoriteDrivers, d.tla);
          dchip.classList.toggle('active');
          save();
        });
        driverChips.appendChild(dchip);
      });
    });
  }

  document.addEventListener('DOMContentLoaded', () => {
    fetch('/api/teams')
      .then((r) => r.json())
      .then(buildPanel)
      .catch(() => {});

    const form = document.getElementById('newsletter-form');
    if (form) {
      form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const email = form.querySelector('input[type=email]').value;
        const msg = document.getElementById('newsletter-msg');
        msg.textContent = '';
        try {
          const res = await fetch('/api/subscribe', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email }),
          });
          const data = await res.json();
          if (data.status === 'ok') {
            msg.textContent = 'Subscribed — thanks!';
            msg.className = 'msg msg-good';
            form.reset();
          } else {
            msg.textContent = data.error || 'Something went wrong.';
            msg.className = 'msg msg-bad';
          }
        } catch (err) {
          msg.textContent = 'Something went wrong.';
          msg.className = 'msg msg-bad';
        }
      });
    }
  });
})();
