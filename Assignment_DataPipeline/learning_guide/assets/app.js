// NammaMart Learning Guide — tabs, story/deep-dive toggles, theme, code explainer
(function () {
  function store(k, v) { try { localStorage.setItem(k, v); } catch (e) {} }
  function load(k) { try { return localStorage.getItem(k); } catch (e) { return null; } }

  // ---------- Tabs ----------
  function showPage(id) {
    document.querySelectorAll('.page').forEach(function (p) {
      p.classList.toggle('active', p.id === id);
    });
    document.querySelectorAll('nav.tabs button').forEach(function (b) {
      b.classList.toggle('active', b.dataset.target === id);
    });
    if (history.replaceState) history.replaceState(null, '', '#' + id);
    window.scrollTo({ top: 0, behavior: 'auto' });
  }

  document.addEventListener('click', function (e) {
    var btn = e.target.closest('nav.tabs button');
    if (btn) { showPage(btn.dataset.target); return; }
    var link = e.target.closest('a[data-jump]');
    if (link) { e.preventDefault(); showPage(link.dataset.jump); }
  });

  // ---------- Story mode ----------
  function applyStory(on) {
    document.body.classList.toggle('story-off', !on);
    var cb = document.getElementById('story-switch');
    var label = document.getElementById('story-label');
    if (cb) cb.checked = on;
    if (label) label.textContent = on ? 'Story: On' : 'Story: Off';
  }
  var savedStory = load('nm_story');
  applyStory(savedStory === null ? true : savedStory === '1');

  // ---------- Deep dive ----------
  function applyDeep(on) {
    document.body.classList.toggle('deep-on', on);
    var cb = document.getElementById('deep-switch');
    var label = document.getElementById('deep-label');
    if (cb) cb.checked = on;
    if (label) label.textContent = on ? 'Deep dive: On' : 'Deep dive: Off';
  }
  var savedDeep = load('nm_deep');
  applyDeep(savedDeep === '1'); // default OFF

  // ---------- Theme ----------
  var THEMES = ['light', 'sepia', 'ocean', 'grape', 'forest', 'rose']; // 'dark' = no class
  function applyTheme(name) {
    THEMES.forEach(function (t) { document.body.classList.remove('theme-' + t); });
    if (name !== 'dark') document.body.classList.add('theme-' + name);
    var sel = document.getElementById('theme-select');
    if (sel) sel.value = name;
  }
  applyTheme(load('nm_theme') || 'dark');

  var STYLES = ['sharp', 'playful']; // 'soft' = no class
  function applyStyle(name) {
    STYLES.forEach(function (s) { document.body.classList.remove('style-' + s); });
    if (name !== 'soft') document.body.classList.add('style-' + name);
    var sel = document.getElementById('style-select');
    if (sel) sel.value = name;
  }
  applyStyle(load('nm_style') || 'soft');

  document.addEventListener('change', function (e) {
    if (!e.target) return;
    if (e.target.id === 'story-switch') { applyStory(e.target.checked); store('nm_story', e.target.checked ? '1' : '0'); }
    if (e.target.id === 'deep-switch')  { applyDeep(e.target.checked);  store('nm_deep', e.target.checked ? '1' : '0'); }
    if (e.target.id === 'theme-select') { applyTheme(e.target.value);   store('nm_theme', e.target.value); }
    if (e.target.id === 'style-select') { applyStyle(e.target.value);   store('nm_style', e.target.value); }
  });

  // ---------- Code explainer ----------
  // Each .explainer contains .code-col > .blk[data-ex-title][data-ex] blocks and
  // an .explain-col panel. Clicking a block shows its explanation; Walk steps through.
  function initExplainers() {
    document.querySelectorAll('.explainer').forEach(function (ex) {
      var blocks = Array.prototype.slice.call(ex.querySelectorAll('.code-col .blk'));
      var panel = ex.querySelector('.explain-col');
      var walkBtn = ex.querySelector('.walk-btn');
      var idx = -1;

      function render(i) {
        blocks.forEach(function (b, j) { b.classList.toggle('active', j === i); });
        var b = blocks[i];
        if (!b) return;
        var title = b.getAttribute('data-ex-title') || 'Explanation';
        var text = b.getAttribute('data-ex') || '';
        var paras = text.split('|').map(function (t) { return '<p>' + t + '</p>'; }).join('');
        panel.innerHTML = '<div class="ex-title">' + title + '</div>' +
                          '<div class="ex-text">' + paras + '</div>' +
                          '<div class="walk-counter">Block ' + (i + 1) + ' of ' + blocks.length +
                          ' · click any line, or use “Walk me through”.</div>';
        idx = i;
        b.scrollIntoView({ block: 'nearest' });
      }

      blocks.forEach(function (b, i) { b.addEventListener('click', function () { render(i); }); });
      if (walkBtn) {
        walkBtn.addEventListener('click', function () {
          var next = (idx + 1) % blocks.length;
          render(next);
          walkBtn.textContent = idx === blocks.length - 1 ? '↻ Start over' : '▶ Next block';
        });
      }
    });
  }
  initExplainers();

  // open tab from URL hash, else first page
  var start = (location.hash || '').replace('#', '');
  showPage(start && document.getElementById(start) ? start : 'start');
})();
