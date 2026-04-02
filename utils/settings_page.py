"""HTML template for the web-based settings editor.

Generates a single-page settings form with two tabs (pd_zurg env vars
and plex_debrid settings.json). Communicates with /api/settings/* endpoints.
"""

import json


def get_settings_html(env_schema, pd_schema):
    """Return the complete settings editor HTML page with shared CSS and nav.

    Args:
        env_schema: The env var schema dict from get_env_schema()
        pd_schema: The plex_debrid schema dict from get_plex_debrid_schema()
    """
    from utils.ui_common import (get_base_head, get_nav_html, THEME_TOGGLE_JS,
                                 WANTED_BADGE_JS, KEYBOARD_JS, TOAST_JS)
    # Escape </ to prevent script tag breakout in JSON-in-HTML context
    env_json = json.dumps(env_schema).replace('</', '<\\/')
    pd_json = json.dumps(pd_schema).replace('</', '<\\/')
    html = _SETTINGS_HTML
    html = html.replace('__BASE_HEAD__', get_base_head('pd_zurg Settings'))
    html = html.replace('__NAV_HTML__', get_nav_html('settings'))
    html = html.replace('__THEME_TOGGLE_JS__', THEME_TOGGLE_JS + KEYBOARD_JS + TOAST_JS)
    html = html.replace('__WANTED_BADGE_JS__', WANTED_BADGE_JS)
    html = html.replace('__ENV_SCHEMA_JSON__', env_json)
    html = html.replace('__PD_SCHEMA_JSON__', pd_json)
    return html


_SETTINGS_HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
__BASE_HEAD__
</head>
<body>
__NAV_HTML__
<style>
body{max-width:900px}

/* Tabs */
.tabs{display:flex;gap:0;margin-bottom:16px;border-bottom:2px solid var(--border)}
.tab{padding:10px 20px;cursor:pointer;color:var(--text2);font-size:.9em;font-weight:500;border-bottom:2px solid transparent;margin-bottom:-2px;transition:color .15s,border-color .15s;user-select:none}
.tab:hover{color:var(--text)}
.tab.active{color:var(--blue);border-bottom-color:var(--blue)}
.tab.dirty::after{content:' *';color:var(--yellow);font-weight:700}
.tab-content{display:none}
.tab-content.active{display:block}

/* Search filter */
.search-bar{margin-bottom:14px}
.search-bar input{width:100%;background:var(--input-bg);border:1px solid var(--input-border);border-radius:6px;padding:9px 12px 9px 34px;color:var(--text);font-size:.85em;outline:none;transition:border-color .15s;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' fill='%23636e7b' viewBox='0 0 16 16'%3E%3Cpath d='M11.742 10.344a6.5 6.5 0 1 0-1.397 1.398l3.85 3.85a1 1 0 0 0 1.415-1.414l-3.85-3.85zm-5.242.156a5 5 0 1 1 0-10 5 5 0 0 1 0 10z'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:10px center}
.search-bar input:focus{border-color:var(--input-focus)}
.search-bar .search-count{font-size:.75em;color:var(--text2);margin-top:4px}

/* Category sections */
.category{background:var(--card);border:1px solid var(--border);border-radius:8px;margin-bottom:12px;overflow:hidden}
.cat-header{display:flex;justify-content:space-between;align-items:center;padding:14px 16px;cursor:pointer;user-select:none;transition:background .15s}
.cat-header:hover{background:#1c2129}
.cat-header h2{font-size:.9em;font-weight:600;color:var(--text);display:flex;align-items:center;gap:8px}
.cat-header .desc{font-size:.8em;color:var(--text2);font-weight:400}
.cat-header .arrow{color:var(--text3);font-size:.8em;transition:transform .2s}
.cat-header.open .arrow{transform:rotate(180deg)}
.cat-body{padding:0 16px 16px;display:none}
.cat-body.open{display:block}

/* Hidden fields toggle */
.advanced-toggle{font-size:.8em;color:var(--text3);cursor:pointer;padding:8px 0 4px;border-top:1px solid var(--border2);margin-top:8px;user-select:none}
.advanced-toggle:hover{color:var(--blue)}
.advanced-fields{display:none}
.advanced-fields.open{display:block}

/* Form fields */
.field{display:grid;grid-template-columns:200px 1fr;gap:8px 16px;align-items:start;padding:10px 0;border-bottom:1px solid var(--border2)}
.field:last-child{border-bottom:none}
.field-label{font-size:.85em;color:var(--text);padding-top:6px;display:flex;flex-direction:column;gap:3px}
.field-label .key{font-family:monospace;font-size:.75em;color:var(--text3)}
.field-label .required{color:var(--red);margin-left:2px}
.field-help{font-size:.75em;color:var(--text2);margin-top:4px}
.field-input{display:flex;flex-direction:column;gap:4px}
.field-error{font-size:.75em;color:var(--red);display:none}
.field-error.show{display:block}

input[type="text"],input[type="password"],input[type="number"],input[type="url"],select,textarea{
  width:100%;background:var(--input-bg);border:1px solid var(--input-border);border-radius:6px;padding:8px 10px;color:var(--text);font-size:.85em;font-family:inherit;outline:none;transition:border-color .15s
}
input:focus,select:focus,textarea:focus{border-color:var(--input-focus)}
input.invalid,select.invalid,textarea.invalid{border-color:var(--red)}
select{cursor:pointer;appearance:auto}
textarea{min-height:120px;resize:vertical;font-family:monospace;font-size:.8em;line-height:1.5}

/* Checkbox / toggle */
.toggle-wrap{display:flex;align-items:center;gap:8px;padding-top:4px}
.toggle{position:relative;width:40px;height:22px;flex-shrink:0}
.toggle input{position:absolute;opacity:0;width:0;height:0;pointer-events:none}
.toggle .slider{position:absolute;inset:0;background:var(--border);border-radius:22px;cursor:pointer;transition:.2s}
.toggle .slider:before{content:'';position:absolute;height:16px;width:16px;left:3px;bottom:3px;background:var(--text2);border-radius:50%;transition:.2s}
.toggle input:checked+.slider{background:var(--green)}
.toggle input:checked+.slider:before{transform:translateX(18px);background:#fff}

/* Checkbox/radio groups */
.check-group{display:flex;flex-wrap:wrap;gap:6px;padding-top:4px}
.check-item{display:flex;align-items:center;gap:6px;padding:5px 10px;background:var(--bg);border:1px solid var(--border2);border-radius:6px;font-size:.83em;cursor:pointer;transition:border-color .15s}
.check-item:hover{border-color:var(--blue)}
.check-item input{accent-color:var(--blue)}
.check-item.checked{border-color:var(--green);background:#3fb9500d}

/* Password field with toggle */
.secret-wrap{display:flex;gap:6px}
.secret-wrap input{flex:1}

/* List inputs */
.list-container{display:flex;flex-direction:column;gap:6px}
.list-row{display:flex;gap:6px;align-items:center}
.list-row input{flex:1}
.list-row .pair-input{display:flex;gap:6px;flex:1}
.list-row .pair-input input{flex:1}
.list-row .btn-icon:hover{border-color:var(--red);color:var(--red)}
.btn-icon.add{color:var(--green)}
.btn-icon.add:hover{border-color:var(--green);color:var(--green)}
.list-labels{display:flex;gap:6px;font-size:.75em;color:var(--text3);margin-bottom:2px}
.list-labels span{flex:1}
.list-labels .spacer{width:28px;flex-shrink:0}

/* Buttons */
.actions{display:flex;gap:10px;margin-top:20px;flex-wrap:wrap}

/* Responsive */
@media(max-width:768px){
  .field{grid-template-columns:1fr;gap:4px}
  .field-label{padding-top:0}
  .tabs{overflow-x:auto}
}

/* Settings spinner has margin-right for inline use */
.spinner{margin-right:6px}

/* OAuth panel */
.oauth-panel{background:var(--bg);border:1px solid var(--blue);border-radius:8px;padding:16px;margin-top:8px}
.oauth-panel .oauth-code{font-size:1.8em;font-weight:700;color:var(--blue);letter-spacing:.15em;font-family:monospace;margin:10px 0}
.oauth-panel .oauth-url{font-size:.85em}
.oauth-panel .oauth-url a{color:var(--blue)}
.oauth-panel .oauth-status{font-size:.8em;color:var(--text2);margin-top:8px}
.btn-oauth{border-color:var(--blue);color:var(--blue);font-size:.8em;margin-top:6px}
.btn-oauth:hover{background:#58a6ff1a}
.btn-cancel{border-color:var(--red);color:var(--red)}
.btn-cancel:hover{background:#f851491a}

/* Version/Quality profile editor */
.preset-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:8px;margin-bottom:12px}
.preset-card{background:var(--bg);border:1px solid var(--border2);border-radius:8px;padding:10px 12px;cursor:pointer;transition:border-color .15s,background .15s}
.preset-card:hover{border-color:var(--blue);background:#58a6ff08}
.preset-card .preset-name{font-size:.85em;font-weight:600;color:var(--text);margin-bottom:3px}
.preset-card .preset-desc{font-size:.72em;color:var(--text2);line-height:1.4}
.profile-list{display:flex;flex-direction:column;gap:8px}
.profile-card{background:var(--bg);border:1px solid var(--border2);border-radius:8px;padding:12px;transition:border-color .15s}
.profile-card.expanded{border-color:var(--blue)}
.profile-header{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.profile-header input[type="text"]{flex:1;min-width:120px;max-width:250px;font-weight:600}
.profile-header .toggle{flex-shrink:0}
.profile-summary{font-size:.75em;color:var(--text2);margin-top:6px;line-height:1.5}
.profile-actions{display:flex;gap:6px;margin-left:auto}
.profile-rules{margin-top:10px;padding-top:10px;border-top:1px solid var(--border2)}
.rule-row{display:flex;gap:6px;align-items:center;margin-bottom:6px;flex-wrap:wrap}
.rule-row select,.rule-row input{font-size:.8em;padding:5px 8px}
.rule-row select{min-width:100px;max-width:150px}
.rule-row input[type="text"]{flex:1;min-width:80px}
.rule-section-label{font-size:.72em;color:var(--text3);text-transform:uppercase;letter-spacing:.05em;margin:8px 0 4px;font-weight:600}
.profile-json{margin-top:10px}
.profile-json textarea{font-size:.75em}
.versions-toolbar{display:flex;gap:8px;margin-top:10px;align-items:center}

/* Tab toolbar */
.tab-toolbar{display:flex;gap:8px;margin-bottom:12px;justify-content:flex-end;flex-wrap:wrap}
[data-theme="light"] .cat-header:hover{background:#f0f3f6}
[data-theme="light"] .toggle .slider{background:var(--border)}
[data-theme="light"] .toggle .slider:before{background:#fff}
[data-theme="light"] .preset-card:hover{background:#0969da08}

/* 4a: Sticky save bar */
.save-bar{position:fixed;bottom:0;left:0;right:0;background:var(--card);border-top:1px solid var(--border);padding:10px 20px;display:flex;align-items:center;gap:12px;z-index:20;box-shadow:0 -4px 12px rgba(0,0,0,.2);justify-content:center;transform:translateY(100%);transition:transform .3s ease,opacity .3s ease;pointer-events:none;opacity:0}
.save-bar.visible{transform:translateY(0);pointer-events:auto;opacity:1}
.save-bar .change-count{font-size:.85em;font-weight:600;color:var(--yellow);white-space:nowrap}
body.has-save-bar{padding-bottom:72px}
body.has-save-bar .footer{margin-bottom:48px}
[data-theme="light"] .save-bar{box-shadow:0 -4px 12px rgba(0,0,0,.08)}

/* 4b: Category modification indicators */
.cat-dirty{display:none;align-items:center;gap:6px;margin-left:auto;margin-right:8px}
.cat-header.has-changes .cat-dirty{display:flex}
.cat-dirty-dot{width:8px;height:8px;border-radius:50%;background:var(--yellow);flex-shrink:0}
.cat-dirty-count{font-size:.72em;color:var(--yellow);font-weight:500;white-space:nowrap}

/* 4c: Search result highlighting */
mark{background:var(--yellow);color:#0d1117;border-radius:2px;padding:0 1px}
[data-theme="light"] mark{background:#fff3bf;color:var(--text)}

/* 4d: Per-field reset */
.field-reset{display:none;align-self:start;align-items:center;justify-content:center;width:28px;height:28px;min-width:28px;border:1px solid var(--border2);border-radius:6px;background:transparent;color:var(--text3);cursor:pointer;font-size:1em;flex-shrink:0;transition:color .15s,border-color .15s;margin-top:4px;padding:0;line-height:1}
.field-reset:hover{color:var(--yellow);border-color:var(--yellow)}
.field.changed .field-reset{display:inline-flex}
.field.changed{background:rgba(210,153,34,.04);border-radius:4px;margin:0 -6px;padding:10px 6px}
</style>

<div class="tabs" role="tablist">
  <div class="tab active" role="tab" tabindex="0" aria-selected="true" aria-controls="tab-env" data-kb="tab-1" onclick="switchTab('env')" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();switchTab('env')}">pd_zurg</div>
  <div class="tab" role="tab" tabindex="0" aria-selected="false" aria-controls="tab-pd" data-kb="tab-2" onclick="switchTab('pd')" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();switchTab('pd')}">plex_debrid</div>
</div>

<div class="banner" id="banner"></div>

<!-- pd_zurg env vars tab -->
<div class="tab-content active" id="tab-env" role="tabpanel">
  <div class="tab-toolbar">
    <a class="btn btn-ghost btn-sm" href="/api/settings/export/env" download=".env">Export .env</a>
    <label class="btn btn-ghost btn-sm" style="cursor:pointer">Import .env<input type="file" accept=".env,text/plain" style="display:none" onchange="envImport(this)"></label>
    <button type="button" class="btn btn-ghost btn-sm" onclick="envResetDefaults()">Reset All to Defaults</button>
  </div>
  <div class="search-bar"><input type="text" id="search-env" data-kb="search" placeholder="Filter settings... (/)" oninput="filterSettings('env',this.value)"><div class="search-count" id="search-env-count"></div></div>
  <div id="env-categories"></div>
  <div class="actions">
    <button type="button" class="btn btn-primary" id="btn-env-save" onclick="envSave()">Save &amp; Apply</button>
    <button type="button" class="btn btn-ghost" id="btn-env-validate" onclick="envValidate()">Validate</button>
    <button type="button" class="btn btn-ghost" onclick="envReset()">Undo Changes</button>
  </div>
</div>

<!-- plex_debrid settings tab -->
<div class="tab-content" id="tab-pd" role="tabpanel">
  <div class="tab-toolbar">
    <a class="btn btn-ghost btn-sm" href="/api/settings/export/plex-debrid" download="settings.json">Export settings.json</a>
    <label class="btn btn-ghost btn-sm" style="cursor:pointer">Import settings.json<input type="file" accept=".json,application/json" style="display:none" onchange="pdImport(this)"></label>
    <button type="button" class="btn btn-ghost btn-sm" onclick="pdResetDefaults()">Reset to Defaults</button>
  </div>
  <div class="search-bar"><input type="text" id="search-pd" data-kb="search" placeholder="Filter settings... (/)" oninput="filterSettings('pd',this.value)"><div class="search-count" id="search-pd-count"></div></div>
  <div id="pd-categories"></div>
  <div class="actions">
    <button type="button" class="btn btn-primary" id="btn-pd-save" onclick="pdSave()">Save &amp; Restart plex_debrid</button>
    <button type="button" class="btn btn-ghost" id="btn-pd-validate" onclick="pdValidate()">Validate</button>
    <button type="button" class="btn btn-ghost" onclick="pdReset()">Undo Changes</button>
  </div>
</div>

<div class="footer">pd_zurg changes apply via SIGHUP reload. plex_debrid changes trigger a service restart.</div>

<div class="save-bar" id="save-bar" role="status" aria-live="polite">
  <span class="change-count" id="save-bar-count"></span>
  <button type="button" class="btn btn-primary btn-sm" id="save-bar-btn" onclick="saveBarSave()">Save &amp; Apply</button>
  <button type="button" class="btn btn-ghost btn-sm" onclick="saveBarDiscard()">Discard</button>
</div>

<script>
__THEME_TOGGLE_JS__

const ENV_SCHEMA = __ENV_SCHEMA_JSON__;
const PD_SCHEMA = __PD_SCHEMA_JSON__;
let envValues = {};
let pdValues = {};
let isDirty = false;  // combined flag for beforeunload
let envDirty = false;
let pdDirty = false;

/* Keyboard shortcut: Escape handler for this page */
window.onKbEscape = function() {
  var envTab = document.getElementById('tab-env');
  var id = envTab && envTab.classList.contains('active') ? 'search-env' : 'search-pd';
  var si = document.getElementById(id);
  if (si && si.value) { si.value = ''; filterSettings(id === 'search-env' ? 'env' : 'pd', ''); }
};

// -----------------------------------------------------------------------
// Shared helpers
// -----------------------------------------------------------------------
function esc(s) {
  const d = document.createElement('div');
  d.appendChild(document.createTextNode(String(s ?? '')));
  return d.innerHTML;
}
function escJs(s) { return String(s ?? '').replace(/\\/g,'\\\\').replace(/'/g,"\\'"); }

let _bannerTimer = null;
function showBanner(type, html) {
  const b = document.getElementById('banner');
  b.className = 'banner ' + type;
  b.innerHTML = html;
  b.scrollIntoView({behavior: 'smooth', block: 'nearest'});
  if (_bannerTimer) clearTimeout(_bannerTimer);
  if (type === 'success') { _bannerTimer = setTimeout(hideBanner, 8000); }
}

function hideBanner() {
  document.getElementById('banner').className = 'banner';
}

function switchTab(name) {
  const curDirty = (activeTabName() === 'env' ? getEnvChangedFields() : getPdChangedFields()).size > 0;
  if (curDirty && !confirm('You have unsaved changes. Switch tabs anyway?')) return;
  document.querySelectorAll('.tab').forEach(t => { t.classList.remove('active'); t.setAttribute('aria-selected', 'false'); });
  document.querySelectorAll('.tab-content').forEach(t => { t.classList.remove('active'); });
  if (name === 'env') {
    const tab = document.querySelector('.tab:nth-child(1)');
    tab.classList.add('active'); tab.setAttribute('aria-selected', 'true');
    document.getElementById('tab-env').classList.add('active');
  } else {
    const tab = document.querySelector('.tab:nth-child(2)');
    tab.classList.add('active'); tab.setAttribute('aria-selected', 'true');
    document.getElementById('tab-pd').classList.add('active');
  }
  hideBanner();
  updateDirtyUI();
}

function toggleCategory(header) {
  header.classList.toggle('open');
  header.nextElementSibling.classList.toggle('open');
  header.setAttribute('aria-expanded', header.classList.contains('open'));
}

function toggleSecret(btn) {
  const input = btn.previousElementSibling;
  if (input.type === 'password') { input.type = 'text'; btn.textContent = 'Hide'; }
  else { input.type = 'password'; btn.textContent = 'Show'; }
}

function toggleAdvanced(el) {
  const fields = el.nextElementSibling;
  fields.classList.toggle('open');
  el.textContent = fields.classList.contains('open') ? 'Hide advanced settings' : 'Show advanced settings';
}

function setButtonLoading(id, loading, text) {
  const btn = document.getElementById(id);
  if (!btn) return;
  btn.disabled = loading;
  btn.innerHTML = loading ? '<span class="spinner"></span>' + esc(text || 'Working...') : (text || btn.textContent);
}

// -----------------------------------------------------------------------
// pd_zurg env var tab
// -----------------------------------------------------------------------
function renderEnvField(field, value) {
  const id = 'env-' + field.key;
  let inputHtml = '';

  if (field.type === 'boolean') {
    const isTrue = String(value).toLowerCase() === 'true';
    const checked = isTrue ? ' checked' : '';
    inputHtml = `<div class="toggle-wrap"><label class="toggle"><input type="checkbox" id="${id}" data-key="${esc(field.key)}" data-type="boolean"${checked} aria-label="${esc(field.label)}"><span class="slider"></span></label></div>`;
  } else if (field.type === 'secret') {
    inputHtml = `<div class="secret-wrap"><input type="password" id="${id}" data-key="${esc(field.key)}" data-type="secret" value="${esc(value || '')}"><button type="button" class="btn btn-ghost btn-sm" onclick="toggleSecret(this)">Show</button></div>`;
  } else if (field.type.startsWith('select:')) {
    const options = field.type.slice(7).split(',');
    let opts = '<option value="">— select —</option>';
    options.forEach(o => {
      const sel = (value || '').toLowerCase() === o.toLowerCase() ? ' selected' : '';
      opts += `<option value="${esc(o)}"${sel}>${esc(o)}</option>`;
    });
    inputHtml = `<select id="${id}" data-key="${esc(field.key)}" data-type="select">${opts}</select>`;
  } else if (field.type.startsWith('number:')) {
    const range = field.type.slice(7).split('-');
    inputHtml = `<input type="number" id="${id}" data-key="${esc(field.key)}" data-type="number" value="${esc(value || '')}" min="${range[0]}" max="${range[1]}" placeholder="${range[0]}-${range[1]}">`;
  } else if (field.type === 'url') {
    inputHtml = `<input type="url" id="${id}" data-key="${esc(field.key)}" data-type="url" value="${esc(value || '')}" placeholder="http://...">`;
  } else {
    inputHtml = `<input type="text" id="${id}" data-key="${esc(field.key)}" data-type="string" value="${esc(value || '')}">`;
  }

  const helpHtml = field.help ? `<div class="field-help">${esc(field.help)}</div>` : '';
  const reqMark = field.required ? '<span class="required">*</span>' : '';
  const resetBtn = `<button type="button" class="field-reset" onclick="resetField('env','${escJs(field.key)}')" title="Undo change">\u21BA</button>`;

  return `<div class="field" id="row-${field.key}" data-field-key="${esc(field.key)}"><div class="field-label"><span><span class="label-text">${esc(field.label)}</span>${reqMark}</span><span class="key">${esc(field.key)}</span></div><div class="field-input"><div style="display:flex;gap:6px;align-items:start">${inputHtml}${resetBtn}</div>${helpHtml}<div class="field-error" id="err-${field.key}"></div></div></div>`;
}

function renderEnvCategories(values) {
  const container = document.getElementById('env-categories');
  let html = '';
  ENV_SCHEMA.categories.forEach((cat, i) => {
    const openClass = i === 0 ? ' open' : '';
    let fieldsHtml = '';
    cat.fields.forEach(f => { fieldsHtml += renderEnvField(f, values[f.key] || ''); });
    const expanded = i === 0 ? 'true' : 'false';
    html += `<div class="category" data-cat-idx="${i}" data-tab="env"><div class="cat-header${openClass}" role="button" tabindex="0" aria-expanded="${expanded}" onclick="toggleCategory(this)" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();toggleCategory(this)}"><h2><span class="cat-name">${esc(cat.name)}</span> <span class="desc">\u2014 ${esc(cat.description)}</span></h2><span class="cat-dirty"><span class="cat-dirty-dot"></span><span class="cat-dirty-count"></span></span><span class="arrow" aria-hidden="true">&#9660;</span></div><div class="cat-body${openClass}">${fieldsHtml}</div></div>`;
  });
  container.innerHTML = html;
}

function collectEnvData() {
  const data = {};
  document.querySelectorAll('#tab-env [data-key]').forEach(el => {
    data[el.dataset.key] = el.dataset.type === 'boolean' ? (el.checked ? 'true' : 'false') : el.value;
  });
  return data;
}

function clearFieldErrors(container) {
  (container || document).querySelectorAll('.field-error').forEach(el => { el.className = 'field-error'; el.textContent = ''; });
  (container || document).querySelectorAll('.invalid').forEach(el => { el.classList.remove('invalid'); });
}

function highlightErrors(errors) {
  errors.forEach(msg => {
    const match = msg.match(/^["']?([A-Z_]+)[=:'"]/);
    if (match) {
      const errEl = document.getElementById('err-' + match[1]);
      if (errEl) { errEl.textContent = msg; errEl.className = 'field-error show'; }
      const input = document.getElementById('env-' + match[1]);
      if (input) input.classList.add('invalid');
    }
  });
}

async function envValidate() {
  const tab = document.getElementById('tab-env');
  clearFieldErrors(tab);
  hideBanner();
  setButtonLoading('btn-env-validate', true, 'Validating...');
  try {
    const resp = await fetch('/api/settings/validate', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(collectEnvData()) });
    const result = await resp.json();
    if (result.errors && result.errors.length) {
      showBanner('error', '<strong>Validation failed:</strong><br>' + result.errors.map(e => '&bull; ' + esc(e)).join('<br>') + (result.warnings && result.warnings.length ? '<br><br><strong>Warnings:</strong><br>' + result.warnings.map(w => '&bull; ' + esc(w)).join('<br>') : ''));
      highlightErrors(result.errors);
    } else if (result.warnings && result.warnings.length) {
      showBanner('warning', '<strong>Passed with warnings:</strong><br>' + result.warnings.map(w => '&bull; ' + esc(w)).join('<br>'));
    } else {
      showBanner('success', 'Validation passed');
    }
  } catch (e) { showBanner('error', 'Failed: ' + esc(e.message)); }
  finally { setButtonLoading('btn-env-validate', false, 'Validate'); }
}

async function envSave() {
  const tab = document.getElementById('tab-env');
  clearFieldErrors(tab);
  hideBanner();
  setButtonLoading('btn-env-save', true, 'Saving...');
  try {
    const resp = await fetch('/api/settings/env', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(collectEnvData()) });
    const result = await resp.json();
    if (result.status === 'error') {
      showBanner('error', '<strong>Save failed:</strong><br>' + result.errors.map(e => '&bull; ' + esc(e)).join('<br>'));
      highlightErrors(result.errors);
      showToast('Save failed: ' + result.errors.length + ' error(s)', 'error');
    } else if (result.status === 'saved') {
      let html = '<strong>Settings saved and applied!</strong>';
      if (result.restarted && result.restarted.length) html += '<br>Services restarting: ' + result.restarted.map(s => esc(s)).join(', ');
      if (result.warnings && result.warnings.length) html += '<br><br><strong>Warnings:</strong><br>' + result.warnings.map(w => '&bull; ' + esc(w)).join('<br>');
      showBanner('success', html);
      showToast('Settings saved and applied!', 'success');
      envValues = collectEnvData();
      envDirty = false;
      updateDirtyUI();
    } else {
      showBanner('warning', '<strong>Saved</strong> (reload failed \u2014 restart container to apply)');
      showToast('Saved (reload failed \u2014 restart container)', 'warning');
    }
  } catch (e) { showBanner('error', 'Failed: ' + esc(e.message)); showToast('Save failed', 'error'); }
  finally { setButtonLoading('btn-env-save', false, 'Save & Apply'); }
}

function envReset() { clearFieldErrors(document.getElementById('tab-env')); hideBanner(); renderEnvCategories(envValues); updateDirtyUI(); }

// -----------------------------------------------------------------------
// plex_debrid settings tab
// -----------------------------------------------------------------------

function pdFieldId(key) { return 'pd-' + key.replace(/[^a-zA-Z0-9]/g, '_'); }

function renderPdField(field, value) {
  const id = pdFieldId(field.key);
  let inputHtml = '';

  switch (field.type) {
    case 'multiselect': {
      const selected = Array.isArray(value) ? value : [];
      let items = '';
      (field.options || []).forEach(opt => {
        const chk = selected.includes(opt) ? ' checked' : '';
        const cls = selected.includes(opt) ? ' checked' : '';
        items += `<label class="check-item${cls}"><input type="checkbox" data-pdkey="${esc(field.key)}" data-pdtype="multiselect" value="${esc(opt)}"${chk}>${esc(opt)}</label>`;
      });
      inputHtml = `<div class="check-group" id="${id}">${items}</div>`;
      break;
    }
    case 'radio': {
      const selected = Array.isArray(value) && value.length ? value[0] : '';
      let items = '';
      (field.options || []).forEach(opt => {
        const chk = selected === opt ? ' checked' : '';
        const cls = selected === opt ? ' checked' : '';
        items += `<label class="check-item${cls}"><input type="radio" name="${id}" data-pdkey="${esc(field.key)}" data-pdtype="radio" value="${esc(opt)}"${chk}>${esc(opt)}</label>`;
      });
      inputHtml = `<div class="check-group" id="${id}">${items}</div>`;
      break;
    }
    case 'boolean_str': {
      const isTrue = String(value).toLowerCase() === 'true';
      const checked = isTrue ? ' checked' : '';
      inputHtml = `<div class="toggle-wrap"><label class="toggle"><input type="checkbox" id="${id}" data-pdkey="${esc(field.key)}" data-pdtype="boolean_str"${checked} aria-label="${esc(field.label)}"><span class="slider"></span></label></div>`;
      break;
    }
    case 'secret': {
      inputHtml = `<div class="secret-wrap"><input type="password" id="${id}" data-pdkey="${esc(field.key)}" data-pdtype="secret" value="${esc(value || '')}"><button type="button" class="btn btn-ghost btn-sm" onclick="toggleSecret(this)">Show</button></div>`;
      if (field.oauth) {
        inputHtml += `<button type="button" class="btn btn-ghost btn-oauth" onclick="oauthConnect('${esc(field.oauth)}','${id}')" id="oauth-btn-${id}">Connect ${esc(field.label.replace(' API Key','').replace(' Key',''))}</button><div id="oauth-panel-${id}"></div>`;
      }
      break;
    }
    case 'select': {
      let opts = '<option value="">— select —</option>';
      (field.options || []).forEach(o => {
        const sel = (value || '').toLowerCase() === o.toLowerCase() ? ' selected' : '';
        opts += `<option value="${esc(o)}"${sel}>${esc(o)}</option>`;
      });
      inputHtml = `<select id="${id}" data-pdkey="${esc(field.key)}" data-pdtype="select">${opts}</select>`;
      break;
    }
    case 'list_strings': {
      const items = Array.isArray(value) ? value : [];
      let rows = '';
      items.forEach((v, i) => {
        rows += `<div class="list-row"><input type="text" value="${esc(v)}" data-pdkey="${esc(field.key)}" data-pdtype="list_strings"><button type="button" class="btn btn-ghost btn-icon" onclick="removeListRow(this)" title="Remove">&times;</button></div>`;
      });
      inputHtml = `<div class="list-container" id="${id}">${rows}<button type="button" class="btn btn-ghost btn-icon add" onclick="addListStringRow(this.parentElement,'${esc(field.key)}')" title="Add">+</button></div>`;
      break;
    }
    case 'list_pairs': {
      const items = Array.isArray(value) ? value : [];
      const cols = field.options || ['Column 1', 'Column 2'];
      let labels = `<div class="list-labels"><span>${esc(cols[0])}</span><span>${esc(cols[1])}</span><span class="spacer"></span></div>`;
      let rows = '';
      items.forEach((pair, i) => {
        const a = Array.isArray(pair) ? (pair[0] || '') : '';
        const b = Array.isArray(pair) ? (pair[1] || '') : '';
        rows += `<div class="list-row"><div class="pair-input"><input type="text" value="${esc(a)}" placeholder="${esc(cols[0])}"><input type="text" value="${esc(b)}" placeholder="${esc(cols[1])}"></div><button type="button" class="btn btn-ghost btn-icon" onclick="removeListRow(this)" title="Remove">&times;</button></div>`;
      });
      inputHtml = `<div class="list-container" id="${id}" data-pdkey="${esc(field.key)}" data-pdtype="list_pairs" data-cols="${esc(JSON.stringify(cols))}">${labels}${rows}<button type="button" class="btn btn-ghost btn-icon add" onclick="addListPairRow(this.parentElement)" title="Add">+</button></div>`;
      if (field.oauth) {
        inputHtml += `<button type="button" class="btn btn-ghost btn-oauth" onclick="oauthConnectPair('${esc(field.oauth)}','${id}')" id="oauth-btn-${id}">Connect via OAuth</button><div id="oauth-panel-${id}"></div>`;
      }
      break;
    }
    case 'json': {
      if (field.key === 'Versions') {
        // Render the visual quality profile editor
        inputHtml = renderVersionsEditor(id, field.key, value);
      } else {
        const jsonStr = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
        inputHtml = `<textarea id="${id}" data-pdkey="${esc(field.key)}" data-pdtype="json" rows="8">${esc(jsonStr)}</textarea>`;
      }
      break;
    }
    case 'hidden':
      return ''; // Don't render hidden fields
    default: {
      inputHtml = `<input type="text" id="${id}" data-pdkey="${esc(field.key)}" data-pdtype="string" value="${esc(value || '')}">`;
    }
  }

  const helpHtml = field.help ? `<div class="field-help">${esc(field.help)}</div>` : '';
  const resetBtn = `<button type="button" class="field-reset" onclick="resetField('pd','${escJs(field.key)}')" title="Undo change">\u21BA</button>`;

  return `<div class="field" id="pdrow-${id}" data-field-key="${esc(field.key)}"><div class="field-label"><span><span class="label-text">${esc(field.label)}</span></span><span class="key">${esc(field.key)}</span></div><div class="field-input"><div style="display:flex;gap:6px;align-items:start">${inputHtml}${resetBtn}</div>${helpHtml}<div class="field-error" id="pderr-${id}"></div></div></div>`;
}

function renderPdCategories(values) {
  const container = document.getElementById('pd-categories');
  let html = '';
  PD_SCHEMA.categories.forEach((cat, i) => {
    const openClass = i === 0 ? ' open' : '';
    let mainFields = '';
    let advFields = '';
    let hasAdvanced = false;

    cat.fields.forEach(f => {
      const rendered = renderPdField(f, values[f.key]);
      if (!rendered) return;
      if (f.hidden) {
        advFields += rendered;
        hasAdvanced = true;
      } else {
        mainFields += rendered;
      }
    });

    let advHtml = '';
    if (hasAdvanced) {
      advHtml = `<div class="advanced-toggle" onclick="toggleAdvanced(this)">Show advanced settings</div><div class="advanced-fields">${advFields}</div>`;
    }

    const expanded = i === 0 ? 'true' : 'false';
    html += `<div class="category" data-cat-idx="${i}" data-tab="pd"><div class="cat-header${openClass}" role="button" tabindex="0" aria-expanded="${expanded}" onclick="toggleCategory(this)" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();toggleCategory(this)}"><h2><span class="cat-name">${esc(cat.name)}</span> <span class="desc">\u2014 ${esc(cat.description)}</span></h2><span class="cat-dirty"><span class="cat-dirty-dot"></span><span class="cat-dirty-count"></span></span><span class="arrow" aria-hidden="true">&#9660;</span></div><div class="cat-body${openClass}">${mainFields}${advHtml}</div></div>`;
  });
  container.innerHTML = html;

  // Wire up checkbox/radio styling
  container.querySelectorAll('.check-item input').forEach(inp => {
    inp.addEventListener('change', function() {
      if (this.type === 'radio') {
        this.closest('.check-group').querySelectorAll('.check-item').forEach(ci => ci.classList.remove('checked'));
      }
      this.closest('.check-item').classList.toggle('checked', this.checked);
    });
  });
}

// List manipulation
function addListStringRow(container, key) {
  const addBtn = container.querySelector('.btn-icon.add');
  const row = document.createElement('div');
  row.className = 'list-row';
  row.innerHTML = `<input type="text" value="" data-pdkey="${esc(key)}" data-pdtype="list_strings"><button type="button" class="btn btn-ghost btn-icon" onclick="removeListRow(this)" title="Remove">&times;</button>`;
  container.insertBefore(row, addBtn);
  row.querySelector('input').focus();
}

function addListPairRow(container) {
  const addBtn = container.querySelector('.btn-icon.add');
  const cols = JSON.parse(container.dataset.cols || '["",""]');
  const row = document.createElement('div');
  row.className = 'list-row';
  row.innerHTML = `<div class="pair-input"><input type="text" value="" placeholder="${esc(cols[0])}"><input type="text" value="" placeholder="${esc(cols[1])}"></div><button type="button" class="btn btn-ghost btn-icon" onclick="removeListRow(this)" title="Remove">&times;</button>`;
  container.insertBefore(row, addBtn);
  row.querySelector('input').focus();
}

function removeListRow(btn) {
  btn.closest('.list-row').remove();
}

// -----------------------------------------------------------------------
// Versions / Quality Profile Editor
// -----------------------------------------------------------------------
let _versionsData = []; // Current profiles array, kept in sync
let _expandedProfile = -1; // Which profile has rules visible (-1 = none)

const _ruleFields = PD_SCHEMA.version_editor ? PD_SCHEMA.version_editor.rule_fields : {};
const _ruleWeights = PD_SCHEMA.version_editor ? PD_SCHEMA.version_editor.rule_weights : [];
const _condFields = PD_SCHEMA.version_editor ? PD_SCHEMA.version_editor.condition_fields : {};
const _presets = PD_SCHEMA.version_presets || {};

function summarizeProfile(profile) {
  if (!Array.isArray(profile) || profile.length < 4) return 'Invalid profile';
  const conditions = profile[1] || [];
  const rules = profile[3] || [];
  const parts = [];
  // Summarize conditions
  conditions.forEach(c => {
    if (c[0] === 'media type' && c[1] !== 'all') parts.push(c[1]);
  });
  // Summarize rules
  rules.forEach(r => {
    if (r[0] === 'resolution' && r[1] === 'requirement' && (r[2] === '<=' || r[2] === '=='))
      parts.push('Up to ' + r[3] + 'p');
    if (r[0] === 'cache status' && r[1] === 'requirement' && r[2] === 'cached')
      parts.push('Cached');
    if (r[0] === 'bitrate' && r[1] === 'requirement')
      parts.push('Bitrate ' + r[2] + ' ' + r[3] + ' Mbit/s');
    if (r[0] === 'size' && r[1] === 'requirement' && r[2] === '<=')
      parts.push('Max ' + r[3] + 'GB');
    if (r[0] === 'title' && r[1] === 'requirement' && r[2] === 'exclude') {
      if (r[3].includes('CAM')) parts.push('No CAM/TS');
      else if (r[3].includes('HDR')) parts.push('No HDR');
      else if (r[3].includes('DV') || r[3].includes('DOVI')) parts.push('No DV');
    }
    if (r[0] === 'title' && r[1] === 'preference' && r[2] === 'include') {
      if (r[3].includes('HDR')) parts.push('Prefer HDR/DV');
      else if (r[3].toLowerCase().includes('x265') || r[3].toLowerCase().includes('hevc')) parts.push('Prefer x265');
    }
  });
  if (!parts.length) parts.push(rules.length + ' rules');
  return parts.join(', ');
}

function renderRuleRow(rule, idx, profileIdx) {
  const field = rule[0] || '';
  const weight = rule[1] || 'requirement';
  const op = rule[2] || '';
  const val = rule[3] || '';

  let fieldOpts = '';
  Object.keys(_ruleFields).forEach(f => {
    fieldOpts += `<option value="${esc(f)}"${f===field?' selected':''}>${esc(f)}</option>`;
  });

  let weightOpts = '';
  _ruleWeights.forEach(w => {
    weightOpts += `<option value="${esc(w)}"${w===weight?' selected':''}>${esc(w)}</option>`;
  });

  const fieldMeta = _ruleFields[field] || {operators:[], has_value:true};
  let opOpts = '';
  fieldMeta.operators.forEach(o => {
    opOpts += `<option value="${esc(o)}"${o===op?' selected':''}>${esc(o)}</option>`;
  });

  const valInput = fieldMeta.has_value !== false
    ? `<input type="text" value="${esc(val)}" placeholder="${esc(fieldMeta.unit||fieldMeta.value_type||'value')}" onchange="updateRule(${profileIdx},${idx},this.closest('.rule-row'))">`
    : '';

  return `<div class="rule-row" data-ridx="${idx}">
    <select onchange="ruleFieldChanged(${profileIdx},${idx},this)">${fieldOpts}</select>
    <select onchange="updateRule(${profileIdx},${idx},this.closest('.rule-row'))">${weightOpts}</select>
    <select onchange="updateRule(${profileIdx},${idx},this.closest('.rule-row'))">${opOpts}</select>
    ${valInput}
    <button type="button" class="btn btn-ghost btn-icon" onclick="deleteRule(${profileIdx},${idx})" title="Remove">&times;</button>
  </div>`;
}

function renderConditionRow(cond, idx, profileIdx) {
  const field = cond[0] || '';
  const op = cond[1] || '';
  const val = cond[2] || '';

  let fieldOpts = '';
  Object.keys(_condFields).forEach(f => {
    fieldOpts += `<option value="${esc(f)}"${f===field?' selected':''}>${esc(f)}</option>`;
  });

  const meta = _condFields[field] || {operators:[], has_value:true};
  let opOpts = '';
  meta.operators.forEach(o => {
    opOpts += `<option value="${esc(o)}"${o===op?' selected':''}>${esc(o)}</option>`;
  });

  const valInput = meta.has_value !== false
    ? `<input type="text" value="${esc(val)}" placeholder="value" onchange="updateCondition(${profileIdx},${idx},this.closest('.rule-row'))">`
    : '';

  return `<div class="rule-row" data-cidx="${idx}">
    <select onchange="condFieldChanged(${profileIdx},${idx},this)">${fieldOpts}</select>
    <select onchange="updateCondition(${profileIdx},${idx},this.closest('.rule-row'))">${opOpts}</select>
    ${valInput}
    <button type="button" class="btn btn-ghost btn-icon" onclick="deleteCondition(${profileIdx},${idx})" title="Remove">&times;</button>
  </div>`;
}

function renderProfileCard(profile, idx) {
  const name = profile[0] || 'Unnamed';
  const lang = profile[2] || 'en';
  const summary = summarizeProfile(profile);
  const conditions = profile[1] || [];
  const rules = profile[3] || [];

  let condHtml = '';
  conditions.forEach((c, ci) => { condHtml += renderConditionRow(c, ci, idx); });

  let rulesHtml = '';
  rules.forEach((r, ri) => { rulesHtml += renderRuleRow(r, ri, idx); });

  // Language options
  const langs = ['en','de','fr','es','it','pt','nl','pl','ru','ja','ko','zh','ar','hi',''];
  let langOpts = '';
  langs.forEach(l => {
    const label = l || 'any';
    langOpts += `<option value="${esc(l)}"${l===lang?' selected':''}>${esc(label)}</option>`;
  });

  return `<div class="profile-card" id="profile-${idx}">
    <div class="profile-header">
      <input type="text" value="${esc(name)}" onchange="_versionsData[${idx}][0]=this.value" placeholder="Profile name">
      <select style="max-width:70px;font-size:.8em" onchange="_versionsData[${idx}][2]=this.value" title="Language">${langOpts}</select>
      <div class="profile-actions">
        <button type="button" class="btn btn-ghost btn-sm" id="profile-edit-btn-${idx}" onclick="toggleProfileRules(${idx})">${_expandedProfile===idx?'Close':'Edit'}</button>
        <button type="button" class="btn btn-ghost btn-sm" onclick="duplicateProfile(${idx})">Duplicate</button>
        <button type="button" class="btn btn-ghost btn-icon" onclick="deleteProfile(${idx})" title="Delete profile">&times;</button>
      </div>
    </div>
    <div class="profile-summary">${esc(summary)}</div>
    <div class="profile-rules" id="profile-rules-${idx}" style="display:${_expandedProfile===idx?'block':'none'}">
      <div class="rule-section-label">Conditions (${conditions.length})</div>
      ${condHtml}
      <button type="button" class="btn btn-ghost btn-sm" onclick="addCondition(${idx})" style="margin-top:4px;color:var(--green);border-color:var(--green)">+ Add Condition</button>
      <div class="rule-section-label" style="margin-top:12px">Rules (${rules.length})</div>
      ${rulesHtml}
      <button type="button" class="btn btn-ghost btn-sm" onclick="addRule(${idx})" style="margin-top:4px;color:var(--green);border-color:var(--green)">+ Add Rule</button>
    </div>
  </div>`;
}

function renderVersionsEditor(id, key, value) {
  _versionsData = Array.isArray(value) ? JSON.parse(JSON.stringify(value)) : [];

  // Preset buttons
  let presetsHtml = '<div class="preset-grid">';
  Object.keys(_presets).forEach(k => {
    const p = _presets[k];
    presetsHtml += `<div class="preset-card" onclick="addPreset('${esc(k)}')"><div class="preset-name">${esc(p.name)}</div><div class="preset-desc">${esc(p.description)}</div></div>`;
  });
  presetsHtml += '</div>';

  // Profile cards
  let profilesHtml = '<div class="profile-list" id="versions-profiles">';
  _versionsData.forEach((p, i) => { profilesHtml += renderProfileCard(p, i); });
  profilesHtml += '</div>';

  // Toolbar
  const toolbarHtml = `<div class="versions-toolbar">
    <button type="button" class="btn btn-ghost btn-sm" onclick="addEmptyProfile()" style="color:var(--green);border-color:var(--green)">+ New Profile</button>
    <button type="button" class="btn btn-ghost btn-sm" id="versions-json-btn" onclick="toggleVersionsJson()">Edit as JSON</button>
  </div>
  <div id="versions-json-editor" style="display:none;margin-top:8px">
    <textarea id="versions-json-textarea" data-pdkey="${esc(key)}" data-pdtype="json" rows="12">${esc(JSON.stringify(_versionsData, null, 2))}</textarea>
    <button type="button" class="btn btn-ghost btn-sm" onclick="applyVersionsJson()" style="margin-top:4px">Apply JSON</button>
  </div>`;

  return `<div id="${id}" data-pdkey="${esc(key)}" data-pdtype="versions">
    <div style="font-size:.8em;color:var(--text2);margin-bottom:8px">Add a preset or build your own profile:</div>
    ${presetsHtml}${profilesHtml}${toolbarHtml}
  </div>`;
}

function refreshVersionsUI() {
  const container = document.getElementById('versions-profiles');
  if (!container) return;
  let h = '';
  _versionsData.forEach((p, i) => { h += renderProfileCard(p, i); });
  container.innerHTML = h;
  // Update JSON textarea if visible
  const ta = document.getElementById('versions-json-textarea');
  if (ta) ta.value = JSON.stringify(_versionsData, null, 2);
  updateDirtyUI();
}

function addPreset(key) {
  const preset = _presets[key];
  if (!preset) return;
  // Deep clone the preset profile
  _versionsData.push(JSON.parse(JSON.stringify(preset.profile)));
  refreshVersionsUI();
}

function addEmptyProfile() {
  _versionsData.push([
    'New Profile',
    [['retries', '<=', '48'], ['media type', 'all', '']],
    'en',
    [['cache status', 'requirement', 'cached', '']]
  ]);
  _expandedProfile = _versionsData.length - 1;
  refreshVersionsUI();
}

function deleteProfile(idx) {
  if (!confirm('Delete profile "' + (_versionsData[idx]?.[0] || '') + '"?')) return;
  _versionsData.splice(idx, 1);
  refreshVersionsUI();
}

function toggleProfileRules(idx) {
  const el = document.getElementById('profile-rules-' + idx);
  const card = document.getElementById('profile-' + idx);
  const btn = document.getElementById('profile-edit-btn-' + idx);
  if (!el) return;
  const show = el.style.display === 'none';
  el.style.display = show ? 'block' : 'none';
  if (card) card.classList.toggle('expanded', show);
  if (btn) btn.textContent = show ? 'Close' : 'Edit';
  _expandedProfile = show ? idx : -1;
}

function addRule(profileIdx) {
  _versionsData[profileIdx][3].push(['resolution', 'requirement', '<=', '1080']);
  _expandedProfile = profileIdx;
  refreshVersionsUI();
}

function deleteRule(profileIdx, ruleIdx) {
  _versionsData[profileIdx][3].splice(ruleIdx, 1);
  _expandedProfile = profileIdx;
  refreshVersionsUI();
}

function updateRule(profileIdx, ruleIdx, row) {
  const selects = row.querySelectorAll('select');
  const input = row.querySelector('input[type="text"]');
  const field = selects[0]?.value || '';
  const weight = selects[1]?.value || '';
  const op = selects[2]?.value || '';
  const val = input?.value || '';
  _versionsData[profileIdx][3][ruleIdx] = [field, weight, op, val];
  updateDirtyUI();
}

function ruleFieldChanged(profileIdx, ruleIdx, select) {
  const field = select.value;
  const meta = _ruleFields[field] || {operators:[]};
  _versionsData[profileIdx][3][ruleIdx] = [field, 'requirement', meta.operators[0] || '', ''];
  _expandedProfile = profileIdx;
  refreshVersionsUI();
}

// Condition manipulation
function addCondition(profileIdx) {
  _versionsData[profileIdx][1].push(['media type', 'all', '']);
  _expandedProfile = profileIdx;
  refreshVersionsUI();
}

function deleteCondition(profileIdx, condIdx) {
  _versionsData[profileIdx][1].splice(condIdx, 1);
  _expandedProfile = profileIdx;
  refreshVersionsUI();
}

function updateCondition(profileIdx, condIdx, row) {
  const selects = row.querySelectorAll('select');
  const input = row.querySelector('input[type="text"]');
  _versionsData[profileIdx][1][condIdx] = [
    selects[0]?.value || '',
    selects[1]?.value || '',
    input?.value || ''
  ];
  updateDirtyUI();
}

function condFieldChanged(profileIdx, condIdx, select) {
  const field = select.value;
  const meta = _condFields[field] || {operators:[]};
  _versionsData[profileIdx][1][condIdx] = [field, meta.operators[0] || '', ''];
  _expandedProfile = profileIdx;
  refreshVersionsUI();
  const el = document.getElementById('profile-rules-' + profileIdx);
  if (el) { el.style.display = 'block'; document.getElementById('profile-' + profileIdx)?.classList.add('expanded'); }
}

function duplicateProfile(idx) {
  const copy = JSON.parse(JSON.stringify(_versionsData[idx]));
  copy[0] = copy[0] + ' (copy)';
  _versionsData.splice(idx + 1, 0, copy);
  refreshVersionsUI();
}

function toggleVersionsJson() {
  const el = document.getElementById('versions-json-editor');
  const btn = document.getElementById('versions-json-btn');
  if (!el) return;
  const show = el.style.display === 'none';
  el.style.display = show ? 'block' : 'none';
  if (btn) btn.textContent = show ? 'Close JSON' : 'Edit as JSON';
  if (show) {
    document.getElementById('versions-json-textarea').value = JSON.stringify(_versionsData, null, 2);
  }
}

function applyVersionsJson() {
  const ta = document.getElementById('versions-json-textarea');
  try {
    const parsed = JSON.parse(ta.value);
    if (!Array.isArray(parsed)) { showToast('Versions must be a JSON array', 'error'); return; }
    _versionsData = parsed;
    refreshVersionsUI();
    showBanner('success', 'JSON applied to profile editor');
  } catch (e) {
    showToast('Invalid JSON: ' + e.message, 'error');
  }
}

function collectPdData() {
  const data = {};
  // Multiselect
  const multiKeys = new Set();
  document.querySelectorAll('#tab-pd [data-pdtype="multiselect"]').forEach(inp => {
    const key = inp.dataset.pdkey;
    if (!multiKeys.has(key)) { multiKeys.add(key); data[key] = []; }
    if (inp.checked) data[key].push(inp.value);
  });
  // Radio
  document.querySelectorAll('#tab-pd [data-pdtype="radio"]:checked').forEach(inp => {
    data[inp.dataset.pdkey] = [inp.value];
  });
  // Ensure radio keys exist even if nothing selected
  document.querySelectorAll('#tab-pd [data-pdtype="radio"]').forEach(inp => {
    if (!(inp.dataset.pdkey in data)) data[inp.dataset.pdkey] = [];
  });
  // Boolean string
  document.querySelectorAll('#tab-pd [data-pdtype="boolean_str"]').forEach(inp => {
    data[inp.dataset.pdkey] = inp.checked ? 'true' : 'false';
  });
  // String, secret, select
  document.querySelectorAll('#tab-pd [data-pdtype="string"], #tab-pd [data-pdtype="secret"], #tab-pd [data-pdtype="select"]').forEach(inp => {
    data[inp.dataset.pdkey] = inp.value;
  });
  // List of strings
  const listKeys = new Set();
  document.querySelectorAll('#tab-pd [data-pdtype="list_strings"]').forEach(inp => {
    const key = inp.dataset.pdkey;
    if (!listKeys.has(key)) { listKeys.add(key); data[key] = []; }
    if (inp.value.trim()) data[key].push(inp.value.trim());
  });
  // List of pairs
  document.querySelectorAll('#tab-pd [data-pdtype="list_pairs"]').forEach(container => {
    const key = container.dataset.pdkey;
    data[key] = [];
    container.querySelectorAll('.list-row').forEach(row => {
      const inputs = row.querySelectorAll('input');
      if (inputs.length >= 2) {
        data[key].push([inputs[0].value, inputs[1].value]);
      }
    });
  });
  // Versions (visual editor)
  document.querySelectorAll('#tab-pd [data-pdtype="versions"]').forEach(el => {
    data[el.dataset.pdkey] = JSON.parse(JSON.stringify(_versionsData));
  });
  // JSON (other json fields, excluding versions textarea)
  document.querySelectorAll('#tab-pd [data-pdtype="json"]').forEach(ta => {
    if (ta.id === 'versions-json-textarea') return; // Handled above
    try {
      data[ta.dataset.pdkey] = JSON.parse(ta.value);
    } catch (e) {
      data[ta.dataset.pdkey] = ta.value; // Will fail validation
    }
  });
  // Preserve hidden/version fields from original values
  if (pdValues.version !== undefined && !('version' in data)) {
    data.version = pdValues.version;
  }
  // Preserve Watchlist loop interval if not collected (it's a string field)
  return data;
}

async function pdValidate() {
  clearFieldErrors(document.getElementById('tab-pd'));
  hideBanner();
  setButtonLoading('btn-pd-validate', true, 'Validating...');
  try {
    const data = collectPdData();
    // Client-side type checks first
    let errors = []; let warnings = [];
    PD_SCHEMA.categories.forEach(cat => cat.fields.forEach(f => {
      const v = data[f.key];
      if (f.type === 'multiselect' || f.type === 'radio' || f.type === 'list_strings' || f.type === 'list_pairs') {
        if (v !== undefined && !Array.isArray(v)) errors.push(`"${f.key}" must be a list`);
      }
      if (f.type === 'json' && f.key === 'Versions' && v !== undefined && !Array.isArray(v)) errors.push('"Versions" must be a list');
    }));
    if (errors.length) {
      showBanner('error', '<strong>Validation failed:</strong><br>' + errors.map(e => '&bull; ' + esc(e)).join('<br>'));
    } else {
      showBanner('success', 'Validation passed \u2014 no structural errors found');
    }
  } catch (e) { showBanner('error', 'Validation error: ' + esc(e.message)); }
  finally { setButtonLoading('btn-pd-validate', false, 'Validate'); }
}

async function pdSave() {
  if (!confirm('This will save settings and restart plex_debrid. Active downloads may be interrupted. Continue?')) return;
  clearFieldErrors(document.getElementById('tab-pd'));
  hideBanner();
  setButtonLoading('btn-pd-save', true, 'Saving...');
  try {
    const data = collectPdData();
    const resp = await fetch('/api/settings/plex-debrid', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data) });
    const result = await resp.json();
    if (result.status === 'error') {
      showBanner('error', '<strong>Save failed:</strong><br>' + result.errors.map(e => '&bull; ' + esc(e)).join('<br>'));
      showToast('Save failed: ' + result.errors.length + ' error(s)', 'error');
    } else if (result.status === 'saved') {
      let html = '<strong>plex_debrid settings saved!</strong>';
      if (result.restarted) html += ' Service is restarting.';
      if (result.warnings && result.warnings.length) html += '<br><br><strong>Warnings:</strong><br>' + result.warnings.map(w => '&bull; ' + esc(w)).join('<br>');
      showBanner('success', html);
      showToast('plex_debrid settings saved!', 'success');
      pdValues = data;
      pdDirty = false;
      updateDirtyUI();
    } else {
      showBanner('warning', '<strong>Saved</strong> (restart failed \u2014 restart container manually)');
      showToast('Saved (restart failed)', 'warning');
    }
  } catch (e) { showBanner('error', 'Failed: ' + esc(e.message)); showToast('Save failed', 'error'); }
  finally { setButtonLoading('btn-pd-save', false, 'Save & Restart plex_debrid'); }
}

function pdReset() { clearFieldErrors(document.getElementById('tab-pd')); hideBanner(); renderPdCategories(pdValues); updateDirtyUI(); }

// -----------------------------------------------------------------------
// OAuth
// -----------------------------------------------------------------------
let oauthPollers = {};

async function oauthConnect(service, fieldId) {
  const btn = document.getElementById('oauth-btn-' + fieldId);
  const panelEl = document.getElementById('oauth-panel-' + fieldId);
  if (!btn || !panelEl) return;

  btn.disabled = true;
  btn.textContent = 'Connecting...';
  panelEl.innerHTML = '';

  try {
    const resp = await fetch('/api/settings/oauth/start', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({service})
    });
    const result = await resp.json();

    if (result.error) {
      panelEl.innerHTML = `<div class="oauth-panel" style="border-color:var(--red)"><div style="color:var(--red)">${esc(result.error)}</div></div>`;
      btn.disabled = false;
      btn.textContent = 'Retry';
      return;
    }

    panelEl.innerHTML = `<div class="oauth-panel">
      <div class="oauth-url">Visit <a href="${esc(result.verification_url)}" target="_blank" rel="noopener">${esc(result.verification_url)}</a> and enter this code:</div>
      <div class="oauth-code">${esc(result.user_code)}</div>
      <div class="oauth-status"><span class="spinner"></span>Waiting for authorization...</div>
      <button type="button" class="btn btn-ghost btn-oauth btn-cancel" onclick="oauthCancel('${esc(service)}','${fieldId}')">Cancel</button>
    </div>`;

    // Start polling
    const interval = (result.interval || 5) * 1000;
    oauthPollers[fieldId] = setInterval(async () => {
      try {
        const pr = await fetch('/api/settings/oauth/poll', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({service, device_code: result.device_code})
        });
        const poll = await pr.json();

        if (poll.error) {
          oauthCancel(service, fieldId);
          panelEl.innerHTML = `<div class="oauth-panel" style="border-color:var(--red)"><div style="color:var(--red)">${esc(poll.error)}</div></div>`;
          return;
        }

        if (poll.status === 'complete' && poll.token) {
          oauthCancel(service, fieldId);
          // Fill the input field
          const input = document.getElementById(fieldId);
          if (input) {
            input.value = poll.token;
            if (input.type === 'password') input.type = 'text';
          }
          panelEl.innerHTML = `<div class="oauth-panel" style="border-color:var(--green)"><div style="color:var(--green)">Connected! Token received.</div></div>`;
          setTimeout(() => { panelEl.innerHTML = ''; }, 5000);
        }
      } catch (e) { /* keep polling */ }
    }, interval);

  } catch (e) {
    panelEl.innerHTML = `<div class="oauth-panel" style="border-color:var(--red)"><div style="color:var(--red)">${esc(e.message)}</div></div>`;
    btn.disabled = false;
    btn.textContent = 'Retry';
  }
}

async function oauthConnectPair(service, containerId) {
  // For list_pairs (e.g., Trakt users): prompt for name, then OAuth for token
  const name = prompt('Enter a name for this user:');
  if (!name) return;

  const btn = document.getElementById('oauth-btn-' + containerId);
  const panelEl = document.getElementById('oauth-panel-' + containerId);
  if (!btn || !panelEl) return;

  btn.disabled = true;
  btn.textContent = 'Connecting...';

  try {
    const resp = await fetch('/api/settings/oauth/start', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({service})
    });
    const result = await resp.json();

    if (result.error) {
      panelEl.innerHTML = `<div class="oauth-panel" style="border-color:var(--red)"><div style="color:var(--red)">${esc(result.error)}</div></div>`;
      btn.disabled = false;
      btn.textContent = 'Connect via OAuth';
      return;
    }

    panelEl.innerHTML = `<div class="oauth-panel">
      <div class="oauth-url">Visit <a href="${esc(result.verification_url)}" target="_blank" rel="noopener">${esc(result.verification_url)}</a> and enter this code:</div>
      <div class="oauth-code">${esc(result.user_code)}</div>
      <div class="oauth-status"><span class="spinner"></span>Waiting for authorization...</div>
      <button type="button" class="btn btn-ghost btn-oauth btn-cancel" onclick="oauthCancel('${esc(service)}','${containerId}')">Cancel</button>
    </div>`;

    const interval = (result.interval || 5) * 1000;
    oauthPollers[containerId] = setInterval(async () => {
      try {
        const pr = await fetch('/api/settings/oauth/poll', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({service, device_code: result.device_code})
        });
        const poll = await pr.json();

        if (poll.error) {
          oauthCancel(service, containerId);
          panelEl.innerHTML = `<div class="oauth-panel" style="border-color:var(--red)"><div style="color:var(--red)">${esc(poll.error)}</div></div>`;
          return;
        }

        if (poll.status === 'complete' && poll.token) {
          oauthCancel(service, containerId);
          // Add a new pair row with [name, token]
          const container = document.getElementById(containerId);
          if (container) {
            const addBtn = container.querySelector('.btn-icon.add');
            const cols = JSON.parse(container.dataset.cols || '["",""]');
            const row = document.createElement('div');
            row.className = 'list-row';
            row.innerHTML = `<div class="pair-input"><input type="text" value="${esc(name)}" placeholder="${esc(cols[0])}"><input type="text" value="${esc(poll.token)}" placeholder="${esc(cols[1])}"></div><button type="button" class="btn btn-ghost btn-icon" onclick="removeListRow(this)" title="Remove">&times;</button>`;
            container.insertBefore(row, addBtn);
          }
          panelEl.innerHTML = `<div class="oauth-panel" style="border-color:var(--green)"><div style="color:var(--green)">Connected! User "${esc(name)}" added.</div></div>`;
          setTimeout(() => { panelEl.innerHTML = ''; }, 5000);
        }
      } catch (e) { /* keep polling */ }
    }, interval);

  } catch (e) {
    panelEl.innerHTML = `<div class="oauth-panel" style="border-color:var(--red)"><div style="color:var(--red)">${esc(e.message)}</div></div>`;
    btn.disabled = false;
    btn.textContent = 'Connect via OAuth';
  }
}

function oauthCancel(service, fieldId) {
  if (oauthPollers[fieldId]) {
    clearInterval(oauthPollers[fieldId]);
    delete oauthPollers[fieldId];
  }
  const btn = document.getElementById('oauth-btn-' + fieldId);
  if (btn) {
    btn.disabled = false;
    const isList = btn.textContent.includes('OAuth');
    btn.textContent = isList ? 'Connect via OAuth' : 'Connect';
  }
}

// -----------------------------------------------------------------------
// Import / Export / Reset
// -----------------------------------------------------------------------
async function envResetDefaults() {
  if (!confirm('Reset all pd_zurg settings to empty defaults? You will still need to click Save to apply.')) return;
  try {
    const resp = await fetch('/api/settings/reset/env', {method: 'POST'});
    const defaults = await resp.json();
    renderEnvCategories(defaults);
    updateDirtyUI();
    showBanner('info', 'Form reset to defaults. Click <strong>Save &amp; Apply</strong> to write changes.');
  } catch (e) { showBanner('error', 'Reset failed: ' + esc(e.message)); }
}

async function pdResetDefaults() {
  if (!confirm('Reset plex_debrid settings to defaults? You will still need to click Save to apply.')) return;
  try {
    const resp = await fetch('/api/settings/reset/plex-debrid', {method: 'POST'});
    const defaults = await resp.json();
    renderPdCategories(defaults);
    updateDirtyUI();
    showBanner('info', 'Form reset to defaults. Click <strong>Save &amp; Restart plex_debrid</strong> to write changes.');
  } catch (e) { showBanner('error', 'Reset failed: ' + esc(e.message)); }
}

function envImport(input) {
  const file = input.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = function(e) {
    const lines = e.target.result.split('\n');
    const imported = {};
    lines.forEach(line => {
      line = line.trim();
      if (!line || line.startsWith('#')) return;
      const eq = line.indexOf('=');
      if (eq < 1) return;
      let key = line.substring(0, eq).trim();
      let val = line.substring(eq + 1).trim();
      // Strip surrounding quotes
      if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
        val = val.slice(1, -1);
      }
      imported[key] = val;
    });
    if (!Object.keys(imported).length) {
      showBanner('error', 'No valid settings found in file');
      return;
    }
    // Merge imported values into current form
    const merged = Object.assign({}, envValues, imported);
    renderEnvCategories(merged);
    updateDirtyUI();
    showBanner('info', 'Imported ' + Object.keys(imported).length + ' settings into form. Review and click <strong>Save &amp; Apply</strong> to write changes.');
  };
  reader.readAsText(file);
  input.value = '';
}

function pdImport(input) {
  const file = input.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = function(e) {
    try {
      const imported = JSON.parse(e.target.result);
      if (typeof imported !== 'object' || Array.isArray(imported)) {
        showBanner('error', 'Invalid settings file: must be a JSON object');
        return;
      }
      renderPdCategories(imported);
      updateDirtyUI();
      showBanner('info', 'Settings imported into form. Review and click <strong>Save &amp; Restart plex_debrid</strong> to apply.');
    } catch (err) {
      showBanner('error', 'Failed to parse JSON: ' + esc(err.message));
    }
  };
  reader.readAsText(file);
  input.value = ''; // Allow re-importing same file
}

// -----------------------------------------------------------------------
// Settings search/filter
// -----------------------------------------------------------------------
function clearHighlights(container) {
  container.querySelectorAll('mark').forEach(m => {
    const parent = m.parentNode;
    parent.replaceChild(document.createTextNode(m.textContent), m);
    parent.normalize();
  });
}

function highlightText(el, query) {
  if (!el || !query) return;
  const text = el.textContent;
  const lc = text.toLowerCase();
  const frag = document.createDocumentFragment();
  let pos = 0, idx;
  while ((idx = lc.indexOf(query, pos)) !== -1) {
    if (idx > pos) frag.appendChild(document.createTextNode(text.substring(pos, idx)));
    const mark = document.createElement('mark');
    mark.textContent = text.substring(idx, idx + query.length);
    frag.appendChild(mark);
    pos = idx + query.length;
  }
  if (pos === 0) return;
  if (pos < text.length) frag.appendChild(document.createTextNode(text.substring(pos)));
  el.textContent = '';
  el.appendChild(frag);
}

function filterSettings(tab, query) {
  const container = document.getElementById(tab === 'env' ? 'env-categories' : 'pd-categories');
  const q = query.toLowerCase().trim();
  const countEl = document.getElementById('search-' + tab + '-count');
  let total = 0, shown = 0;

  // Clear previous highlights
  clearHighlights(container);

  container.querySelectorAll('.category').forEach(cat => {
    const body = cat.querySelector('.cat-body');
    const header = cat.querySelector('.cat-header');
    const catName = cat.querySelector('.cat-name');
    let catVisible = 0;

    // Check both regular and advanced fields
    cat.querySelectorAll('.field').forEach(field => {
      total++;
      const label = (field.querySelector('.field-label') || {}).textContent || '';
      if (!q || label.toLowerCase().includes(q)) {
        field.style.display = '';
        shown++;
        catVisible++;
        // Highlight matching text in label and help
        if (q) {
          const labelText = field.querySelector('.label-text');
          const helpText = field.querySelector('.field-help');
          highlightText(labelText, q);
          highlightText(helpText, q);
        }
      } else {
        field.style.display = 'none';
      }
    });

    if (q && catVisible > 0) {
      // Highlight category name if it matches and has visible fields
      if (catName) highlightText(catName, q);
      // Auto-expand categories with matches
      header.classList.add('open');
      body.classList.add('open');
      // Also expand advanced section if it has matches
      const adv = body.querySelector('.advanced-fields');
      if (adv) {
        const advVisible = adv.querySelectorAll('.field').length - adv.querySelectorAll('.field[style*="display"]').length;
        if (advVisible > 0) { adv.classList.add('open'); }
      }
    }

    cat.style.display = (q && catVisible === 0) ? 'none' : '';
  });

  countEl.textContent = q ? (shown + ' of ' + total + ' settings') : '';
}

// -----------------------------------------------------------------------
// Change detection & dirty state UI
// -----------------------------------------------------------------------
function activeTabName() {
  return document.getElementById('tab-env').classList.contains('active') ? 'env' : 'pd';
}

function getEnvChangedFields() {
  const changes = new Set();
  document.querySelectorAll('#tab-env [data-key]').forEach(el => {
    const key = el.dataset.key;
    const current = el.dataset.type === 'boolean' ? (el.checked ? 'true' : 'false') : el.value;
    const saved = envValues[key] !== undefined ? String(envValues[key]) : '';
    if (current !== saved) changes.add(key);
  });
  return changes;
}

function getPdChangedFields() {
  const changes = new Set();
  try {
    const current = collectPdData();
    const allKeys = new Set([...Object.keys(current), ...Object.keys(pdValues)]);
    allKeys.forEach(key => {
      const a = key in current ? current[key] : null;
      const b = key in pdValues ? pdValues[key] : null;
      if (JSON.stringify(a) !== JSON.stringify(b)) {
        changes.add(key);
      }
    });
  } catch(e) { /* form not ready yet */ }
  return changes;
}

let _dirtyRaf = 0;
function markDirty() {
  if (!_dirtyRaf) {
    _dirtyRaf = requestAnimationFrame(() => { _dirtyRaf = 0; updateDirtyUI(); });
  }
}

function updateDirtyUI() {
  const envChanges = getEnvChangedFields();
  const pdChanges = getPdChangedFields();
  envDirty = envChanges.size > 0;
  pdDirty = pdChanges.size > 0;
  isDirty = envDirty || pdDirty;

  // Tab dirty indicators
  const envTab = document.querySelector('.tab:nth-child(1)');
  const pdTab = document.querySelector('.tab:nth-child(2)');
  envTab.classList.toggle('dirty', envDirty);
  pdTab.classList.toggle('dirty', pdDirty);
  document.getElementById('btn-env-save').classList.toggle('dirty', envDirty);
  document.getElementById('btn-pd-save').classList.toggle('dirty', pdDirty);

  // Save bar
  const activeTab = activeTabName();
  const activeChanges = activeTab === 'env' ? envChanges : pdChanges;
  const bar = document.getElementById('save-bar');
  const countEl = document.getElementById('save-bar-count');
  const saveBtn = document.getElementById('save-bar-btn');
  if (activeChanges.size > 0) {
    countEl.textContent = activeChanges.size + ' unsaved change' + (activeChanges.size !== 1 ? 's' : '');
    saveBtn.textContent = activeTab === 'env' ? 'Save & Apply' : 'Save & Restart';
    bar.classList.add('visible');
    document.body.classList.add('has-save-bar');
  } else {
    bar.classList.remove('visible');
    document.body.classList.remove('has-save-bar');
  }

  // Per-field changed class + category indicators
  updateFieldAndCategoryIndicators('env', envChanges, ENV_SCHEMA);
  updateFieldAndCategoryIndicators('pd', pdChanges, PD_SCHEMA);
}

function updateFieldAndCategoryIndicators(tab, changes, schema) {
  const containerId = tab === 'env' ? 'env-categories' : 'pd-categories';
  const container = document.getElementById(containerId);
  if (!container) return;

  // Per-field changed class
  container.querySelectorAll('.field[data-field-key]').forEach(field => {
    field.classList.toggle('changed', changes.has(field.dataset.fieldKey));
  });

  // Category indicators
  container.querySelectorAll('.category[data-cat-idx]').forEach(catEl => {
    const idx = parseInt(catEl.dataset.catIdx);
    const cat = schema.categories[idx];
    if (!cat) return;
    let count = 0;
    cat.fields.forEach(f => { if (changes.has(f.key)) count++; });
    const header = catEl.querySelector('.cat-header');
    const countSpan = catEl.querySelector('.cat-dirty-count');
    header.classList.toggle('has-changes', count > 0);
    if (countSpan) countSpan.textContent = count > 0 ? count + ' changed' : '';
  });
}

// Save bar actions
function saveBarSave() {
  const btn = document.getElementById('save-bar-btn');
  if (btn) btn.disabled = true;
  const done = () => { if (btn) btn.disabled = false; };
  (activeTabName() === 'env' ? envSave() : pdSave()).finally(done);
}
function saveBarDiscard() {
  if (activeTabName() === 'env') envReset(); else pdReset();
}

// Per-field reset
function resetField(tab, key) {
  if (tab === 'env') {
    const el = document.getElementById('env-' + key);
    if (!el) return;
    const saved = envValues[key] !== undefined ? String(envValues[key]) : '';
    if (el.dataset.type === 'boolean') {
      el.checked = saved.toLowerCase() === 'true';
    } else {
      el.value = saved;
    }
  } else {
    // Fast path for simple pd field types — set value directly
    const saved = pdValues[key];
    let handled = false;
    const simpleEl = document.querySelector('#tab-pd [data-pdkey="' + CSS.escape(key) + '"]');
    if (simpleEl) {
      const t = simpleEl.dataset.pdtype;
      if (t === 'string' || t === 'secret' || t === 'select') {
        simpleEl.value = saved !== undefined ? String(saved) : '';
        handled = true;
      } else if (t === 'boolean_str') {
        simpleEl.checked = String(saved).toLowerCase() === 'true';
        handled = true;
      }
    }
    if (!handled) {
      // Complex types (lists, versions, json): re-render with this key reverted
      const current = collectPdData();
      current[key] = saved !== undefined ? JSON.parse(JSON.stringify(saved)) : '';
      renderPdCategories(current);
      // Reapply active search filter if present
      const searchInput = document.getElementById('search-pd');
      if (searchInput && searchInput.value.trim()) filterSettings('pd', searchInput.value);
    }
  }
  updateDirtyUI();
}

// -----------------------------------------------------------------------
// Init
// -----------------------------------------------------------------------
async function init() {
  // Load env values
  try {
    const resp = await fetch('/api/settings/env');
    if (resp.ok) { envValues = await resp.json(); }
    else { showBanner('error', 'Failed to load pd_zurg settings (HTTP ' + resp.status + '). Check authentication.'); }
  } catch (e) { showBanner('error', 'Failed to load pd_zurg settings: ' + esc(e.message)); }
  renderEnvCategories(envValues);

  // Load plex_debrid values
  try {
    const resp = await fetch('/api/settings/plex-debrid');
    if (resp.ok) { pdValues = await resp.json(); }
    else if (resp.status !== 404) { showBanner('error', 'Failed to load plex_debrid settings (HTTP ' + resp.status + ')'); }
  } catch (e) { showBanner('error', 'Failed to load plex_debrid settings: ' + esc(e.message)); }
  renderPdCategories(pdValues);
}

init();

// Track dirty state per tab
document.addEventListener('input', markDirty);
document.addEventListener('change', markDirty);
window.addEventListener('beforeunload', (e) => {
  if (isDirty) { e.preventDefault(); e.returnValue = ''; }
});

// Keyboard shortcut: Ctrl+S / Cmd+S to save active tab
document.addEventListener('keydown', (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === 's') {
    e.preventDefault();
    const envActive = document.getElementById('tab-env').classList.contains('active');
    if (envActive) envSave();
    else pdSave();
  }
});
__WANTED_BADGE_JS__
</script>
</body>
</html>'''
