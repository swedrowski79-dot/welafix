<?php
declare(strict_types=1);
/** @var array $data */
?>
<div class="card">
  <h2>Dashboard</h2>
  <div class="tiles">
    <div class="tile">
      <div class="tile-label">Systemstatus</div>
      <div class="status-row">
        <span class="status-dot" id="mssql-dot"></span>
        <span class="status-text" id="mssql-status">MSSQL: prüfe…</span>
      </div>
      <div class="status-row">
        <span class="status-dot" id="sqlite-dot"></span>
        <span class="status-text" id="sqlite-status">SQLite: prüfe…</span>
      </div>
      <div class="status-row">
        <span class="status-dot" id="xtapi-dot"></span>
        <span class="status-text" id="xtapi-status">XT-API: prüfe…</span>
      </div>
      <div class="status-row">
        <button class="status-toggle" type="button" id="debug-toggle">
          <span class="status-dot" id="debug-dot"></span>
          <span id="debug-label">Debug: prüfe…</span>
        </button>
      </div>
    </div>
    <div class="tile">
      <div class="tile-label">Sync</div>
      <div class="muted" style="margin-bottom: 8px;">AFS Schritte</div>
      <div class="btn-row">
        <button class="btn" type="button" data-endpoint="/sync/warengruppe">1. AFS_Warengruppen</button>
        <button class="btn" type="button" data-endpoint="/sync/artikel" data-sync="artikel">2. AFS_Artikel</button>
        <button class="btn" type="button" data-endpoint="/api/meta/fill">3. fill_Meta</button>
        <button class="btn" type="button" data-endpoint="/sync/dokument">4. AFS_Dokumente</button>
        <button class="btn" type="button" data-endpoint="/sync/media">5. AFS_Media</button>
        <button class="btn" type="button" id="artikel-cancel" disabled>Abbrechen</button>
      </div>
      <div class="inline-controls">
        <label for="artikel-batch">Batch</label>
        <select id="artikel-batch">
          <option value="200">200</option>
          <option value="500" selected>500</option>
          <option value="1000">1000</option>
          <option value="2000">2000</option>
          <option value="5000">5000</option>
          <option value="10000">10000</option>
        </select>
      </div>
      <div class="inline-controls">
        <label for="xt-batch">XT Batchgröße</label>
        <input id="xt-batch" type="number" min="50" max="5000" step="50" value="500" />
        <button class="btn" type="button" id="xt-batch-save">Speichern</button>
      </div>
      <div class="muted" style="margin: 14px 0 8px;">XT Schritte</div>
      <div class="btn-row">
        <button class="btn" type="button" data-endpoint="/sync/xt-mapping">XT Mapping Sync</button>
      </div>
    </div>
    <div class="tile">
      <div class="tile-label">Tools</div>
      <a class="btn" href="/dashboard/sqlite">SQLite Browser</a>
      <button class="btn" type="button" data-endpoint="/api/filedb/check">FileDB Check</button>
      <button class="btn" type="button" data-endpoint="/api/filedb/apply">FileDB to SQLite</button>
      <button class="btn" type="button" data-endpoint="/migrate.php">Migration</button>
      <button class="btn" type="button" data-endpoint="/sync/xt-full?mapping=xt_commerce_full_tables">XT Full Tables Sync</button>
      <button class="btn" type="button" id="xt-import-open">XT-Commerce Tabellen Import</button>
    </div>
  </div>
</div>

<div class="card">
  <h2>Output</h2>
  <pre id="out" class="log">Klicke einen Button, um eine API-Antwort zu sehen.</pre>
</div>

<div id="xt-modal" class="xt-modal hidden">
  <div class="xt-modal-backdrop"></div>
  <div class="xt-modal-panel">
    <div class="xt-modal-header">
      <strong>XT-Commerce Tabellen Import</strong>
      <button class="btn" type="button" id="xt-import-close">Schließen</button>
    </div>
    <div class="xt-modal-body">
      <div class="inline-controls">
        <label for="xt-table">XT Tabelle</label>
        <select id="xt-table"></select>
      </div>
      <div class="inline-controls">
        <label for="xt-page-size">Batch Size</label>
        <select id="xt-page-size">
          <option value="500">500</option>
          <option value="1000">1000</option>
          <option value="2000" selected>2000</option>
          <option value="5000">5000</option>
          <option value="10000">10000</option>
        </select>
        <button class="btn" type="button" id="xt-import-start">Import starten</button>
      </div>
      <pre id="xt-import-status" class="log">Bereit.</pre>
    </div>
  </div>
</div>

<script>
  (function () {
    const output = document.getElementById('out');
    const buttons = document.querySelectorAll('button[data-endpoint]');
    const cancelBtn = document.getElementById('artikel-cancel');
    const setOutput = (value) => { output.textContent = value; };

    const setStatus = (dotId, textId, ok, label) => {
      const dot = document.getElementById(dotId);
      const text = document.getElementById(textId);
      if (!dot || !text) return;
      dot.classList.remove('ok', 'fail');
      dot.classList.add(ok ? 'ok' : 'fail');
      text.textContent = label + ': ' + (ok ? 'ok' : 'fehler');
    };

    const setDebugStatus = (enabled) => {
      const dot = document.getElementById('debug-dot');
      const btn = document.getElementById('debug-toggle');
      const label = document.getElementById('debug-label');
      if (!dot || !btn || !label) return;
      dot.classList.remove('ok', 'fail');
      dot.classList.add(enabled ? 'ok' : 'fail');
      label.textContent = 'Debug: ' + (enabled ? 'ein' : 'aus');
      btn.dataset.enabled = enabled ? '1' : '0';
    };

    const checkStatus = async (endpoint, dotId, textId, label) => {
      try {
        const response = await fetch(endpoint, { headers: { 'Accept': 'application/json' } });
        const json = await response.json();
        setStatus(dotId, textId, !!json.ok, label);
      } catch (e) {
        setStatus(dotId, textId, false, label);
      }
    };

    checkStatus('/api/test-mssql', 'mssql-dot', 'mssql-status', 'MSSQL');
    checkStatus('/api/test-sqlite', 'sqlite-dot', 'sqlite-status', 'SQLite');
    const checkXtStatus = async () => {
      try {
        const response = await fetch('/api/xt/check', { headers: { 'Accept': 'application/json' } });
        const json = await response.json();
        const dot = document.getElementById('xtapi-dot');
        const text = document.getElementById('xtapi-status');
        if (!dot || !text) return;
        dot.classList.remove('ok', 'fail');
        if (json.ok) {
          dot.classList.add('ok');
          text.textContent = 'XT-API: erreichbar';
          return;
        }
        dot.classList.add('fail');
        const status = json.status || 'fehler';
        let label = 'XT-API: ';
        if (status === 'auth_fail') label += 'auth fail';
        else if (status === 'db_fail') label += 'db fail';
        else if (status === 'unreachable') label += 'nicht erreichbar';
        else label += 'fehler';
        text.textContent = label;
      } catch (e) {
        setStatus('xtapi-dot', 'xtapi-status', false, 'XT-API');
      }
    };
    checkXtStatus();

    let cancelRequested = false;

    const runArtikelSync = async (button) => {
      const endpoint = button.getAttribute('data-endpoint');
      const batch = document.getElementById('artikel-batch').value;
      let after = '';
      let done = false;

      cancelRequested = false;
      cancelBtn.disabled = false;

      button.disabled = true;
      const originalText = button.textContent;
      button.textContent = 'Synce…';

      while (!done && !cancelRequested) {
        const url = endpoint + '?batch=' + encodeURIComponent(batch) + '&after=' + encodeURIComponent(after);
        setOutput('Lade ' + url + ' ...');
        try {
          const response = await fetch(url, { headers: { 'Accept': 'application/json' } });
          const text = await response.text();
          let json = null;
          try {
            json = JSON.parse(text);
          } catch (e) {
            setOutput(text);
            break;
          }

          setOutput(JSON.stringify(json, null, 2));

          done = !!json.done;
          after = json.last_key || after;
          if (!done && !after) {
            break;
          }
        } catch (e) {
          setOutput('Fehler: ' + e.message);
          break;
        }
      }

      if (cancelRequested) {
        setOutput('Sync abgebrochen.');
      }

      button.disabled = false;
      button.textContent = originalText;
      cancelBtn.disabled = true;
      cancelRequested = false;
    };

    const loadXtBatch = async () => {
      try {
        const res = await fetch('/api/settings?key=xt_import_batch_size');
        const json = await res.json();
        if (json && json.ok && json.value) {
          document.getElementById('xt-batch').value = json.value;
        }
      } catch (e) {}
    };

    const loadXtSelftestToggle = async () => {
      try {
        const res = await fetch('/api/settings?key=xt_debug_enabled');
        const json = await res.json();
        if (json && json.ok) {
          const enabled = !(json.value !== null && json.value !== '' && json.value !== '1');
          setDebugStatus(enabled);
        }
      } catch (e) {}
    };

    const saveXtBatch = async () => {
      const input = document.getElementById('xt-batch');
      let value = parseInt(input.value, 10);
      if (isNaN(value)) value = 500;
      if (value < 50) value = 50;
      if (value > 5000) value = 5000;
      input.value = value;
      const body = new URLSearchParams();
      body.set('key', 'xt_import_batch_size');
      body.set('value', String(value));
      try {
        const res = await fetch('/api/settings', { method: 'POST', body });
        const json = await res.json();
        setOutput(JSON.stringify(json, null, 2));
      } catch (e) {
        setOutput('Fehler: ' + e.message);
      }
    };

    document.getElementById('xt-batch-save').addEventListener('click', saveXtBatch);
    loadXtBatch();
    loadXtSelftestToggle();

    document.getElementById('debug-toggle').addEventListener('click', async () => {
      const btn = document.getElementById('debug-toggle');
      const current = btn.dataset.enabled === '1';
      const next = current ? '0' : '1';
      const body = new URLSearchParams();
      body.set('key', 'xt_debug_enabled');
      body.set('value', next);
      try {
        const res = await fetch('/api/settings', { method: 'POST', body });
        const json = await res.json();
        if (json && json.ok) {
          setDebugStatus(next === '1');
        } else {
          setOutput(JSON.stringify(json, null, 2));
        }
      } catch (e) {
        setOutput('Fehler: ' + e.message);
      }
    });

    const modal = document.getElementById('xt-modal');
    const modalStatus = document.getElementById('xt-import-status');
    const xtTableSelect = document.getElementById('xt-table');

    const openModal = async () => {
      modal.classList.remove('hidden');
      modalStatus.textContent = 'Lade Tabellenliste...';
      try {
        const res = await fetch('/api/xt/schema/tables');
        const json = await res.json();
        xtTableSelect.innerHTML = '';
        if (json.ok && Array.isArray(json.tables)) {
          json.tables.forEach((t) => {
            const opt = document.createElement('option');
            opt.value = t;
            opt.textContent = t;
            xtTableSelect.appendChild(opt);
          });
          modalStatus.textContent = 'Bereit.';
        } else {
          modalStatus.textContent = 'Fehler: ' + (json.error || 'keine Tabellen');
        }
      } catch (e) {
        modalStatus.textContent = 'Fehler: ' + e.message;
      }
    };

    const closeModal = () => {
      modal.classList.add('hidden');
    };

    const startImport = async () => {
      const table = xtTableSelect.value;
      const pageSize = document.getElementById('xt-page-size').value;
      if (!table) {
        modalStatus.textContent = 'Bitte Tabelle auswählen.';
        return;
      }
      modalStatus.textContent = 'Import läuft...';
      const body = new URLSearchParams();
      body.set('table', table);
      body.set('page_size', pageSize);
      try {
        const res = await fetch('/api/xt/import-table', { method: 'POST', body });
        const json = await res.json();
        modalStatus.textContent = JSON.stringify(json, null, 2);
      } catch (e) {
        modalStatus.textContent = 'Fehler: ' + e.message;
      }
    };

    document.getElementById('xt-import-open').addEventListener('click', openModal);
    document.getElementById('xt-import-close').addEventListener('click', closeModal);
    document.getElementById('xt-import-start').addEventListener('click', startImport);

    cancelBtn.addEventListener('click', () => {
      cancelRequested = true;
    });

    buttons.forEach((button) => {
      button.addEventListener('click', async () => {
        const endpoint = button.getAttribute('data-endpoint');
        const isArtikelSync = button.getAttribute('data-sync') === 'artikel';
        if (isArtikelSync) {
          await runArtikelSync(button);
          return;
        }

        const url = endpoint;
        button.disabled = true;
        const originalText = button.textContent;
        button.textContent = 'Synce…';
        setOutput('Lade ' + url + ' ...');
        try {
          const response = await fetch(url, { headers: { 'Accept': 'application/json' } });
          const text = await response.text();
          try {
            const json = JSON.parse(text);
            setOutput(JSON.stringify(json, null, 2));
          } catch (e) {
            setOutput(text);
          }
        } catch (e) {
          setOutput('Fehler: ' + e.message);
        } finally {
          button.disabled = false;
          button.textContent = originalText;
        }
      });
    });
  })();
</script>
