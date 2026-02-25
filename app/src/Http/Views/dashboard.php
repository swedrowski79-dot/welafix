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
    </div>
    <div class="tile">
      <div class="tile-label">Sync</div>
      <div class="btn-row">
        <button class="btn" type="button" data-endpoint="/sync/warengruppe">Warengruppe</button>
        <button class="btn" type="button" data-endpoint="/sync/artikel" data-sync="artikel">Artikel</button>
        <button class="btn" type="button" data-endpoint="/sync/media/images">Media Import (Bilder)</button>
        <button class="btn" type="button" data-endpoint="/sync/media/documents">Media Import (Dokumente)</button>
        <button class="btn" type="button" id="artikel-cancel" disabled>Abbrechen</button>
      </div>
      <div class="inline-controls">
        <label for="artikel-batch">Batch</label>
        <select id="artikel-batch">
          <option value="200">200</option>
          <option value="500" selected>500</option>
          <option value="1000">1000</option>
        </select>
      </div>
    </div>
    <div class="tile">
      <div class="tile-label">Tools</div>
      <a class="btn" href="/dashboard/sqlite">SQLite Browser</a>
    </div>
  </div>
</div>

<div class="card">
  <h2>Output</h2>
  <pre id="out" class="log">Klicke einen Button, um eine API-Antwort zu sehen.</pre>
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
