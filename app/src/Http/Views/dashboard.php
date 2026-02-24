<?php
declare(strict_types=1);
/** @var array $data */
$counts = $data['counts'];
?>
<div class="card">
  <h2>Welafix Dashboard</h2>
  <div class="btn-row">
    <button class="btn" type="button" data-endpoint="/api/status">Status</button>
    <button class="btn" type="button" data-endpoint="/api/test-mssql">MSSQL Test</button>
    <button class="btn" type="button" data-endpoint="/api/test-sqlite">SQLite Test</button>
    <button class="btn" type="button" data-endpoint="/sync/artikel" data-sync="artikel">Artikel Sync</button>
    <button class="btn" type="button" id="artikel-cancel" disabled>Abbrechen</button>
    <label for="artikel-batch">Batch</label>
    <select id="artikel-batch">
      <option value="200">200</option>
      <option value="500" selected>500</option>
      <option value="1000">1000</option>
    </select>
  </div>
  <pre id="out">Klicke einen Button, um eine API-Antwort zu sehen.</pre>
</div>

<div class="card">
  <h2>Übersicht</h2>
  <div class="grid">
    <div class="card">
      <strong>Artikel changed</strong><br>
      <span class="badge"><?= htmlspecialchars((string)$counts['artikel_changed']) ?></span>
    </div>
    <div class="card">
      <strong>Warengruppen changed</strong><br>
      <span class="badge"><?= htmlspecialchars((string)$counts['warengruppe_changed']) ?></span>
    </div>
    <div class="card">
      <strong>Media changed</strong><br>
      <span class="badge"><?= htmlspecialchars((string)$counts['media_changed']) ?></span>
    </div>
  </div>
</div>

<div class="card">
  <h2>Sync</h2>
  <div class="btn-row">
    <a class="btn" href="/sync/warengruppe">Warengruppen sync</a>
    <a class="btn" href="/sync/artikel">Artikel sync</a>
  </div>
  <p>
    Aktuell changed:
    Artikel <span class="badge"><?= htmlspecialchars((string)$counts['artikel_changed']) ?></span>
    Warengruppen <span class="badge"><?= htmlspecialchars((string)$counts['warengruppe_changed']) ?></span>
  </p>
</div>

<div class="card">
  <h2>Konfiguration</h2>
  <p>Mappings liegen unter <code>app/src/Config/mappings/</code>.</p>
</div>

<script>
  (function () {
    const output = document.getElementById('out');
    const buttons = document.querySelectorAll('button[data-endpoint]');
    const setOutput = (value) => {
      output.textContent = value;
    };

    const cancelBtn = document.getElementById('artikel-cancel');
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
