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
    <button class="btn" type="button" data-endpoint="/sync/artikel">Artikel Sync</button>
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

    buttons.forEach((button) => {
      button.addEventListener('click', async () => {
        const endpoint = button.getAttribute('data-endpoint');
        setOutput('Lade ' + endpoint + ' ...');
        try {
          const response = await fetch(endpoint, { headers: { 'Accept': 'application/json' } });
          const text = await response.text();
          try {
            const json = JSON.parse(text);
            setOutput(JSON.stringify(json, null, 2));
          } catch (e) {
            setOutput(text);
          }
        } catch (e) {
          setOutput('Fehler: ' + e.message);
        }
      });
    });
  })();
</script>
