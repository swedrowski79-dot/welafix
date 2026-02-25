<?php
declare(strict_types=1);
$path = isset($_SERVER['REQUEST_URI']) ? parse_url((string)$_SERVER['REQUEST_URI'], PHP_URL_PATH) : null;
$path = is_string($path) ? $path : '';
?>
<div class="card">
  <h2>Seite nicht gefunden</h2>
  <p class="muted">Die angeforderte Seite existiert nicht oder wurde verschoben.</p>
  <?php if ($path !== ''): ?>
    <p class="muted">Pfad: <code><?= htmlspecialchars($path, ENT_QUOTES, 'UTF-8') ?></code></p>
  <?php endif; ?>
  <div class="grid" style="margin-top: 12px;">
    <div class="card">
      <strong>Dashboard</strong><br>
      <span class="muted">Systemstatus, Syncs und Logs</span><br><br>
      <a class="btn" href="/dashboard">Zum Dashboard</a>
    </div>
    <div class="card">
      <strong>SQLite Browser</strong><br>
      <span class="muted">Daten prüfen &amp; suchen</span><br><br>
      <a class="btn" href="/dashboard/sqlite">SQLite öffnen</a>
    </div>
  </div>
</div>
