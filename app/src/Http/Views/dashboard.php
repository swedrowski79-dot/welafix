<?php
declare(strict_types=1);
/** @var array $data */
$counts = $data['counts'];
?>
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
