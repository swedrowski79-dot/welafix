<?php
declare(strict_types=1);
/** @var array $data */
?>
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Welafix Schnittstelle</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 24px; }
    .card { border: 1px solid #ddd; border-radius: 12px; padding: 16px; margin-bottom: 16px; }
    .grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }
    .badge { display:inline-block; padding: 4px 10px; border-radius: 999px; border: 1px solid #ccc; }
    .btn { display: inline-block; padding: 8px 12px; border: 1px solid #ccc; border-radius: 8px; text-decoration: none; background: #f6f6f6; }
    .btn-row { display: flex; gap: 8px; flex-wrap: wrap; }
    a { color: inherit; }
    code { background: #f6f6f6; padding: 2px 6px; border-radius: 6px; }
  </style>
</head>
<body>
  <h1>Welafix Schnittstelle</h1>
  <p class="badge">Status: läuft ✅</p>

  <?php require $view; ?>

  <div class="card">
    <h3>Links</h3>
    <ul>
      <li><a href="/test_mssql.php">MSSQL Verbindungstest</a></li>
      <li><a href="/health">Health JSON</a></li>
    </ul>
  </div>
</body>
</html>
