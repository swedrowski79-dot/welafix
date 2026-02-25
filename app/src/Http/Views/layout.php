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
    :root {
      --bg: #f6f7fb;
      --card: #ffffff;
      --text: #1a1a1a;
      --muted: #667085;
      --border: #e4e7ec;
      --accent: #2563eb;
      --ok: #16a34a;
      --fail: #dc2626;
    }
    * { box-sizing: border-box; }
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; background: var(--bg); color: var(--text); }
    .container { max-width: 1100px; margin: 0 auto; padding: 24px; }
    header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
    header h1 { margin: 0; font-size: 22px; }
    .badge { display:inline-block; padding: 4px 10px; border-radius: 999px; border: 1px solid var(--border); background: #fff; color: var(--muted); font-size: 12px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px; margin-bottom: 16px; box-shadow: 0 1px 1px rgba(16,24,40,0.04); }
    .tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }
    .tile { border: 1px solid var(--border); border-radius: 12px; padding: 14px; background: #fafbff; }
    .tile-label { font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); margin-bottom: 8px; }
    .status-row { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; }
    .status-dot { width: 10px; height: 10px; border-radius: 50%; background: #cbd5e1; display: inline-block; }
    .status-dot.ok { background: var(--ok); }
    .status-dot.fail { background: var(--fail); }
    .status-text { font-size: 14px; }
    .btn { display: inline-block; padding: 8px 12px; border: 1px solid var(--border); border-radius: 8px; text-decoration: none; background: #fff; color: var(--text); cursor: pointer; transition: border-color 0.15s ease, box-shadow 0.15s ease; }
    .btn:hover { border-color: #cbd5e1; box-shadow: 0 1px 3px rgba(16,24,40,0.08); }
    .btn:disabled { opacity: 0.6; cursor: not-allowed; box-shadow: none; }
    .btn-row { display: flex; gap: 8px; flex-wrap: wrap; }
    .inline-controls { display: flex; align-items: center; gap: 8px; margin-top: 10px; font-size: 14px; color: var(--muted); }
    select, input { padding: 6px 8px; border-radius: 8px; border: 1px solid var(--border); }
    a { color: inherit; }
    code { background: #eef2ff; padding: 2px 6px; border-radius: 6px; }
    .log { background: #0b1020; color: #d1d5db; padding: 14px; border-radius: 10px; overflow: auto; min-height: 140px; }
    .error { color: var(--fail); }
    .table-wrap { overflow: auto; border: 1px solid var(--border); border-radius: 10px; }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td { padding: 8px 10px; border-bottom: 1px solid var(--border); text-align: left; white-space: nowrap; }
    th { background: #f8fafc; position: sticky; top: 0; z-index: 1; }
    .pagination { display: flex; align-items: center; gap: 8px; margin-top: 10px; }
    .muted { color: var(--muted); }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>Welafix Schnittstelle</h1>
      <span class="badge">Status: läuft</span>
    </header>

    <?php require $view; ?>

    <div class="card">
      <h3>Links</h3>
      <div class="btn-row">
        <a class="btn" href="/dashboard">Dashboard</a>
        <a class="btn" href="/dashboard/sqlite">SQLite Browser</a>
        <a class="btn" href="/test_mssql.php">MSSQL Verbindungstest</a>
        <a class="btn" href="/health">Health JSON</a>
      </div>
    </div>
  </div>
</body>
</html>
