<?php
declare(strict_types=1);

require __DIR__ . '/../src/Bootstrap/autoload.php';
\Welafix\Bootstrap\Env::load(__DIR__ . '/../.env');

use Welafix\Database\ConnectionFactory;

$limit = 10;
if (isset($argv[1]) && is_numeric($argv[1])) {
    $limit = max(1, (int)$argv[1]);
}

$base = (string)env('FILEDB_PATH', __DIR__ . '/../storage/data');
$base = rtrim($base, "/\\");
$artikelBase = $base . '/Artikel';
$wgBase = $base . '/Warengruppen';
$artikelStd = $artikelBase . '/Standard';
$wgStd = $wgBase . '/Standard';

echo "FILEDB_PATH: {$base}\n";
echo "Artikel Base: {$artikelBase} (" . (is_dir($artikelBase) ? 'ok' : 'missing') . ")\n";
echo "Artikel Standard: {$artikelStd} (" . (is_dir($artikelStd) ? 'ok' : 'missing') . ")\n";
echo "Warengruppen Base: {$wgBase} (" . (is_dir($wgBase) ? 'ok' : 'missing') . ")\n";
echo "Warengruppen Standard: {$wgStd} (" . (is_dir($wgStd) ? 'ok' : 'missing') . ")\n\n";

$factory = new ConnectionFactory();
$pdo = $factory->sqlite();

echo "== Warengruppen (Bezeichnung) ==\n";
$wgStmt = $pdo->query('SELECT afs_wg_id, name FROM warengruppe LIMIT ' . $limit);
$wgRows = $wgStmt ? $wgStmt->fetchAll(PDO::FETCH_ASSOC) : [];
foreach ($wgRows as $row) {
    $name = trim((string)($row['name'] ?? ''));
    $id = (string)($row['afs_wg_id'] ?? '');
    $dir = $wgBase . '/' . $name;
    $exists = is_dir($dir);
    $fallback = (!$exists && is_dir($wgStd)) ? 'uses Standard' : 'no Standard';
    echo "- {$id} | {$name} => " . ($exists ? 'OK' : 'MISSING') . ($exists ? '' : " ({$fallback})") . "\n";
}

echo "\n== Artikel (Artikelnummer) ==\n";
$artStmt = $pdo->query('SELECT artikelnummer FROM artikel LIMIT ' . $limit);
$artRows = $artStmt ? $artStmt->fetchAll(PDO::FETCH_ASSOC) : [];
foreach ($artRows as $row) {
    $nr = trim((string)($row['artikelnummer'] ?? ''));
    $dir = $artikelBase . '/' . $nr;
    $exists = is_dir($dir);
    $fallback = (!$exists && is_dir($artikelStd)) ? 'uses Standard' : 'no Standard';
    echo "- {$nr} => " . ($exists ? 'OK' : 'MISSING') . ($exists ? '' : " ({$fallback})") . "\n";
}

