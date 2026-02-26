<?php
declare(strict_types=1);

require __DIR__ . '/../src/Bootstrap/autoload.php';

\Welafix\Bootstrap\Env::load(__DIR__ . '/../.env');

$path = (string)env('SQLITE_PATH', '');
header('Content-Type: application/json; charset=utf-8');

if ($path === '' || !is_file($path)) {
    echo json_encode([
        'ok' => false,
        'error' => 'SQLITE_PATH not found',
        'path' => $path,
    ], JSON_PRETTY_PRINT);
    return;
}

$pdo = new PDO('sqlite:' . $path);
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

$getCols = function (string $table) use ($pdo): array {
    $stmt = $pdo->query('PRAGMA table_info(' . $table . ')');
    $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
    return array_values(array_map(static fn($r) => $r['name'], $rows));
};

$documentsCols = $getCols('documents');
$artikelCols = $getCols('artikel');

$docCounts = [];
foreach (['Artikel','Artikel_ID','Dateiname','Titel'] as $col) {
    if (!in_array($col, $documentsCols, true)) {
        $docCounts[$col] = 'missing';
        continue;
    }
    $stmt = $pdo->query("SELECT COUNT(*) FROM documents WHERE {$col} IS NOT NULL AND TRIM({$col}) != ''");
    $docCounts[$col] = (int)$stmt->fetchColumn();
}

echo json_encode([
    'ok' => true,
    'sqlite_path' => $path,
    'documents_cols' => $documentsCols,
    'artikel_cols' => $artikelCols,
    'documents_nonempty' => $docCounts,
], JSON_PRETTY_PRINT);
