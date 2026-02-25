<?php
declare(strict_types=1);

require __DIR__ . '/../src/Bootstrap/autoload.php';

\Welafix\Bootstrap\Env::load(__DIR__ . '/../.env');

header('Content-Type: text/plain; charset=utf-8');

try {
    $pdo = \Welafix\Database\Db::guardMssql(\Welafix\Database\Db::mssql(), __METHOD__);
    $stmt = $pdo->query('SELECT TOP 5 name FROM sys.tables ORDER BY name');
    $tables = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
    echo "OK\n";
    echo json_encode($tables, JSON_PRETTY_PRINT);
} catch (Throwable $e) {
    http_response_code(500);
    echo "FAIL\n" . $e->getMessage() . "\n";
}
