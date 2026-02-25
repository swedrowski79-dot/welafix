<?php
declare(strict_types=1);

require __DIR__ . '/../app/src/Bootstrap/autoload.php';

use Welafix\Database\ConnectionFactory;
use Welafix\Database\Db;

putenv('SQLITE_PATH=' . sys_get_temp_dir() . '/guard_test.db');
Db::setFactory(new ConnectionFactory());

$pdo = Db::guardSqlite(Db::sqlite(), __METHOD__);

echo "Test 1 (SQLite + TOP) => expect exception\n";
try {
    $pdo->query('SELECT TOP 1 * FROM artikel');
    echo "FAIL: no exception\n";
} catch (Throwable $e) {
    echo "OK: " . $e->getMessage() . "\n";
}

echo "Test 2 (SQLite + LIMIT) => expect ok\n";
try {
    $pdo->query('SELECT 1');
    echo "OK\n";
} catch (Throwable $e) {
    echo "FAIL: " . $e->getMessage() . "\n";
}

echo "Test 3 (MSSQL + TOP) => expect ok or skip\n";
try {
    if (!extension_loaded('sqlsrv') || getenv('MSSQL_HOST') === false) {
        echo "SKIP: sqlsrv not available or MSSQL_HOST missing\n";
    } else {
        $mssql = Db::guardMssql(Db::mssql(), __METHOD__);
        $stmt = $mssql->query('SELECT TOP 1 1 AS ok');
        $row = $stmt ? $stmt->fetch(PDO::FETCH_ASSOC) : null;
        echo "OK: " . json_encode($row) . "\n";
    }
} catch (Throwable $e) {
    echo "FAIL: " . $e->getMessage() . "\n";
}
