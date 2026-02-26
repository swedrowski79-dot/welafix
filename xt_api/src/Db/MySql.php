<?php
declare(strict_types=1);

namespace XtApi\Db;

use PDO;
use RuntimeException;
use function XtApi\env;

final class MySql
{
    private static ?PDO $pdo = null;

    public static function connect(): PDO
    {
        if (self::$pdo) {
            return self::$pdo;
        }

        $host = (string)env('XT_DB_HOST', '');
        $port = (string)env('XT_DB_PORT', '3306');
        $db   = (string)env('XT_DB_NAME', '');
        $user = (string)env('XT_DB_USER', '');
        $pass = (string)env('XT_DB_PASS', '');

        if ($host === '' || $db === '') {
            throw new RuntimeException('XT_DB_HOST/XT_DB_NAME fehlen.');
        }

        $dsn = "mysql:host={$host};port={$port};dbname={$db};charset=utf8mb4";
        $pdo = new PDO($dsn, $user, $pass, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        self::$pdo = $pdo;
        return $pdo;
    }
}
