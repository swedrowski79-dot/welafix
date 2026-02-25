<?php
declare(strict_types=1);

namespace Welafix\Database;

use PDO;
use Welafix\Database\SqliteGuardedPdo;

final class Db
{
    private static ?ConnectionFactory $factory = null;

    public static function setFactory(ConnectionFactory $factory): void
    {
        self::$factory = $factory;
    }

    public static function mssql(): PDO
    {
        return self::factory()->mssql();
    }

    public static function sqlite(): PDO
    {
        return self::factory()->sqlite();
    }

    public static function guardMssql(PDO $pdo, string $component = ''): PDO
    {
        $driver = $pdo->getAttribute(PDO::ATTR_DRIVER_NAME);
        if ($driver !== 'sqlsrv') {
            $componentLabel = $component !== '' ? $component : 'unbekannt';
            $sqlitePath = (string)env('SQLITE_PATH', '');
            $message = "Komponente {$componentLabel} erwartet MSSQL (sqlsrv), bekam aber DRIVER={$driver}.";
            if ($sqlitePath !== '') {
                $message .= " SQLITE_PATH={$sqlitePath}";
            }
            throw new \RuntimeException($message);
        }
        return $pdo;
    }

    public static function guardSqlite(PDO $pdo, string $component = ''): PDO
    {
        if ($pdo instanceof SqliteGuardedPdo) {
            $pdo->setComponent($component);
        }
        return $pdo;
    }

    public static function media(): PDO
    {
        return self::factory()->media();
    }

    public static function ensureMigrated(): void
    {
        $factory = self::factory();
        $factory->ensureSqliteMigrated();
        $factory->ensureMediaMigrated();
    }

    private static function factory(): ConnectionFactory
    {
        if (self::$factory === null) {
            self::$factory = new ConnectionFactory();
        }
        return self::$factory;
    }
}
