<?php
declare(strict_types=1);

namespace Welafix\Http\Controllers;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use RuntimeException;
use Welafix\Database\ConnectionFactory;

final class ApiController
{
    public function __construct(private ConnectionFactory $factory) {}

    public function status(): void
    {
        $time = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);
        $this->jsonResponse([
            'ok' => true,
            'time' => $time,
            'app' => 'welafix',
        ]);
    }

    public function testMssql(): void
    {
        try {
            $pdo = $this->factory->mssql();
            $stmt = $pdo->query('SELECT DB_NAME() AS database_name, @@SERVERNAME AS server_name');
            $row = $stmt->fetch(PDO::FETCH_ASSOC) ?: [];

            $server = (string)($row['server_name'] ?? (getenv('MSSQL_HOST') ?: ''));
            $database = (string)($row['database_name'] ?? (getenv('MSSQL_DB') ?: ''));

            $this->jsonResponse([
                'ok' => true,
                'server' => $server,
                'database' => $database,
                'message' => 'LOGIN OK',
            ]);
        } catch (\Throwable $e) {
            $this->jsonResponse([
                'ok' => false,
                'error' => $e->getMessage(),
            ], 500);
        }
    }

    public function testSqlite(): void
    {
        $path = __DIR__ . '/../../../database/app.db';

        try {
            if (!file_exists($path)) {
                throw new RuntimeException('SQLite DB nicht gefunden.');
            }
            if (!is_readable($path)) {
                throw new RuntimeException('SQLite DB ist nicht lesbar.');
            }

            $pdo = new PDO('sqlite:' . $path);
            $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $pdo->query('SELECT 1');

            $this->jsonResponse([
                'ok' => true,
                'path' => $path,
                'readable' => true,
                'writable' => is_writable($path),
            ]);
        } catch (\Throwable $e) {
            $this->jsonResponse([
                'ok' => false,
                'error' => $e->getMessage(),
            ], 500);
        }
    }

    private function jsonResponse(array $payload, int $status = 200): void
    {
        http_response_code($status);
        header('Content-Type: application/json; charset=utf-8');
        echo json_encode($payload, JSON_PRETTY_PRINT);
    }
}
