<?php
declare(strict_types=1);

namespace Welafix\Bootstrap;

use Welafix\Database\ConnectionFactory;
use Welafix\Config\MappingLoader;
use Welafix\Http\Controllers\DashboardController;
use Welafix\Domain\Warengruppe\WarengruppeSyncService;
use Welafix\Http\Controllers\ApiController;
use Welafix\Domain\Artikel\ArtikelSyncService;

final class App
{
    public function run(): void
    {
        // very small router for now
        $path = parse_url($_SERVER['REQUEST_URI'] ?? '/', PHP_URL_PATH) ?: '/';

        // ensure SQLite exists / migrated
        $factory = new ConnectionFactory();
        $factory->ensureSqliteMigrated();

        if ($path === '/' || $path === '/dashboard') {
            (new DashboardController($factory))->index();
            return;
        }

        if ($path === '/health') {
            header('Content-Type: application/json');
            echo json_encode(['ok' => true]);
            return;
        }

        if ($path === '/mappings') {
            header('Content-Type: application/json');

            try {
                $loader = new MappingLoader();
                $mappings = $loader->loadAll();
            } catch (\RuntimeException $e) {
                http_response_code(500);
                echo json_encode(['error' => $e->getMessage()]);
                return;
            }

            $output = [];
            foreach ($mappings as $name => $mapping) {
                $source = $mapping['source'] ?? [];
                $output[] = [
                    'mapping-name' => $name,
                    'table' => $source['table'] ?? null,
                    'key' => $source['key'] ?? null,
                    'where' => $source['where'] ?? null,
                    'select' => $mapping['select'] ?? [],
                ];
            }

            echo json_encode($output);
            return;
        }

        if ($path === '/api/status') {
            (new ApiController($factory))->status();
            return;
        }

        if ($path === '/api/test-mssql') {
            (new ApiController($factory))->testMssql();
            return;
        }

        if ($path === '/api/test-sqlite') {
            (new ApiController($factory))->testSqlite();
            return;
        }

        if ($path === '/sync/warengruppe') {
            header('Content-Type: application/json');
            try {
                $service = new WarengruppeSyncService($factory);
                $stats = $service->runImportAndBuildPaths();
                echo json_encode($stats);
            } catch (\Throwable $e) {
                http_response_code(500);
                $sql = null;
                if (isset($service) && method_exists($service, 'getLastSql')) {
                    $sql = $service->getLastSql();
                }
                echo json_encode([
                    'error' => $e->getMessage(),
                    'sql' => $sql ? $this->truncateSql($sql) : null,
                ]);
            }
            return;
        }

        if ($path === '/sync/artikel') {
            header('Content-Type: application/json');
            try {
                $service = new ArtikelSyncService($factory);
                $stats = $service->runImport();
                echo json_encode($stats);
            } catch (\Throwable $e) {
                http_response_code(500);
                echo json_encode(['error' => $e->getMessage()]);
            }
            return;
        }

        http_response_code(404);
        echo "404 Not Found";
    }

    private function truncateSql(string $sql): string
    {
        $max = 300;
        $sql = trim(preg_replace('/\s+/', ' ', $sql) ?? $sql);
        if (strlen($sql) <= $max) {
            return $sql;
        }
        return substr($sql, 0, $max) . '...';
    }
}
