<?php
declare(strict_types=1);

namespace Welafix\Bootstrap;

use Welafix\Database\ConnectionFactory;
use Welafix\Config\MappingLoader;
use Welafix\Http\Controllers\DashboardController;
use Welafix\Domain\Warengruppe\WarengruppeSyncService;
use Welafix\Http\Controllers\ApiController;

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
                echo json_encode(['error' => $e->getMessage()]);
            }
            return;
        }

        http_response_code(404);
        echo "404 Not Found";
    }
}
