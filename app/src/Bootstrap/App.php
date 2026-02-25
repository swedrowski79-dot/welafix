<?php
declare(strict_types=1);

namespace Welafix\Bootstrap;

use Welafix\Database\ConnectionFactory;
use Welafix\Config\MappingLoader;
use Welafix\Http\Controllers\DashboardController;
use Welafix\Http\Controllers\SqliteBrowserController;
use Welafix\Domain\Warengruppe\WarengruppeSyncService;
use Welafix\Http\Controllers\ApiController;
use Welafix\Domain\Artikel\ArtikelSyncService;
use Welafix\Domain\Media\MediaImporter;

final class App
{
    public function run(): void
    {
        // very small router for now
        $path = parse_url($_SERVER['REQUEST_URI'] ?? '/', PHP_URL_PATH) ?: '/';

        // ensure SQLite exists / migrated
        $factory = new ConnectionFactory();
        $factory->ensureSqliteMigrated();
        $factory->ensureMediaMigrated();

        if ($path === '/' || $path === '/dashboard') {
            (new DashboardController($factory))->index();
            return;
        }

        if ($path === '/dashboard/sqlite') {
            (new DashboardController($factory))->sqliteBrowser();
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

        if ($path === '/api/documents') {
            (new ApiController($factory))->documentsList();
            return;
        }

        if (preg_match('#^/api/documents/(\d+)$#', $path, $matches)) {
            (new ApiController($factory))->documentDetail((int)$matches[1]);
            return;
        }

        if ($path === '/api/sync-state') {
            (new ApiController($factory))->syncState();
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

        if ($path === '/api/sqlite/tables') {
            (new SqliteBrowserController($factory))->tables();
            return;
        }

        if ($path === '/api/sqlite/table') {
            (new SqliteBrowserController($factory))->table();
            return;
        }

        if ($path === '/api/media') {
            (new ApiController($factory))->mediaList();
            return;
        }

        if ($path === '/api/media/stats') {
            (new ApiController($factory))->mediaStats();
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
                $context = null;
                if (isset($service) && method_exists($service, 'getLastSql')) {
                    $sql = $service->getLastSql();
                }
                if (isset($service) && method_exists($service, 'getLastContext')) {
                    $context = $service->getLastContext();
                }
                echo json_encode([
                    'error' => $e->getMessage(),
                    'sql' => $sql ? $this->truncateSql($sql) : null,
                    'context' => $context,
                ]);
            }
            return;
        }

        if ($path === '/sync/artikel') {
            header('Content-Type: application/json');
            try {
                $service = new ArtikelSyncService($factory);
                $batch = isset($_GET['batch']) ? (int)$_GET['batch'] : 500;
                $after = isset($_GET['after']) ? (string)$_GET['after'] : '';
                $maxSeconds = isset($_GET['max_seconds']) ? (int)$_GET['max_seconds'] : 0;

                $batch = max(1, min(1000, $batch));
                $start = microtime(true);
                $stats = $service->processBatch($after, $batch);

                if ($maxSeconds > 0) {
                    while (!$stats['done']) {
                        if ((microtime(true) - $start) >= $maxSeconds) {
                            break;
                        }
                        $stats = $service->processBatch($stats['last_key'], $batch);
                    }
                }

                echo json_encode($stats);
            } catch (\Throwable $e) {
                http_response_code(500);
                $sql = null;
                $params = null;
                if (isset($service) && method_exists($service, 'getLastSql')) {
                    $sql = $service->getLastSql();
                }
                if (isset($service) && method_exists($service, 'getLastParams')) {
                    $params = $service->getLastParams();
                }
                echo json_encode([
                    'error' => $e->getMessage(),
                    'sql' => $sql ? $this->truncateSql($sql) : null,
                    'params' => $params,
                ]);
            }
            return;
        }

        if ($path === '/sync/media/images') {
            header('Content-Type: application/json');
            try {
                $importer = new MediaImporter($factory);
                $stats = $importer->importArticleImages();
                echo json_encode($stats);
            } catch (\Throwable $e) {
                http_response_code(500);
                echo json_encode(['error' => $e->getMessage()]);
            }
            return;
        }

        if ($path === '/sync/media/documents') {
            header('Content-Type: application/json');
            try {
                $importer = new MediaImporter($factory);
                $stats = $importer->importDocuments();
                echo json_encode($stats);
            } catch (\Throwable $e) {
                http_response_code(500);
                echo json_encode(['error' => $e->getMessage()]);
            }
            return;
        }


        http_response_code(404);
        $view = __DIR__ . '/../Http/Views/404.php';
        $layout = __DIR__ . '/../Http/Views/layout.php';
        $data = [];
        require $layout;
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
