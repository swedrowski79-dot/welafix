<?php
declare(strict_types=1);

namespace Welafix\Bootstrap;

use Welafix\Database\ConnectionFactory;
use Welafix\Database\Db;
use Welafix\Config\MappingLoader;
use Welafix\Http\Controllers\DashboardController;
use Welafix\Http\Controllers\SqliteBrowserController;
use Welafix\Domain\Warengruppe\WarengruppeSyncService;
use Welafix\Http\Controllers\ApiController;
use Welafix\Domain\Artikel\ArtikelSyncService;
use Welafix\Domain\Export\TemplateExportService;
use Welafix\Domain\Sync\DailyDeltaSyncService;

final class App
{
    public function run(): void
    {
        // very small router for now
        $path = parse_url($_SERVER['REQUEST_URI'] ?? '/', PHP_URL_PATH) ?: '/';

        // ensure SQLite exists / migrated
        $factory = new ConnectionFactory();
        Db::setFactory($factory);

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
                echo json_encode(['error' => $e->getMessage(), 'sql' => null, 'params' => null]);
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

        if ($path === '/api/artikel') {
            (new ApiController($factory))->artikelList();
            return;
        }

        if ($path === '/api/warengruppe') {
            (new ApiController($factory))->warengruppeList();
            return;
        }

        if ($path === '/api/sync-state') {
            (new ApiController($factory))->syncState();
            return;
        }

        if ($path === '/api/filedb/check') {
            (new ApiController($factory))->fileDbCheck();
            return;
        }

        if ($path === '/api/filedb/apply') {
            (new ApiController($factory))->fileDbApply();
            return;
        }
        if ($path === '/api/meta/fill') {
            (new ApiController($factory))->fillMeta();
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
        if ($path === '/api/sqlite/clear') {
            (new SqliteBrowserController($factory))->clearTable();
            return;
        }
        if ($path === '/api/sqlite/drop') {
            (new SqliteBrowserController($factory))->dropTable();
            return;
        }
        if ($path === '/api/sqlite/update-cell') {
            (new SqliteBrowserController($factory))->updateCell();
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
        if ($path === '/api/media/usage') {
            (new ApiController($factory))->mediaUsage();
            return;
        }
        if ($path === '/api/xt/check') {
            (new ApiController($factory))->xtApiCheck();
            return;
        }
        if ($path === '/api/settings') {
            (new ApiController($factory))->settings();
            return;
        }
        if ($path === '/api/xt/schema/tables') {
            (new ApiController($factory))->xtSchemaTables();
            return;
        }
        if ($path === '/api/xt/schema/table') {
            (new ApiController($factory))->xtSchemaTable();
            return;
        }
        if ($path === '/api/xt/import-table') {
            (new ApiController($factory))->xtImportTable();
            return;
        }
        if ($path === '/sync/xt') {
            header('Content-Type: application/json');
            http_response_code(410);
            echo json_encode([
                'ok' => false,
                'error' => 'Der Legacy-Pfad /sync/xt ist deaktiviert. Verwende /sync/xt-mapping oder /sync/xt-full.',
                'legacy_note' => 'legacy/xt-import-path/README.md',
            ]);
            return;
        }
        if ($path === '/sync/xt-mapping') {
            header('Content-Type: application/json');
            try {
                $start = microtime(true);
                $reconcile = new \Welafix\Domain\Afs\AfsVisibilityReconcileService($factory);
                $reconcileStats = $reconcile->run();
                $service = new \Welafix\Domain\Xt\XtMappingSyncService();
                $stats = $service->run('welafix_xt');
                $stats['reconcile_deletes'] = $reconcileStats;
                $stats['duration_ms'] = (int)round((microtime(true) - $start) * 1000);
                echo json_encode($stats);
            } catch (\Throwable $e) {
                http_response_code(500);
                echo json_encode(['ok' => false, 'error' => $e->getMessage()]);
            }
            return;
        }
        if ($path === '/sync/xt-apply') {
            header('Content-Type: application/json');
            try {
                $start = microtime(true);
                $apply = new \Welafix\Domain\Xt\XtApiApplyService($factory);
                $stats = $apply->run('welafix_xt');
                $reset = new \Welafix\Domain\Afs\AfsUpdateResetService($factory);
                $stats['reset'] = $reset->run();
                $stats['duration_ms'] = (int)round((microtime(true) - $start) * 1000);
                echo json_encode($stats);
            } catch (\Throwable $e) {
                http_response_code(500);
                echo json_encode(['ok' => false, 'error' => $e->getMessage()]);
            }
            return;
        }
        if ($path === '/sync/daily-delta') {
            header('Content-Type: application/json');
            try {
                $start = microtime(true);
                $batch = isset($_GET['artikel_batch']) ? (int)$_GET['artikel_batch'] : 500;
                $batch = max(1, min(10000, $batch));
                $service = new DailyDeltaSyncService($factory);
                $stats = $service->run($batch);
                $stats['duration_ms'] = (int)round((microtime(true) - $start) * 1000);
                echo json_encode($stats);
            } catch (\Throwable $e) {
                http_response_code(500);
                echo json_encode(['ok' => false, 'error' => $e->getMessage()]);
            }
            return;
        }
        if ($path === '/sync/reconcile-deletes') {
            header('Content-Type: application/json');
            try {
                $start = microtime(true);
                $service = new \Welafix\Domain\Afs\AfsVisibilityReconcileService($factory);
                $stats = $service->run();
                $stats['duration_ms'] = (int)round((microtime(true) - $start) * 1000);
                echo json_encode($stats);
            } catch (\Throwable $e) {
                http_response_code(500);
                echo json_encode(['ok' => false, 'error' => $e->getMessage()]);
            }
            return;
        }
        if ($path === '/sync/xt-full') {
            header('Content-Type: application/json');
            try {
                $start = microtime(true);
                $mappingName = isset($_GET['mapping']) ? (string)$_GET['mapping'] : 'xt_commerce_full_tables';
                $job = isset($_GET['job']) ? (string)$_GET['job'] : null;
                $pageSize = isset($_GET['page_size']) ? (int)$_GET['page_size'] : 2000;
                $service = new \Welafix\Domain\Xt\XtFullTableImportService();
                $stats = $service->run($mappingName, $job, $pageSize);
                $stats['duration_ms'] = (int)round((microtime(true) - $start) * 1000);
                echo json_encode($stats);
            } catch (\Throwable $e) {
                http_response_code(500);
                echo json_encode(['ok' => false, 'error' => $e->getMessage()]);
            }
            return;
        }

        if ($path === '/sync/warengruppe') {
            header('Content-Type: application/json');
            try {
                $start = microtime(true);
                $service = new WarengruppeSyncService($factory);
                $stats = $service->runImportAndBuildPaths();
                try {
                    (new TemplateExportService())->exportWarengruppeTemplates();
                } catch (\Throwable $e) {
                    $stats['template_export_error'] = $e->getMessage();
                }
                $stats['duration_ms'] = (int)round((microtime(true) - $start) * 1000);
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

                $batch = max(1, min(10000, $batch));
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

                try {
                    (new TemplateExportService())->exportArtikelTemplates();
                } catch (\Throwable $e) {
                    $stats['template_export_error'] = $e->getMessage();
                }
                $stats['duration_ms'] = (int)round((microtime(true) - $start) * 1000);
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

        if ($path === '/sync/media') {
            header('Content-Type: application/json');
            try {
                $start = microtime(true);
                $service = new \Welafix\Domain\Media\MediaSyncService($factory);
                $stats = $service->run();
                $stats['duration_ms'] = (int)round((microtime(true) - $start) * 1000);
                echo json_encode($stats);
            } catch (\Throwable $e) {
                http_response_code(500);
                echo json_encode(['error' => $e->getMessage(), 'sql' => null, 'params' => null]);
            }
            return;
        }

        if ($path === '/sync/dokument') {
            header('Content-Type: application/json');
            try {
                $start = microtime(true);
                $service = new \Welafix\Domain\Dokument\DokumentSyncService();
                $stats = $service->run();
                $stats['duration_ms'] = (int)round((microtime(true) - $start) * 1000);
                echo json_encode($stats);
            } catch (\Throwable $e) {
                http_response_code(500);
                $sql = null;
                if (isset($service) && method_exists($service, 'getLastSql')) {
                    $sql = $service->getLastSql();
                }
                echo json_encode(['error' => $e->getMessage(), 'sql' => $sql ? $this->truncateSql($sql) : null, 'params' => null]);
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
