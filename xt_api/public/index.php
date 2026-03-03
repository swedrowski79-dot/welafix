<?php
declare(strict_types=1);

require __DIR__ . '/../src/Bootstrap.php';

use XtApi\Http\Router;
use XtApi\Security\ApiAuth;
use XtApi\Controller\HealthController;
use XtApi\Controller\ExportController;
use XtApi\Controller\SchemaController;
use XtApi\Controller\ExportTableController;

$router = new Router();
$auth = new ApiAuth();
$health = new HealthController();
$export = new ExportController();
$schema = new SchemaController();
$exportTable = new ExportTableController();

$router->get('/health', function () use ($auth, $health) {
    $auth->requireAuth();
    $health->health();
});

$router->get('/version', function () use ($auth, $health) {
    $auth->requireAuth();
    $health->version();
});

$router->post('/export', function () use ($auth, $export) {
    $auth->requireAuth();
    $export->exportFromBody();
});

$router->get('/schema/tables', function () use ($auth, $schema) {
    $auth->requireAuth();
    $schema->tables();
});

$router->getPattern('#^/schema/table/([A-Za-z0-9_]+)$#', function (array $matches) use ($auth, $schema) {
    $auth->requireAuth();
    $schema->table($matches[1] ?? '');
});

$router->getPattern('#^/export/table/([A-Za-z0-9_]+)$#', function (array $matches) use ($auth, $exportTable) {
    $auth->requireAuth();
    $table = $matches[1] ?? '';
    $page = isset($_GET['page']) ? (int)$_GET['page'] : 1;
    $pageSize = isset($_GET['page_size']) ? (int)$_GET['page_size'] : 500;
    $exportTable->export($table, $page, $pageSize);
});

$router->dispatch();
