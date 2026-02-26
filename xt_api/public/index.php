<?php
declare(strict_types=1);

require __DIR__ . '/../src/Bootstrap.php';

use XtApi\Http\Router;
use XtApi\Security\ApiAuth;
use XtApi\Controller\HealthController;

$router = new Router();
$auth = new ApiAuth();
$health = new HealthController();

$router->get('/health', function () use ($auth, $health) {
    $auth->requireAuth();
    $health->health();
});

$router->get('/version', function () use ($auth, $health) {
    $auth->requireAuth();
    $health->version();
});

$router->dispatch();
