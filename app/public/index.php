<?php
declare(strict_types=1);

require __DIR__ . '/../src/Bootstrap/autoload.php';

\Welafix\Bootstrap\Env::load(__DIR__ . '/../.env');

// Basic front controller
$app = new \Welafix\Bootstrap\App();
$app->run();
