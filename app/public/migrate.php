<?php
declare(strict_types=1);

require __DIR__ . '/../src/Bootstrap/autoload.php';

\Welafix\Bootstrap\Env::load(__DIR__ . '/../.env');

$factory = new \Welafix\Database\ConnectionFactory();
\Welafix\Database\Db::setFactory($factory);
\Welafix\Database\Db::ensureMigrated();

echo "OK\n";
