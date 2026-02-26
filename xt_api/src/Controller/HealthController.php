<?php
declare(strict_types=1);

namespace XtApi\Controller;

use DateTimeImmutable;
use DateTimeZone;
use XtApi\Db\MySql;
use XtApi\Http\Response;

final class HealthController
{
    public function health(): void
    {
        $dbOk = false;
        try {
            $pdo = MySql::connect();
            $pdo->query('SELECT 1');
            $dbOk = true;
        } catch (\Throwable $e) {
            error_log('xt_api db error: ' . $e->getMessage());
        }

        Response::json([
            'ok' => $dbOk,
            'db' => $dbOk,
            'time' => (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM),
            'version' => '1.0',
        ]);
    }

    public function version(): void
    {
        Response::json([
            'ok' => true,
            'version' => '1.0',
        ]);
    }
}
