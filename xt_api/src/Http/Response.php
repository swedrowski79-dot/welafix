<?php
declare(strict_types=1);

namespace XtApi\Http;

final class Response
{
    public static function json(array $payload, int $status = 200): void
    {
        http_response_code($status);
        header('Content-Type: application/json; charset=utf-8');
        echo json_encode($payload, JSON_PRETTY_PRINT);
    }

    public static function notFound(): void
    {
        http_response_code(404);
        header('Content-Type: text/html; charset=utf-8');
        echo "<!doctype html><html><head><title>404</title></head><body><h1>404 Not Found</h1></body></html>";
    }
}
