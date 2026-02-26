<?php
declare(strict_types=1);

namespace XtApi\Security;

use XtApi\Http\Response;
use function XtApi\env;

final class ApiAuth
{
    public function requireAuth(): void
    {
        $ts = $_SERVER['HTTP_X_API_TS'] ?? '';
        $sig = $_SERVER['HTTP_X_API_SIG'] ?? '';
        $keyId = $_SERVER['HTTP_X_API_KEY'] ?? '';

        if ($ts === '' || $sig === '' || $keyId === '') {
            $this->deny();
        }

        if (!$this->isFreshTimestamp($ts)) {
            $this->deny();
        }

        $secret = (string)env('XT_API_KEY', '');
        if ($secret === '') {
            $this->deny();
        }

        $method = $_SERVER['REQUEST_METHOD'] ?? 'GET';
        $path = parse_url($_SERVER['REQUEST_URI'] ?? '/', PHP_URL_PATH) ?: '/';
        $body = file_get_contents('php://input') ?: '';
        $base = $method . "\n" . $path . "\n" . $ts . "\n" . $body;
        $expected = hash_hmac('sha256', $base, $secret);

        if (!hash_equals($expected, $sig)) {
            $this->deny();
        }
    }

    private function isFreshTimestamp(string $ts): bool
    {
        if (!ctype_digit($ts)) {
            return false;
        }
        $now = time();
        $diff = abs($now - (int)$ts);
        return $diff <= 300;
    }

    private function deny(): void
    {
        // Security: return 404 for invalid key/signature
        Response::notFound();
        exit;
    }
}
