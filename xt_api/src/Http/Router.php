<?php
declare(strict_types=1);

namespace XtApi\Http;

final class Router
{
    /** @var array<string, array<string, callable>> */
    private array $routes = [];
    /** @var array<string, array<int, array{pattern:string, handler:callable}>> */
    private array $patternRoutes = [];

    public function get(string $path, callable $handler): void
    {
        $this->routes['GET'][$path] = $handler;
    }

    public function post(string $path, callable $handler): void
    {
        $this->routes['POST'][$path] = $handler;
    }

    public function getPattern(string $pattern, callable $handler): void
    {
        $this->patternRoutes['GET'][] = ['pattern' => $pattern, 'handler' => $handler];
    }

    public function dispatch(): void
    {
        $method = $_SERVER['REQUEST_METHOD'] ?? 'GET';
        $uri = $_SERVER['REQUEST_URI'] ?? '/';
        $path = parse_url($uri, PHP_URL_PATH) ?: '/';
        $path = $this->stripBasePath($path);

        if (isset($this->patternRoutes[$method])) {
            foreach ($this->patternRoutes[$method] as $route) {
                if (preg_match($route['pattern'], $path, $matches)) {
                    ($route['handler'])($matches);
                    return;
                }
            }
        }

        $handler = $this->routes[$method][$path] ?? null;
        if (!$handler) {
            Response::notFound();
            return;
        }
        $handler();
    }

    private function stripBasePath(string $path): string
    {
        $script = $_SERVER['SCRIPT_NAME'] ?? '';
        if ($script === '' || !str_ends_with($script, '/index.php')) {
            return $path;
        }
        $base = substr($script, 0, -strlen('/index.php'));
        if ($base !== '' && str_starts_with($path, $base)) {
            $path = substr($path, strlen($base));
        }
        return $path === '' ? '/' : $path;
    }
}
