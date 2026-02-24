<?php
declare(strict_types=1);

// Minimaler PSR-4 Autoloader für Namespace "Welafix\"
spl_autoload_register(function (string $class): void {
    $prefix = 'Welafix\\';
    $baseDir = __DIR__ . '/../'; // zeigt auf /src

    // nur Welafix\... laden
    if (strncmp($prefix, $class, strlen($prefix)) !== 0) {
        return;
    }

    // Welafix\Foo\Bar -> Foo/Bar.php
    $relativeClass = substr($class, strlen($prefix));
    $file = $baseDir . str_replace('\\', '/', $relativeClass) . '.php';

    if (is_file($file)) {
        require $file;
    }
});