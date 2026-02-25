<?php
declare(strict_types=1);

namespace Welafix\Http\Controllers;

use Welafix\Database\ConnectionFactory;
use PDO;

final class DashboardController
{
    public function __construct(private ConnectionFactory $factory) {}

    public function index(): void
    {
        $pdo = $this->factory->sqlite();

        $counts = [
            'artikel_changed' => (int)$pdo->query("SELECT COUNT(*) FROM artikel WHERE changed = 1")->fetchColumn(),
            'warengruppe_changed' => (int)$pdo->query("SELECT COUNT(*) FROM warengruppe WHERE changed = 1")->fetchColumn(),
            'media_changed' => (int)$pdo->query("SELECT COUNT(*) FROM media_files WHERE changed = 1")->fetchColumn(),
        ];

        $view = __DIR__ . '/../Views/dashboard.php';
        $layout = __DIR__ . '/../Views/layout.php';

        $data = compact('counts');

        require $layout;
    }

    public function sqliteBrowser(): void
    {
        $view = __DIR__ . '/../Views/sqlite_browser.php';
        $layout = __DIR__ . '/../Views/layout.php';
        $data = [];

        require $layout;
    }
}
