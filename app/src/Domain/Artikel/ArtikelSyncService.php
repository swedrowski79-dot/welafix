<?php
declare(strict_types=1);

namespace Welafix\Domain\Artikel;

use Welafix\Database\ConnectionFactory;

final class ArtikelSyncService
{
    public function __construct(private ConnectionFactory $factory) {}

    /**
     * @return array{ok:bool,imported:int}
     */
    public function runImport(): array
    {
        return [
            'ok' => true,
            'imported' => 0,
        ];
    }
}
