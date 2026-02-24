<?php
declare(strict_types=1);

namespace Welafix\Domain\Artikel;

use PDO;
use RuntimeException;

final class ArtikelRepositoryMssql
{
    private PDO $pdo;
    private ?string $lastSql = null;

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    public function fetchTop(int $limit = 100): array
    {
        $limit = max(1, min(1000, $limit));

        $sql = "SELECT TOP {$limit}
            Artikelnummer,
            Bezeichnung,
            VK3,
            Warengruppe,
            Bestand,
            Internet,
            [Update]
          FROM dbo.Artikel
          WHERE Mandant = 1
            AND Art < 255
            AND Artikelnummer IS NOT NULL
            AND Internet = 1";

        $this->lastSql = $sql;

        try {
            $stmt = $this->pdo->query($sql);
            return $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (\Throwable $e) {
            throw new RuntimeException('MSSQL Query fehlgeschlagen: ' . $e->getMessage(), 0, $e);
        }
    }

    public function getLastSql(): ?string
    {
        return $this->lastSql;
    }
}
