<?php
declare(strict_types=1);

namespace Welafix\Domain\Meta;

use PDO;
use Welafix\Database\ConnectionFactory;

final class MetaFillService
{
    public function __construct(private ConnectionFactory $factory) {}

    /**
     * @return array<string, mixed>
     */
    public function run(): array
    {
        $pdo = $this->factory->sqlite();

        $artikelRows = $pdo->query('SELECT * FROM artikel')?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $artikelExtraRows = $pdo->query('SELECT * FROM artikel_extra_data')?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $artikelExtraMap = $this->indexExtraRows($artikelExtraRows, 'Artikelnummer');

        $warengruppeRows = $pdo->query('SELECT * FROM warengruppe')?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $warengruppeExtraRows = $pdo->query('SELECT * FROM warengruppe_extra_data')?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $warengruppeExtraMap = $this->indexExtraRows($warengruppeExtraRows, 'warengruppenname');

        $existingArtikel = $this->loadExistingMetaArtikel($pdo);
        $existingWarengruppen = $this->loadExistingMetaWarengruppen($pdo);

        $artikelUpsert = $pdo->prepare(
            'INSERT INTO Meta_Data_Artikel (afs_artikel_id, artikelnummer, meta_title, meta_description, updated)
             VALUES (:afs_artikel_id, :artikelnummer, :meta_title, :meta_description, :updated)
             ON CONFLICT(afs_artikel_id) DO UPDATE SET
               artikelnummer = excluded.artikelnummer,
               meta_title = excluded.meta_title,
               meta_description = excluded.meta_description,
               updated = excluded.updated'
        );
        $warengruppeUpsert = $pdo->prepare(
            'INSERT INTO Meta_Data_Waregruppen (afs_wg_id, warengruppenname, meta_title, meta_description, updated)
             VALUES (:afs_wg_id, :warengruppenname, :meta_title, :meta_description, :updated)
             ON CONFLICT(afs_wg_id) DO UPDATE SET
               warengruppenname = excluded.warengruppenname,
               meta_title = excluded.meta_title,
               meta_description = excluded.meta_description,
               updated = excluded.updated'
        );

        $stats = [
            'ok' => true,
            'artikel' => [
                'source_count' => count($artikelRows),
                'written' => 0,
                'updated' => 0,
                'unchanged' => 0,
                'fallback_artikelnummer' => 0,
                'fallback_standard' => 0,
            ],
            'warengruppen' => [
                'source_count' => count($warengruppeRows),
                'written' => 0,
                'updated' => 0,
                'unchanged' => 0,
                'fallback_name' => 0,
                'fallback_standard' => 0,
            ],
        ];

        $pdo->beginTransaction();
        try {
            $seenArtikelIds = [];
            $seenWarengruppeIds = [];

            foreach ($artikelRows as $row) {
                $artikelnummer = trim((string)($row['artikelnummer'] ?? ''));
                $afsArtikelId = trim((string)($row['afs_artikel_id'] ?? ''));
                if ($afsArtikelId === '' || $artikelnummer === '') {
                    continue;
                }

                $source = null;
                if (isset($artikelExtraMap[strtolower($artikelnummer)])) {
                    $source = $artikelExtraMap[strtolower($artikelnummer)];
                    $stats['artikel']['fallback_artikelnummer']++;
                } elseif (isset($artikelExtraMap['standard'])) {
                    $source = $artikelExtraMap['standard'];
                    $stats['artikel']['fallback_standard']++;
                }

                $metaTitle = $this->renderTemplate(
                    $this->readExtraValue($source, 'meta_title'),
                    $row
                );
                $metaDescription = $this->renderTemplate(
                    $this->readExtraValue($source, 'meta_description'),
                    $row
                );

                $isUpdated = $this->hasArtikelChanged($existingArtikel[$afsArtikelId] ?? null, $artikelnummer, $metaTitle, $metaDescription);
                $artikelUpsert->execute([
                    ':afs_artikel_id' => $afsArtikelId,
                    ':artikelnummer' => $artikelnummer,
                    ':meta_title' => $metaTitle,
                    ':meta_description' => $metaDescription,
                    ':updated' => $isUpdated ? 1 : 0,
                ]);
                $seenArtikelIds[$afsArtikelId] = true;
                $stats['artikel']['written']++;
                if ($isUpdated) {
                    $stats['artikel']['updated']++;
                } else {
                    $stats['artikel']['unchanged']++;
                }
            }

            foreach ($warengruppeRows as $row) {
                $afsWgId = (int)($row['afs_wg_id'] ?? 0);
                $name = trim((string)($row['name'] ?? ''));
                if ($afsWgId <= 0 || $name === '') {
                    continue;
                }

                $source = null;
                if (isset($warengruppeExtraMap[strtolower($name)])) {
                    $source = $warengruppeExtraMap[strtolower($name)];
                    $stats['warengruppen']['fallback_name']++;
                } elseif (isset($warengruppeExtraMap['standard'])) {
                    $source = $warengruppeExtraMap['standard'];
                    $stats['warengruppen']['fallback_standard']++;
                }

                $metaTitle = $this->renderTemplate(
                    $this->readExtraValue($source, 'meta_title'),
                    $row
                );
                $metaDescription = $this->renderTemplate(
                    $this->readExtraValue($source, 'meta_description'),
                    $row
                );

                $isUpdated = $this->hasWarengruppeChanged($existingWarengruppen[$afsWgId] ?? null, $name, $metaTitle, $metaDescription);
                $warengruppeUpsert->execute([
                    ':afs_wg_id' => $afsWgId,
                    ':warengruppenname' => $name,
                    ':meta_title' => $metaTitle,
                    ':meta_description' => $metaDescription,
                    ':updated' => $isUpdated ? 1 : 0,
                ]);
                $seenWarengruppeIds[$afsWgId] = true;
                $stats['warengruppen']['written']++;
                if ($isUpdated) {
                    $stats['warengruppen']['updated']++;
                } else {
                    $stats['warengruppen']['unchanged']++;
                }
            }

            $this->deleteMissingMetaArtikel($pdo, array_keys($seenArtikelIds));
            $this->deleteMissingMetaWarengruppen($pdo, array_keys($seenWarengruppeIds));

            $pdo->commit();
        } catch (\Throwable $e) {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            throw $e;
        }

        return $stats;
    }

    /**
     * @param array<int, array<string, mixed>> $rows
     * @return array<string, array<string, mixed>>
     */
    private function indexExtraRows(array $rows, string $keyColumn): array
    {
        $indexed = [];
        foreach ($rows as $row) {
            $key = $this->readExtraValue($row, $keyColumn);
            $key = trim((string)$key);
            if ($key === '') {
                continue;
            }
            $indexed[strtolower($key)] = $row;
        }
        return $indexed;
    }

    /**
     * @param array<string, mixed>|null $row
     */
    private function readExtraValue(?array $row, string $field): ?string
    {
        if ($row === null) {
            return null;
        }
        foreach ($row as $key => $value) {
            if (strcasecmp((string)$key, $field) === 0) {
                if ($value === null) {
                    return null;
                }
                return (string)$value;
            }
        }
        return null;
    }

    /**
     * @param array<string, mixed> $context
     */
    private function renderTemplate(?string $template, array $context): ?string
    {
        if ($template === null || $template === '') {
            return $template;
        }

        $rendered = preg_replace_callback('/\{\{\s*([A-Za-z0-9_]+)\s*\}\}/', function (array $matches) use ($context): string {
            $field = (string)($matches[1] ?? '');
            if ($field === '') {
                return '';
            }

            foreach ($context as $key => $value) {
                if (strcasecmp((string)$key, $field) !== 0) {
                    continue;
                }
                if ($value === null) {
                    return '';
                }
                return (string)$value;
            }

            return '';
        }, $template);

        return $rendered ?? $template;
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function loadExistingMetaArtikel(PDO $pdo): array
    {
        $rows = $pdo->query('SELECT * FROM Meta_Data_Artikel')?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $map = [];
        foreach ($rows as $row) {
            $id = trim((string)($row['afs_artikel_id'] ?? ''));
            if ($id !== '') {
                $map[$id] = $row;
            }
        }
        return $map;
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function loadExistingMetaWarengruppen(PDO $pdo): array
    {
        $rows = $pdo->query('SELECT * FROM Meta_Data_Waregruppen')?->fetchAll(PDO::FETCH_ASSOC) ?: [];
        $map = [];
        foreach ($rows as $row) {
            $id = (int)($row['afs_wg_id'] ?? 0);
            if ($id > 0) {
                $map[$id] = $row;
            }
        }
        return $map;
    }

    /**
     * @param array<string, mixed>|null $existing
     */
    private function hasArtikelChanged(?array $existing, string $artikelnummer, ?string $metaTitle, ?string $metaDescription): bool
    {
        if ($existing === null) {
            return true;
        }
        return (string)($existing['artikelnummer'] ?? '') !== $artikelnummer
            || (string)($existing['meta_title'] ?? '') !== (string)($metaTitle ?? '')
            || (string)($existing['meta_description'] ?? '') !== (string)($metaDescription ?? '');
    }

    /**
     * @param array<string, mixed>|null $existing
     */
    private function hasWarengruppeChanged(?array $existing, string $name, ?string $metaTitle, ?string $metaDescription): bool
    {
        if ($existing === null) {
            return true;
        }
        return (string)($existing['warengruppenname'] ?? '') !== $name
            || (string)($existing['meta_title'] ?? '') !== (string)($metaTitle ?? '')
            || (string)($existing['meta_description'] ?? '') !== (string)($metaDescription ?? '');
    }

    /**
     * @param array<int, string> $seenIds
     */
    private function deleteMissingMetaArtikel(PDO $pdo, array $seenIds): void
    {
        if ($seenIds === []) {
            $pdo->exec('DELETE FROM Meta_Data_Artikel');
            return;
        }
        $placeholders = implode(', ', array_fill(0, count($seenIds), '?'));
        $stmt = $pdo->prepare('DELETE FROM Meta_Data_Artikel WHERE afs_artikel_id NOT IN (' . $placeholders . ')');
        foreach ($seenIds as $index => $id) {
            $stmt->bindValue($index + 1, $id, PDO::PARAM_STR);
        }
        $stmt->execute();
    }

    /**
     * @param array<int, int> $seenIds
     */
    private function deleteMissingMetaWarengruppen(PDO $pdo, array $seenIds): void
    {
        if ($seenIds === []) {
            $pdo->exec('DELETE FROM Meta_Data_Waregruppen');
            return;
        }
        $placeholders = implode(', ', array_fill(0, count($seenIds), '?'));
        $stmt = $pdo->prepare('DELETE FROM Meta_Data_Waregruppen WHERE afs_wg_id NOT IN (' . $placeholders . ')');
        foreach ($seenIds as $index => $id) {
            $stmt->bindValue($index + 1, (int)$id, PDO::PARAM_INT);
        }
        $stmt->execute();
    }
}
