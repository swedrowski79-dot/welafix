<?php
declare(strict_types=1);

namespace Welafix\Domain\Xt;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use Welafix\Database\ConnectionFactory;

final class XtDeletedSyncService
{
    public function __construct(private ConnectionFactory $factory) {}

    /**
     * @return array<string, int>
     */
    public function prepareDelta(): array
    {
        $pdo = $this->factory->sqlite();
        $productIds = $this->lookupXtIds($pdo, 'xt_products', 'products_id', 'external_id', 'SELECT afs_artikel_id FROM artikel WHERE changed = 1');
        $relationProductIds = $this->lookupXtIds($pdo, 'xt_products', 'products_id', 'external_id', 'SELECT afs_artikel_id FROM artikel_warengruppe WHERE changed = 1');
        $attributeProductIds = $this->lookupXtIds($pdo, 'xt_products', 'products_id', 'external_id', 'SELECT afs_artikel_id FROM artikel_attribute_map WHERE changed = 1');
        $mediaProductIds = $this->lookupXtIds($pdo, 'xt_products', 'products_id', 'external_id', 'SELECT afs_artikel_id FROM artikel_media_map WHERE changed = 1');
        $categoryIds = $this->lookupXtIds($pdo, 'xt_categories', 'categories_id', 'external_id', 'SELECT afs_wg_id FROM warengruppe WHERE changed = 1');
        $relationCategoryIds = $this->lookupXtIds($pdo, 'xt_categories', 'categories_id', 'external_id', 'SELECT afs_wg_id FROM artikel_warengruppe WHERE changed = 1');

        return [
            'products_to_categories_deleted' => $this->deleteByIds($pdo, 'xt_products_to_categories', 'products_id', array_values(array_unique(array_merge($productIds, $relationProductIds))))
                + $this->deleteByIds($pdo, 'xt_products_to_categories', 'categories_id', array_values(array_unique(array_merge($categoryIds, $relationCategoryIds)))),
            'attributes_deleted' => $this->deleteByIds($pdo, 'xt_plg_products_to_attributes', 'products_id', $attributeProductIds),
            'media_links_deleted' => $this->deleteByIds($pdo, 'xt_media_link', 'link_id', $mediaProductIds),
        ];
    }

    /**
     * @return array<string, int>
     */
    public function cleanupDeleted(): array
    {
        $pdo = $this->factory->sqlite();
        $deletedProductIds = $this->lookupXtIds($pdo, 'xt_products', 'products_id', 'external_id', 'SELECT afs_artikel_id FROM artikel WHERE COALESCE(is_deleted,0) = 1');
        $deletedCategoryIds = $this->lookupXtIds($pdo, 'xt_categories', 'categories_id', 'external_id', 'SELECT afs_wg_id FROM warengruppe WHERE COALESCE(is_deleted,0) = 1');
        $deletedMediaIds = $this->lookupXtIds($pdo, 'xt_media', 'id', 'external_id', 'SELECT id FROM media WHERE COALESCE(is_deleted,0) = 1');

        return [
            'products_disabled' => $this->disableByIds($pdo, 'xt_products', 'products_id', 'products_status', $deletedProductIds),
            'categories_disabled' => $this->disableByIds($pdo, 'xt_categories', 'categories_id', 'categories_status', $deletedCategoryIds),
            'media_disabled' => $this->disableByIds($pdo, 'xt_media', 'id', 'status', $deletedMediaIds),
            'products_to_categories_deleted' => $this->deleteByIds($pdo, 'xt_products_to_categories', 'products_id', $deletedProductIds)
                + $this->deleteByIds($pdo, 'xt_products_to_categories', 'categories_id', $deletedCategoryIds),
            'attributes_deleted' => $this->deleteByIds($pdo, 'xt_plg_products_to_attributes', 'products_id', $deletedProductIds),
            'media_links_deleted' => $this->deleteByIds($pdo, 'xt_media_link', 'link_id', $deletedProductIds)
                + $this->deleteByIds($pdo, 'xt_media_link', 'm_id', $deletedMediaIds),
            'seo_products_deleted' => $this->deleteSeoByIds($pdo, 1, $deletedProductIds),
            'seo_categories_deleted' => $this->deleteSeoByIds($pdo, 2, $deletedCategoryIds),
        ] + $this->offlineInvalidCategoryProducts($pdo);
    }

    /**
     * @return array<int, string>
     */
    private function lookupXtIds(PDO $pdo, string $xtTable, string $xtIdColumn, string $xtExternalColumn, string $sourceSql): array
    {
        $sql = 'SELECT x.' . $this->quoteIdentifier($xtIdColumn) . ' AS id
                FROM ' . $this->quoteIdentifier($xtTable) . ' x
                JOIN (' . $sourceSql . ') s ON CAST(s.' . $this->quoteIdentifier('afs_artikel_id') . ' AS TEXT) = CAST(x.' . $this->quoteIdentifier($xtExternalColumn) . ' AS TEXT)';
        if (str_contains($sourceSql, 'afs_wg_id')) {
            $sql = 'SELECT x.' . $this->quoteIdentifier($xtIdColumn) . ' AS id
                    FROM ' . $this->quoteIdentifier($xtTable) . ' x
                    JOIN (' . $sourceSql . ') s ON CAST(s.' . $this->quoteIdentifier('afs_wg_id') . ' AS TEXT) = CAST(x.' . $this->quoteIdentifier($xtExternalColumn) . ' AS TEXT)';
        }
        if (preg_match('/SELECT id FROM media/', $sourceSql)) {
            $sql = 'SELECT x.' . $this->quoteIdentifier($xtIdColumn) . ' AS id
                    FROM ' . $this->quoteIdentifier($xtTable) . ' x
                    JOIN (' . $sourceSql . ') s ON CAST(s.id AS TEXT) = CAST(x.' . $this->quoteIdentifier($xtExternalColumn) . ' AS TEXT)';
        }
        $stmt = $pdo->query($sql);
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        return array_values(array_filter(array_map(static fn(array $row): string => trim((string)($row['id'] ?? '')), $rows)));
    }

    /**
     * @param array<int, string> $ids
     */
    private function deleteByIds(PDO $pdo, string $table, string $column, array $ids): int
    {
        if ($ids === []) {
            return 0;
        }
        $total = 0;
        foreach (array_chunk($ids, 500) as $chunk) {
            $placeholders = implode(',', array_fill(0, count($chunk), '?'));
            $sql = 'DELETE FROM ' . $this->quoteIdentifier($table) . ' WHERE ' . $this->quoteIdentifier($column) . ' IN (' . $placeholders . ')';
            $stmt = $pdo->prepare($sql);
            $stmt->execute($chunk);
            $total += $stmt->rowCount();
        }
        return $total;
    }

    /**
     * @param array<int, string> $ids
     */
    private function disableByIds(PDO $pdo, string $table, string $idColumn, string $statusColumn, array $ids): int
    {
        if ($ids === []) {
            return 0;
        }
        $total = 0;
        foreach (array_chunk($ids, 500) as $chunk) {
            $placeholders = implode(',', array_fill(0, count($chunk), '?'));
            $sql = 'UPDATE ' . $this->quoteIdentifier($table) . '
                    SET ' . $this->quoteIdentifier($statusColumn) . ' = 0, changed = 1
                    WHERE ' . $this->quoteIdentifier($idColumn) . ' IN (' . $placeholders . ')';
            $stmt = $pdo->prepare($sql);
            $stmt->execute($chunk);
            $total += $stmt->rowCount();
        }
        return $total;
    }

    /**
     * @param array<int, string> $linkIds
     */
    private function deleteSeoByIds(PDO $pdo, int $linkType, array $linkIds): int
    {
        if ($linkIds === []) {
            return 0;
        }
        $total = 0;
        foreach (array_chunk($linkIds, 500) as $chunk) {
            $placeholders = implode(',', array_fill(0, count($chunk), '?'));
            $sql = 'DELETE FROM xt_seo_url WHERE link_type = ? AND link_id IN (' . $placeholders . ')';
            $stmt = $pdo->prepare($sql);
            $stmt->execute(array_merge([$linkType], $chunk));
            $total += $stmt->rowCount();
        }
        return $total;
    }

    private function quoteIdentifier(string $name): string
    {
        return '"' . str_replace('"', '""', $name) . '"';
    }

    /**
     * @return array<string, int>
     */
    private function offlineInvalidCategoryProducts(PDO $pdo): array
    {
        $rows = $pdo->query(
            'SELECT DISTINCT p.products_id, x.external_id AS afs_artikel_id, a.artikelnummer
             FROM xt_products_to_categories p
             JOIN xt_products x ON x.products_id = p.products_id
             LEFT JOIN artikel a ON a.afs_artikel_id = x.external_id
             WHERE p.categories_id IS NULL OR CAST(p.categories_id AS TEXT) = \'\''
        )?->fetchAll(PDO::FETCH_ASSOC) ?: [];

        if ($rows === []) {
            return [
                'invalid_category_products_offlined' => 0,
                'invalid_products_to_categories_deleted' => 0,
            ];
        }

        $productIds = [];
        foreach ($rows as $row) {
            $id = trim((string)($row['products_id'] ?? ''));
            if ($id !== '') {
                $productIds[] = $id;
            }
            $artikelnummer = trim((string)($row['artikelnummer'] ?? ''));
            $afsArtikelId = trim((string)($row['afs_artikel_id'] ?? ''));
            $this->logInvalidCategoryArticle($artikelnummer, $afsArtikelId);
        }
        $productIds = array_values(array_unique($productIds));

        $offlined = $this->disableByIds($pdo, 'xt_products', 'products_id', 'products_status', $productIds);
        $deleted = 0;
        $stmt = $pdo->prepare('DELETE FROM xt_products_to_categories WHERE categories_id IS NULL OR CAST(categories_id AS TEXT) = \'\'');
        $stmt->execute();
        $deleted += $stmt->rowCount();

        return [
            'invalid_category_products_offlined' => $offlined,
            'invalid_products_to_categories_deleted' => $deleted,
        ];
    }

    private function logInvalidCategoryArticle(string $artikelnummer, string $afsArtikelId): void
    {
        $timestamp = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM);
        $line = sprintf(
            "[%s] invalid_category_offline artikelnummer=%s afs_artikel_id=%s\n",
            $timestamp,
            $artikelnummer !== '' ? $artikelnummer : '-',
            $afsArtikelId !== '' ? $afsArtikelId : '-'
        );
        $path = __DIR__ . '/../../../logs/app.log';
        @file_put_contents($path, $line, FILE_APPEND);
    }
}
