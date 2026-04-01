<?php
declare(strict_types=1);

namespace Welafix\Domain\Xt;

use PDO;
use RuntimeException;
use Welafix\Database\ConnectionFactory;
use Welafix\Database\Db;

final class XtApiApplyService
{
    /** @var array<string, array<string, mixed>> */
    private array $schemaCache = [];
    /** @var array<string, bool>|null */
    private ?array $remoteSeoKeys = null;
    public function __construct(private ConnectionFactory $factory) {}

    /**
     * @return array<string, mixed>
     */
    public function run(string $mappingName = 'welafix_xt'): array
    {
        $pdo = $this->factory->localDb();
        $this->repairLocalMediaState($pdo);
        $orphans = $this->collectOrphanRelationIds($pdo);
        $mapping = $this->loadMapping($mappingName);
        $targets = $mapping['targets'] ?? [];
        if (!is_array($targets) || $targets === []) {
            throw new RuntimeException('Keine XT-Targets gefunden.');
        }

        $tableOrder = [];
        foreach ($targets as $target) {
            if (!is_array($target)) {
                continue;
            }
            $table = (string)($target['table'] ?? '');
            if ($table !== '' && !in_array($table, $tableOrder, true)) {
                $tableOrder[] = $table;
            }
        }

        $stats = [
            'ok' => true,
            'delete_ops' => [],
            'upserts' => [],
            'file_uploads' => [
                'ok' => true,
                'received' => 0,
                'written' => 0,
                'skipped' => 0,
                'errors' => [],
            ],
            'local_reset' => [],
            'local_cleanup' => [],
        ];

        $affected = $this->collectAffectedIds($pdo, $orphans);
        foreach ($this->buildDeleteOps($affected) as $op) {
            $payload = $op;
            $payload['table'] = $this->remoteTableName((string)$op['table']);
            try {
                $result = $this->request('POST', '/apply', $payload);
            } catch (\Throwable $e) {
                throw new RuntimeException(
                    'XT apply delete failed for table ' . $payload['table'] . ': ' . $e->getMessage(),
                    0,
                    $e
                );
            }
            $stats['delete_ops'][] = $result;
        }

        $stats['local_cleanup'] = $this->cleanupLocalOrphanRelations($pdo);

        foreach ($tableOrder as $table) {
            $rows = $this->loadRowsForPush($pdo, $table, $affected);
            if ($rows === []) {
                $stats['upserts'][$table] = 0;
                continue;
            }
            $remoteTable = $this->remoteTableName($table);
            $schema = $this->fetchTableSchema($remoteTable);
            $keyColumns = $this->resolveKeyColumns($schema);
            if ($keyColumns === []) {
                throw new RuntimeException('Keine Key-Spalten für XT-Tabelle gefunden: ' . $remoteTable);
            }
            $validCols = array_fill_keys(array_map(static fn(array $col): string => (string)$col['name'], $schema['columns'] ?? []), true);
            $filtered = [];
            foreach ($rows as $row) {
                $item = [];
                foreach ($row as $column => $value) {
                    if (isset($validCols[$column])) {
                        $item[$column] = $value;
                    }
                }
                if ($item !== []) {
                    $filtered[] = $item;
                }
            }
            if ($filtered === []) {
                $stats['upserts'][$table] = 0;
                continue;
            }
            try {
                $result = $this->request('POST', '/apply', [
                    'table' => $remoteTable,
                    'mode' => 'upsert',
                    'key_columns' => $keyColumns,
                    'rows' => $filtered,
                ]);
            } catch (\Throwable $e) {
                throw new RuntimeException(
                    'XT apply upsert failed for table ' . $remoteTable . ': ' . $e->getMessage(),
                    0,
                    $e
                );
            }
            $stats['upserts'][$table] = (int)($result['rows'] ?? count($filtered));
        }

        foreach ($tableOrder as $table) {
            if (!$this->tableHasColumn($pdo, $table, 'changed')) {
                continue;
            }
            $stmt = $pdo->prepare('UPDATE ' . $this->quoteIdentifier($table) . ' SET changed = 0 WHERE changed = 1');
            $stmt->execute();
            $stats['local_reset'][$table] = $stmt->rowCount();
        }

        $stats['file_uploads'] = $this->uploadChangedFiles($pdo);

        return $stats;
    }

    /**
     * @return array<string, array<int, string>>
     */
    private function collectAffectedIds(PDO $pdo, array $orphans = []): array
    {
        return [
            'changed_product_ids' => $this->fetchColumn($pdo, 'SELECT products_id FROM xt_products WHERE changed = 1'),
            'deleted_product_ids' => $this->fetchColumn($pdo, 'SELECT products_id FROM xt_products WHERE changed = 1 AND COALESCE(products_status,0) = 0'),
            'changed_category_ids' => $this->fetchColumn($pdo, 'SELECT categories_id FROM xt_categories WHERE changed = 1'),
            'deleted_category_ids' => $this->fetchColumn($pdo, 'SELECT categories_id FROM xt_categories WHERE changed = 1 AND COALESCE(categories_status,0) = 0'),
            'changed_media_ids' => $this->fetchColumn($pdo, 'SELECT id FROM xt_media WHERE changed = 1'),
            'deleted_media_ids' => $this->fetchColumn($pdo, 'SELECT id FROM xt_media WHERE changed = 1 AND COALESCE(status,0) = 0'),
            'changed_attr_product_ids' => $this->fetchColumn($pdo, 'SELECT DISTINCT products_id FROM xt_plg_products_to_attributes WHERE changed = 1'),
            'changed_media_link_product_ids' => $this->fetchColumn($pdo, 'SELECT DISTINCT link_id FROM xt_media_link WHERE changed = 1'),
            'changed_ptc_product_ids' => $this->fetchColumn($pdo, 'SELECT DISTINCT products_id FROM xt_products_to_categories WHERE changed = 1'),
            'changed_ptc_category_ids' => $this->fetchColumn($pdo, 'SELECT DISTINCT categories_id FROM xt_products_to_categories WHERE changed = 1'),
            'orphan_attr_product_ids' => array_values(array_unique(array_map('strval', $orphans['orphan_attr_product_ids'] ?? []))),
        ];
    }

    /**
     * @return array<string, array<int, string>>
     */
    private function collectOrphanRelationIds(PDO $pdo): array
    {
        return [
            'orphan_attr_product_ids' => $this->fetchColumn(
                $pdo,
                'SELECT DISTINCT x.products_id
                 FROM xt_plg_products_to_attributes x
                 LEFT JOIN xt_products p ON p.products_id = x.products_id
                 WHERE p.products_id IS NULL'
            ),
        ];
    }

    private function repairLocalMediaState(PDO $pdo): void
    {
        if ($this->tableHasColumn($pdo, 'xt_media', 'id') && $this->tableHasColumn($pdo, 'xt_media', 'external_id')) {
            $maxId = (int)($pdo->query("SELECT COALESCE(MAX(CAST(id AS SIGNED)), 0) FROM xt_media WHERE id IS NOT NULL AND CAST(id AS CHAR) <> ''")?->fetchColumn() ?? 0);
            $rows = $pdo->query("SELECT external_id FROM xt_media WHERE (id IS NULL OR CAST(id AS CHAR) = '') AND external_id IS NOT NULL AND CAST(external_id AS CHAR) <> '' ORDER BY external_id ASC")?->fetchAll(PDO::FETCH_COLUMN) ?: [];
            if ($rows !== []) {
                $stmt = $pdo->prepare('UPDATE xt_media SET id = :id, changed = 1 WHERE external_id = :external_id AND (id IS NULL OR CAST(id AS CHAR) = \'\')');
                foreach ($rows as $externalId) {
                    $maxId++;
                    $stmt->execute([
                        ':id' => $maxId,
                        ':external_id' => $externalId,
                    ]);
                }
            }
        }

        if ($this->tableHasColumn($pdo, 'xt_media_link', 'type')) {
            $pdo->exec("UPDATE xt_media_link SET type = 'images', changed = 1 WHERE type IS NULL OR CAST(type AS CHAR) = ''");
        }
        if ($this->tableHasColumn($pdo, 'xt_media_link', 'class')) {
            $pdo->exec("UPDATE xt_media_link SET class = 'product', changed = 1 WHERE class IS NULL OR CAST(class AS CHAR) = ''");
        }
        if (
            $this->tableHasColumn($pdo, 'xt_media_link', 'm_id')
            && $this->tableHasColumn($pdo, 'xt_media_link', 'link_id')
            && $this->tableHasColumn($pdo, 'xt_media_link', 'sort_order')
            && $this->tableHasColumn($pdo, 'xt_products', 'products_id')
            && $this->tableHasColumn($pdo, 'xt_products', 'external_id')
            && $this->tableHasColumn($pdo, 'artikel_media_map', 'afs_artikel_id')
            && $this->tableHasColumn($pdo, 'artikel_media_map', 'position')
            && $this->tableHasColumn($pdo, 'artikel_media_map', 'media_id')
            && $this->tableHasColumn($pdo, 'xt_media', 'id')
            && $this->tableHasColumn($pdo, 'xt_media', 'external_id')
        ) {
            $pdo->exec(
                "UPDATE xt_media_link
                 SET m_id = (
                   SELECT xm.id
                   FROM xt_products xp
                   JOIN artikel_media_map am
                     ON CAST(am.afs_artikel_id AS CHAR) = CAST(xp.external_id AS CHAR)
                    AND CAST(am.position AS CHAR) = CAST(xt_media_link.sort_order AS CHAR)
                   JOIN xt_media xm
                     ON CAST(xm.external_id AS CHAR) = CAST(am.media_id AS CHAR)
                   WHERE CAST(xp.products_id AS CHAR) = CAST(xt_media_link.link_id AS CHAR)
                   LIMIT 1
                 ),
                 changed = 1
                 WHERE m_id IS NULL OR CAST(m_id AS CHAR) = ''"
            );
        }
    }

    /**
     * @param array<string, array<int, string>> $affected
     * @return array<int, array<string, mixed>>
     */
    private function buildDeleteOps(array $affected): array
    {
        $ops = [];
        $productIds = array_values(array_unique(array_merge($affected['deleted_product_ids'], $affected['changed_ptc_product_ids'])));
        $categoryIds = array_values(array_unique(array_merge($affected['deleted_category_ids'], $affected['changed_ptc_category_ids'])));
        $attrProductIds = array_values(array_unique(array_merge(
            $affected['changed_attr_product_ids'],
            $affected['deleted_product_ids'],
            $affected['orphan_attr_product_ids'] ?? []
        )));
        $mediaLinkProductIds = array_values(array_unique(array_merge($affected['changed_media_link_product_ids'], $affected['deleted_product_ids'])));

        if ($productIds !== []) {
            $ops[] = ['table' => 'xt_products_to_categories', 'mode' => 'delete_where_in', 'column' => 'products_id', 'values' => $productIds];
        }
        if ($categoryIds !== []) {
            $ops[] = ['table' => 'xt_products_to_categories', 'mode' => 'delete_where_in', 'column' => 'categories_id', 'values' => $categoryIds];
        }
        if ($attrProductIds !== []) {
            $ops[] = ['table' => 'xt_plg_products_to_attributes', 'mode' => 'delete_where_in', 'column' => 'products_id', 'values' => $attrProductIds];
        }
        if ($mediaLinkProductIds !== []) {
            $ops[] = ['table' => 'xt_media_link', 'mode' => 'delete_where_in', 'column' => 'link_id', 'values' => $mediaLinkProductIds];
        }
        if ($affected['deleted_media_ids'] !== []) {
            $ops[] = ['table' => 'xt_media_link', 'mode' => 'delete_where_in', 'column' => 'm_id', 'values' => $affected['deleted_media_ids']];
        }
        if ($affected['deleted_product_ids'] !== []) {
            $ops[] = ['table' => 'xt_seo_url', 'mode' => 'delete_rows', 'key_columns' => ['link_type', 'link_id'], 'rows' => array_map(static fn(string $id): array => ['link_type' => 1, 'link_id' => $id], $affected['deleted_product_ids'])];
        }
        if ($affected['deleted_category_ids'] !== []) {
            $ops[] = ['table' => 'xt_seo_url', 'mode' => 'delete_rows', 'key_columns' => ['link_type', 'link_id'], 'rows' => array_map(static fn(string $id): array => ['link_type' => 2, 'link_id' => $id], $affected['deleted_category_ids'])];
        }

        return $ops;
    }

    /**
     * @return array<string, int>
     */
    private function cleanupLocalOrphanRelations(PDO $pdo): array
    {
        $stats = [
            'xt_plg_products_to_attributes_orphan_products_deleted' => 0,
            'xt_plg_products_to_attributes_orphan_values_deleted' => 0,
            'xt_plg_products_to_attributes_orphan_parents_deleted' => 0,
        ];

        if ($this->tableHasColumn($pdo, 'xt_plg_products_to_attributes', 'products_id') && $this->tableHasColumn($pdo, 'xt_products', 'products_id')) {
            $stmt = $pdo->prepare(
                'DELETE FROM xt_plg_products_to_attributes
                 WHERE NOT EXISTS (
                   SELECT 1 FROM xt_products p
                   WHERE CAST(p.products_id AS CHAR) = CAST(xt_plg_products_to_attributes.products_id AS CHAR)
                 )'
            );
            $stmt->execute();
            $stats['xt_plg_products_to_attributes_orphan_products_deleted'] = $stmt->rowCount();
        }

        if ($this->tableHasColumn($pdo, 'xt_plg_products_to_attributes', 'attributes_id') && $this->tableHasColumn($pdo, 'xt_plg_products_attributes', 'attributes_id')) {
            $stmt = $pdo->prepare(
                'DELETE FROM xt_plg_products_to_attributes
                 WHERE NOT EXISTS (
                   SELECT 1 FROM xt_plg_products_attributes a
                   WHERE CAST(a.attributes_id AS CHAR) = CAST(xt_plg_products_to_attributes.attributes_id AS CHAR)
                 )'
            );
            $stmt->execute();
            $stats['xt_plg_products_to_attributes_orphan_values_deleted'] = $stmt->rowCount();
        }

        if ($this->tableHasColumn($pdo, 'xt_plg_products_to_attributes', 'attributes_parent_id') && $this->tableHasColumn($pdo, 'xt_plg_products_attributes', 'attributes_id')) {
            $stmt = $pdo->prepare(
                'DELETE FROM xt_plg_products_to_attributes
                 WHERE NOT EXISTS (
                   SELECT 1 FROM xt_plg_products_attributes a
                   WHERE CAST(a.attributes_id AS CHAR) = CAST(xt_plg_products_to_attributes.attributes_parent_id AS CHAR)
                 )'
            );
            $stmt->execute();
            $stats['xt_plg_products_to_attributes_orphan_parents_deleted'] = $stmt->rowCount();
        }

        return $stats;
    }

    /**
     * @param array<string, array<int, string>> $affected
     * @return array<int, array<string, mixed>>
     */
    private function loadRowsForPush(PDO $pdo, string $table, array $affected): array
    {
        if (!$this->tableHasColumn($pdo, $table, 'changed')) {
            return [];
        }

        if ($table === 'xt_products_to_categories') {
            return $this->fetchRowsByIds(
                $pdo,
                $table,
                'products_id',
                array_values(array_unique(array_merge($affected['deleted_product_ids'], $affected['changed_ptc_product_ids']))),
                'categories_id',
                array_values(array_unique(array_merge($affected['deleted_category_ids'], $affected['changed_ptc_category_ids'])))
            );
        }
        if ($table === 'xt_plg_products_to_attributes') {
            return $this->fetchRowsByIds($pdo, $table, 'products_id', array_values(array_unique(array_merge($affected['changed_attr_product_ids'], $affected['deleted_product_ids']))));
        }
        if ($table === 'xt_media_link') {
            $rows = $this->fetchRowsByIds($pdo, $table, 'link_id', array_values(array_unique(array_merge($affected['changed_media_link_product_ids'], $affected['deleted_product_ids']))));
            return array_values(array_filter($rows, static function (array $row): bool {
                $mId = trim((string)($row['m_id'] ?? ''));
                $linkId = trim((string)($row['link_id'] ?? ''));
                $type = trim((string)($row['type'] ?? ''));
                return $mId !== '' && $linkId !== '' && $type !== '';
            }));
        }
        if ($table === 'xt_seo_url') {
            return $this->fetchSeoRows($pdo, $affected);
        }

        $stmt = $pdo->query('SELECT * FROM ' . $this->quoteIdentifier($table) . ' WHERE changed = 1');
        return $stmt ? ($stmt->fetchAll(PDO::FETCH_ASSOC) ?: []) : [];
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function fetchRowsByIds(PDO $pdo, string $table, string $column, array $ids, ?string $extraColumn = null, array $extraIds = []): array
    {
        $ids = array_values(array_filter(array_map('strval', $ids), static fn(string $id): bool => $id !== ''));
        $extraIds = array_values(array_filter(array_map('strval', $extraIds), static fn(string $id): bool => $id !== ''));
        if ($ids === [] && $extraIds === []) {
            return [];
        }
        $where = [];
        $params = [];
        if ($ids !== []) {
            $placeholders = implode(',', array_fill(0, count($ids), '?'));
            $where[] = $this->quoteIdentifier($column) . ' IN (' . $placeholders . ')';
            $params = array_merge($params, $ids);
        }
        if ($extraColumn !== null && $extraIds !== []) {
            $placeholders = implode(',', array_fill(0, count($extraIds), '?'));
            $where[] = $this->quoteIdentifier($extraColumn) . ' IN (' . $placeholders . ')';
            $params = array_merge($params, $extraIds);
        }
        $sql = 'SELECT * FROM ' . $this->quoteIdentifier($table) . ' WHERE ' . implode(' OR ', $where);
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
        return $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
    }

    /**
     * @param array<string, array<int, string>> $affected
     * @return array<int, array<string, mixed>>
     */
    private function fetchSeoRows(PDO $pdo, array $affected): array
    {
        $rows = [];
        $productIds = array_values(array_unique($affected['changed_product_ids']));
        $categoryIds = array_values(array_unique($affected['changed_category_ids']));
        if ($productIds !== []) {
            $placeholders = implode(',', array_fill(0, count($productIds), '?'));
            $stmt = $pdo->prepare('SELECT * FROM xt_seo_url WHERE link_type = 1 AND link_id IN (' . $placeholders . ')');
            $stmt->execute($productIds);
            $rows = array_merge($rows, $stmt->fetchAll(PDO::FETCH_ASSOC) ?: []);
        }
        if ($categoryIds !== []) {
            $placeholders = implode(',', array_fill(0, count($categoryIds), '?'));
            $stmt = $pdo->prepare('SELECT * FROM xt_seo_url WHERE link_type = 2 AND link_id IN (' . $placeholders . ')');
            $stmt->execute($categoryIds);
            $rows = array_merge($rows, $stmt->fetchAll(PDO::FETCH_ASSOC) ?: []);
        }
        if ($rows === []) {
            return [];
        }

        $existing = $this->loadRemoteSeoKeys();
        if ($existing === []) {
            return $rows;
        }

        $filtered = [];
        foreach ($rows as $row) {
            $key = $this->seoKey($row);
            if ($key === null) {
                continue;
            }
            if (isset($existing[$key])) {
                continue;
            }
            $filtered[] = $row;
        }
        return $filtered;
    }

    /**
     * @return array<string, bool>
     */
    private function loadRemoteSeoKeys(): array
    {
        if ($this->remoteSeoKeys !== null) {
            return $this->remoteSeoKeys;
        }

        $keys = [];
        $page = 1;
        do {
            $data = $this->request('GET', '/export/table/' . rawurlencode('xt_seo_url') . '?page=' . $page . '&page_size=1000');
            if (!($data['ok'] ?? false)) {
                break;
            }
            $rows = $data['rows'] ?? [];
            if (!is_array($rows)) {
                break;
            }
            foreach ($rows as $row) {
                if (!is_array($row)) {
                    continue;
                }
                $key = $this->seoKey($row);
                if ($key !== null) {
                    $keys[$key] = true;
                }
            }
            $hasMore = (bool)($data['has_more'] ?? false);
            $page++;
        } while ($hasMore);

        $this->remoteSeoKeys = $keys;
        return $keys;
    }

    private function seoKey(array $row): ?string
    {
        $linkType = trim((string)($row['link_type'] ?? ''));
        $linkId = trim((string)($row['link_id'] ?? ''));
        $languageCode = trim((string)($row['language_code'] ?? ''));
        $storeId = trim((string)($row['store_id'] ?? ''));
        if ($linkType === '' || $linkId === '' || $languageCode === '' || $storeId === '') {
            return null;
        }
        return $linkType . '|' . $linkId . '|' . $languageCode . '|' . $storeId;
    }

    /**
     * @return array<string, mixed>
     */
    private function fetchTableSchema(string $table): array
    {
        if (isset($this->schemaCache[$table])) {
            return $this->schemaCache[$table];
        }
        $data = $this->request('GET', '/schema/table/' . rawurlencode($table));
        if (!($data['ok'] ?? false)) {
            throw new RuntimeException('Schema für XT-Tabelle nicht verfügbar: ' . $table);
        }
        $this->schemaCache[$table] = $data;
        return $data;
    }

    /**
     * @param array<string, mixed> $schema
     * @return array<int, string>
     */
    private function resolveKeyColumns(array $schema): array
    {
        $columns = $schema['columns'] ?? [];
        if (!is_array($columns)) {
            return [];
        }
        $pri = [];
        $uni = [];
        foreach ($columns as $column) {
            if (!is_array($column)) {
                continue;
            }
            $name = (string)($column['name'] ?? '');
            $key = (string)($column['key'] ?? '');
            if ($name === '') {
                continue;
            }
            if ($key === 'PRI') {
                $pri[] = $name;
            } elseif ($key === 'UNI') {
                $uni[] = $name;
            }
        }
        return $pri !== [] ? $pri : $uni;
    }

    /**
     * @return array<int, string>
     */
    private function fetchColumn(PDO $pdo, string $sql): array
    {
        $stmt = $pdo->query($sql);
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_NUM) : [];
        $values = [];
        foreach ($rows as $row) {
            if (!isset($row[0])) {
                continue;
            }
            $value = trim((string)$row[0]);
            if ($value !== '') {
                $values[] = $value;
            }
        }
        return $values;
    }

    private function tableHasColumn(PDO $pdo, string $table, string $column): bool
    {
        $stmt = $this->isMysql($pdo)
            ? $pdo->query('DESCRIBE ' . $this->quoteIdentifier($table))
            : $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        foreach ($rows as $row) {
            if (strcasecmp((string)($row['name'] ?? $row['Field'] ?? ''), $column) === 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param array<string, mixed>|null $body
     * @return array<string, mixed>
     */
    private function request(string $method, string $path, ?array $body = null): array
    {
        $base = trim((string)env('XT_API_BASE_URL', (string)env('XT_API_BASE', '')));
        if ($base === '') {
            throw new RuntimeException('XT_API_BASE_URL fehlt.');
        }
        $key = (string)env('XT_API_KEY', '');
        if ($key === '') {
            throw new RuntimeException('XT_API_KEY fehlt.');
        }

        $url = rtrim($base, "/\\") . $path;
        $payload = $body ? (json_encode($body, JSON_UNESCAPED_UNICODE) ?: '') : '';
        $ts = (string)time();
        $signPath = parse_url($url, PHP_URL_PATH) ?: $path;
        $baseString = $method . "\n" . $signPath . "\n" . $ts . "\n" . $payload;
        $sig = hash_hmac('sha256', $baseString, $key);

        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_TIMEOUT, 120);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
        $headers = [
            'X-API-KEY: default',
            'X-API-TS: ' . $ts,
            'X-API-SIG: ' . $sig,
            'Accept: application/json',
        ];
        if ($payload !== '') {
            curl_setopt($ch, CURLOPT_POSTFIELDS, $payload);
            $headers[] = 'Content-Type: application/json';
        }
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        $resp = curl_exec($ch);
        $code = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $err = curl_error($ch);
        curl_close($ch);

        if ($resp === false) {
            throw new RuntimeException('XT-API unreachable: ' . $err);
        }
        if ($code >= 400) {
            throw new RuntimeException('XT-API http error: ' . $code . ' body=' . substr((string)$resp, 0, 300));
        }
        $json = json_decode((string)$resp, true);
        if (!is_array($json)) {
            throw new RuntimeException('XT-API response ungültig.');
        }
        return $json;
    }

    /**
     * @return array<string, mixed>
     */
    private function uploadChangedFiles(PDO $pdo): array
    {
        if (
            !$this->tableHasColumn($pdo, 'media', 'changed') ||
            !$this->tableHasColumn($pdo, 'media', 'filename') ||
            !$this->tableHasColumn($pdo, 'media', 'storage_path') ||
            !$this->tableHasColumn($pdo, 'media', 'checksum')
        ) {
            return ['ok' => true, 'received' => 0, 'written' => 0, 'skipped' => 0, 'errors' => []];
        }

        $rows = $pdo->query(
            "SELECT filename, type, storage_path, checksum
             FROM media
             WHERE changed = 1
               AND COALESCE(is_deleted, 0) = 0
               AND storage_path IS NOT NULL
               AND CAST(storage_path AS CHAR) <> ''
               AND checksum IS NOT NULL
               AND checksum <> ''
               AND checksum <> 'notFound'"
        )?->fetchAll(PDO::FETCH_ASSOC) ?: [];

        if ($rows === []) {
            return ['ok' => true, 'received' => 0, 'written' => 0, 'skipped' => 0, 'errors' => [], 'written_paths' => [], 'skipped_paths' => []];
        }

        $batches = [];
        $current = [];
        $currentBytes = 0;
        foreach ($rows as $row) {
            $path = (string)($row['storage_path'] ?? '');
            $filename = basename((string)($row['filename'] ?? ''));
            if ($path === '' || $filename === '' || !is_file($path)) {
                continue;
            }
            $size = (int)@filesize($path);
            if ($size <= 0) {
                continue;
            }
            if ($current !== [] && (count($current) >= 10 || ($currentBytes + $size) > 4_000_000)) {
                $batches[] = $current;
                $current = [];
                $currentBytes = 0;
            }
            $current[] = [
                'kind' => strtolower((string)($row['type'] ?? 'image')) === 'dokument' ? 'documents' : 'images',
                'filename' => $filename,
                'checksum' => (string)$row['checksum'],
                'storage_path' => $path,
            ];
            $currentBytes += $size;
        }
        if ($current !== []) {
            $batches[] = $current;
        }

        $stats = ['ok' => true, 'received' => 0, 'written' => 0, 'skipped' => 0, 'errors' => [], 'written_paths' => [], 'skipped_paths' => []];
        foreach ($batches as $batch) {
            $files = [];
            foreach ($batch as $file) {
                $content = @file_get_contents((string)$file['storage_path']);
                if ($content === false) {
                    $stats['errors'][] = 'read_failed:' . $file['filename'];
                    continue;
                }
                $files[] = [
                    'kind' => $file['kind'],
                    'filename' => $file['filename'],
                    'checksum' => $file['checksum'],
                    'content_base64' => base64_encode($content),
                ];
            }
            if ($files === []) {
                continue;
            }
            $result = $this->request('POST', '/upload-files', ['files' => $files]);
            $stats['received'] += (int)($result['received'] ?? count($files));
            $stats['written'] += (int)($result['written'] ?? 0);
            $stats['skipped'] += (int)($result['skipped'] ?? 0);
            foreach (($result['errors'] ?? []) as $error) {
                $stats['errors'][] = (string)$error;
            }
            foreach (($result['written_paths'] ?? []) as $path) {
                $stats['written_paths'][] = (string)$path;
            }
            foreach (($result['skipped_paths'] ?? []) as $path) {
                $stats['skipped_paths'][] = (string)$path;
            }
        }
        $stats['ok'] = $stats['errors'] === [];
        return $stats;
    }

    /**
     * @return array<string, mixed>
     */
    private function loadMapping(string $name): array
    {
        $path = __DIR__ . '/../../Config/mappings/' . $name . '.php';
        if (!is_file($path)) {
            throw new RuntimeException('Mapping nicht gefunden: ' . $name);
        }
        $mapping = require $path;
        if (!is_array($mapping)) {
            throw new RuntimeException('Mapping ungültig: ' . $name);
        }
        return $mapping;
    }

    private function quoteIdentifier(string $name): string
    {
        return '`' . str_replace('`', '``', $name) . '`';
    }

    private function isMysql(PDO $pdo): bool
    {
        return (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'mysql';
    }

    private function remoteTableName(string $table): string
    {
        return $table;
    }
}
