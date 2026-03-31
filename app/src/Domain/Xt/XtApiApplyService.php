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

    public function __construct(private ConnectionFactory $factory) {}

    /**
     * @return array<string, mixed>
     */
    public function run(string $mappingName = 'welafix_xt'): array
    {
        $pdo = Db::guardSqlite($this->factory->sqlite(), __METHOD__);
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
            'local_reset' => [],
        ];

        $affected = $this->collectAffectedIds($pdo);
        foreach ($this->buildDeleteOps($affected) as $op) {
            $result = $this->request('POST', '/apply', $op);
            $stats['delete_ops'][] = $result;
        }

        foreach ($tableOrder as $table) {
            $rows = $this->loadRowsForPush($pdo, $table, $affected);
            if ($rows === []) {
                $stats['upserts'][$table] = 0;
                continue;
            }
            $schema = $this->fetchTableSchema($table);
            $keyColumns = $this->resolveKeyColumns($schema);
            if ($keyColumns === []) {
                throw new RuntimeException('Keine Key-Spalten für XT-Tabelle gefunden: ' . $table);
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
            $result = $this->request('POST', '/apply', [
                'table' => $table,
                'mode' => 'upsert',
                'key_columns' => $keyColumns,
                'rows' => $filtered,
            ]);
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

        return $stats;
    }

    /**
     * @return array<string, array<int, string>>
     */
    private function collectAffectedIds(PDO $pdo): array
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
        ];
    }

    /**
     * @param array<string, array<int, string>> $affected
     * @return array<int, array<string, mixed>>
     */
    private function buildDeleteOps(array $affected): array
    {
        $ops = [];
        $productIds = array_values(array_unique(array_merge($affected['changed_product_ids'], $affected['deleted_product_ids'])));
        $categoryIds = array_values(array_unique(array_merge($affected['changed_category_ids'], $affected['deleted_category_ids'])));
        $attrProductIds = array_values(array_unique(array_merge($affected['changed_attr_product_ids'], $affected['deleted_product_ids'])));
        $mediaLinkProductIds = array_values(array_unique(array_merge($affected['changed_media_link_product_ids'], $affected['deleted_product_ids'])));

        if ($productIds !== []) {
            $ops[] = ['table' => 'products_to_categories', 'mode' => 'delete_where_in', 'column' => 'products_id', 'values' => $productIds];
            $ops[] = ['table' => 'xt_seo_url', 'mode' => 'delete_rows', 'key_columns' => ['link_type', 'link_id'], 'rows' => array_map(static fn(string $id): array => ['link_type' => 1, 'link_id' => $id], $productIds)];
        }
        if ($categoryIds !== []) {
            $ops[] = ['table' => 'products_to_categories', 'mode' => 'delete_where_in', 'column' => 'categories_id', 'values' => $categoryIds];
            $ops[] = ['table' => 'xt_seo_url', 'mode' => 'delete_rows', 'key_columns' => ['link_type', 'link_id'], 'rows' => array_map(static fn(string $id): array => ['link_type' => 2, 'link_id' => $id], $categoryIds)];
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

        return $ops;
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

        if ($table === 'products_to_categories') {
            return $this->fetchRowsByIds($pdo, $table, 'products_id', array_values(array_unique(array_merge($affected['changed_product_ids'], $affected['changed_category_ids']))), 'categories_id', $affected['changed_category_ids']);
        }
        if ($table === 'xt_plg_products_to_attributes') {
            return $this->fetchRowsByIds($pdo, $table, 'products_id', array_values(array_unique(array_merge($affected['changed_attr_product_ids'], $affected['deleted_product_ids']))));
        }
        if ($table === 'xt_media_link') {
            return $this->fetchRowsByIds($pdo, $table, 'link_id', array_values(array_unique(array_merge($affected['changed_media_link_product_ids'], $affected['deleted_product_ids']))));
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
        return $rows;
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
        $stmt = $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        foreach ($rows as $row) {
            if (strcasecmp((string)($row['name'] ?? ''), $column) === 0) {
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
        return '"' . str_replace('"', '""', $name) . '"';
    }
}
