<?php
declare(strict_types=1);

namespace Welafix\Domain\Xt;

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use RuntimeException;
use Welafix\Database\Db;
use Welafix\Database\ConnectionFactory;
use Welafix\Domain\Xt\XtDeletedSyncService;

final class XtMappingSyncService
{
    /** @var array<string, array<string, int>> */
    private array $targetMaps = [];
    /** @var array<string, bool> */
    private array $targetMapsLoaded = [];
    /** @var array<string, array{table:string, pk:array<int,string>, ext_entity:?string, ext_field:?string}> */
    private array $targetMetas = [];
    /** @var array<string, array<string, array<string, mixed>>> */
    private array $targetRowCache = [];
    /** @var array<string, array<string, bool>> */
    private array $loggedMissing = [];
    /** @var array<string, array<string, string>> */
    private array $seoUrlOwners = [];
    /** @var array<string, string> */
    private array $seoOwnUrls = [];
    private bool $seoCacheLoaded = false;
    private ?PDO $pdo = null;
    private bool $debugEnabled = false;
    /** @var array<string, int> */
    private array $debugDiffCount = [];

    /**
     * @return array<string, mixed>
     */
    public function run(string $mappingName = 'welafix_xt'): array
    {
        $mapping = $this->loadMapping($mappingName);
        $targets = $mapping['targets'] ?? [];
        if (!is_array($targets) || $targets === []) {
            throw new RuntimeException('Keine Targets im Mapping.');
        }
        $this->targetMetas = $this->buildTargetMetas($targets);

        $pdo = (new ConnectionFactory())->localDb();
        $this->pdo = $pdo;
        $readPdo = (new ConnectionFactory())->localDb();
        $deleteSync = new XtDeletedSyncService(new ConnectionFactory());
        $batchSize = $this->loadBatchSize($pdo);
        $debugMeta = $this->loadDebugEnabled($pdo);
        $debugEnabled = $debugMeta['enabled'];
        $this->debugEnabled = $debugEnabled;

        $stats = [
            'ok' => true,
            'targets' => [],
            'nested_set_rebuild' => 0,
            'prepare' => $deleteSync->prepareDelta(),
            'batch_size' => $batchSize,
            'debug' => $debugEnabled,
            'debug_setting' => $debugMeta['setting'],
            'debug_env' => $debugMeta['env'],
            'target_keys' => array_keys($targets),
        ];

        // ensure schema for all targets first
        foreach ($targets as $targetName => $target) {
            if (!is_array($target)) {
                continue;
            }
            $table = (string)($target['table'] ?? $targetName);
            $primaryKey = $target['primary_key'] ?? null;
            $columns = $target['columns'] ?? [];
            if ($table === '' || !is_array($columns) || $columns === []) {
                continue;
            }
            $this->ensureChangedColumn($pdo, $table);
            $existingCols = $this->getExistingColumns($pdo, $table);
            if ($existingCols === []) {
                $this->createTable($pdo, $table, $columns, $primaryKey);
            } else {
                $this->ensureColumns($pdo, $table, $columns, $existingCols);
            }
        }

        // pre-load target maps for stable FK resolution (mapping-driven)
        foreach ($this->targetMetas as $meta) {
            if ($meta['ext_entity'] && $meta['ext_field'] && $meta['pk'] !== []) {
                $this->loadTargetMapFromDb($pdo, $meta['table'], 'external_id', $meta['pk'][0]);
            }
        }
        $stats['map_sizes'] = [];
        foreach ($this->targetMaps as $table => $map) {
            $stats['map_sizes'][$table] = count($map);
        }

        foreach ($targets as $targetName => $target) {
            if (!is_array($target)) {
                continue;
            }
            $table = (string)($target['table'] ?? $targetName);
            $primaryKey = $target['primary_key'] ?? null;
            $columns = $target['columns'] ?? [];
            if ($table === '' || !is_array($columns) || $columns === []) {
                continue;
            }
            $existingCols = $this->getExistingColumns($pdo, $table);
            $base = $this->resolveBaseSource($columns, $target);
            if ($base === '') {
                $stats['targets'][$table] = ['skipped' => true, 'reason' => 'no_base_source'];
                continue;
            }

            $lastRowId = 0;
            $totals = [
                'inserted' => 0,
                'updated' => 0,
                'unchanged' => 0,
                'needs_nested_set' => false,
                'source_rows' => 0,
                'base' => $base,
            ];
            while (true) {
                $sourceRows = $this->fetchSourceRowsBatch($readPdo, $base, $table, $batchSize, $lastRowId);
                if ($sourceRows === []) {
                    break;
                }
                $totals['source_rows'] += count($sourceRows);
                $uniqueKey = $target['unique_key'] ?? null;
                $result = $this->applyTarget($pdo, $table, $columns, $primaryKey, $existingCols, $base, $sourceRows, true, $uniqueKey);
                $totals['inserted'] += (int)($result['inserted'] ?? 0);
                $totals['updated'] += (int)($result['updated'] ?? 0);
                $totals['unchanged'] += (int)($result['unchanged'] ?? 0);
                if (!empty($result['needs_nested_set'])) {
                    $totals['needs_nested_set'] = true;
                }
                $lastRow = end($sourceRows);
                $lastRowId = (int)($lastRow['__cursor'] ?? $lastRowId);
            }

            $stats['targets'][$targetName] = $totals + ['table' => $table];

            if ($table === 'xt_categories' && (!empty($totals['needs_nested_set']) || $totals['source_rows'] > 0 || $this->xtCategoriesNeedNestedSetRepair($pdo))) {
                $this->repairXtCategoriesDefaults($pdo);
                $rebuilt = $this->rebuildNestedSet($pdo);
                $stats['nested_set_rebuild'] = $rebuilt;
            }
        }

        if ($debugEnabled) {
            $path = $this->getLogsDir() . '/xt_mapping_debug.json';
            $dir = dirname($path);
            if (!is_dir($dir)) {
                @mkdir($dir, 0777, true);
            }
            $ok = @file_put_contents($path, json_encode($stats, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
            $stats['debug_path'] = $path;
            $stats['debug_written'] = $ok !== false;
            $diffPath = $this->getLogsDir() . '/xt_mapping_debug_diff.log';
            $stats['debug_diff_path'] = $diffPath;
            if (is_file($diffPath)) {
                $lines = @file($diffPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [];
                $stats['debug_diff_tail'] = array_slice($lines, -20);
            }
            $stats['warnings_path'] = $this->getLogsDir() . '/xt_mapping_warnings.log';
        }
        $stats['deleted_cleanup'] = $deleteSync->cleanupDeleted();
        $this->repairXtCategoriesDefaults($pdo);
        return $stats;
    }

    /**
     * @param array<string, string> $columns
     * @param array<string, true> $existingCols
     * @param array<int, array<string, mixed>> $sourceRows
     * @return array<string, mixed>
     */
    private function applyTarget(PDO $pdo, string $table, array $columns, mixed $primaryKey, array $existingCols, string $base, iterable $sourceRows, bool $useTransaction = false, mixed $uniqueKey = null): array
    {
        $inserted = 0;
        $updated = 0;
        $unchanged = 0;
        $needsNestedSet = false;

        $pkCols = $this->normalizePk($primaryKey);
        $uniqueCols = $this->normalizePk($uniqueKey);
        $compareCols = $this->getCompareColumns($columns, $existingCols);
        if ($pkCols !== []) {
            $this->ensurePkIndex($pdo, $table, $pkCols);
        }
        $upsert = $this->buildUpsertStatement($pdo, $table, $columns, $existingCols, $pkCols, $compareCols);
        $upsertCols = $this->getUpsertColumns($table, $columns, $existingCols);
        $selectByPk = $pkCols !== [] ? $this->buildSelectByPk($pdo, $table, $pkCols, $compareCols) : null;
        $selectByUnique = $uniqueCols !== [] ? $this->buildSelectByPk($pdo, $table, $uniqueCols, $compareCols) : null;
        $selectByExternal = $this->buildSelectByExternal($pdo, $table, $compareCols, $existingCols);
        $updateStmt = $pkCols !== [] ? $this->buildUpdateStatement($pdo, $table, $columns, $existingCols, $pkCols) : null;
        $updateByUnique = $uniqueCols !== [] ? $this->buildUpdateStatement($pdo, $table, $columns, $existingCols, $uniqueCols) : null;
        $updateCols = $pkCols !== [] ? $this->getUpdateColumns($table, $columns, $existingCols, $pkCols) : [];
        $updateByUniqueCols = $uniqueCols !== [] ? $this->getUpdateColumns($table, $columns, $existingCols, $uniqueCols) : [];

        if ($useTransaction) {
            $pdo->beginTransaction();
        }
        try {
            foreach ($sourceRows as $sourceRow) {
                $context = [$base => $sourceRow];
                $values = [];
                $autoCols = [];
                foreach ($columns as $col => $expr) {
                    if (!isset($existingCols[strtolower($col)])) {
                        $this->warn($table, $this->pkJson($primaryKey, $values), (string)$expr, 'missing_column');
                        continue;
                    }
                    $exprValue = $this->evalExpr((string)$expr, $context, $table, $primaryKey, $values);
                    if ($exprValue === '__AUTO__') {
                        $autoCols[$col] = true;
                        $values[$col] = null;
                    } else {
                        $values[$col] = $exprValue;
                    }
                }
                if ($table === 'xt_seo_url') {
                    $values = $this->ensureUniqueSeoUrlValues($pdo, $values);
                }

                $pkValues = $this->buildPkValues($pkCols, $values);
                $uniqueValues = $this->buildPkValues($uniqueCols, $values);
                if ($pkCols !== [] && !$this->hasUsablePrimaryKey($pkCols, $pkValues, $autoCols) && !isset($values['external_id']) && $uniqueCols === []) {
                    $unchanged++;
                    continue;
                }

                $hasPk = $pkCols !== [] && $this->hasNonAutoPk($pkValues);
                $hasUnique = $uniqueCols !== [] && $this->hasNonAutoPk($uniqueValues);
                // If we can compare, prefer select+diff to avoid pointless updates.
                if ($hasPk && $upsert && $compareCols === []) {
                    $this->bindValues($upsert, $values, $columns, $existingCols, $pkCols, $autoCols, $upsertCols);
                    $upsert->execute();
                    $count = $upsert->rowCount();
                    if ($count > 0) {
                        $updated++;
                        if ($table === 'xt_categories') {
                            $needsNestedSet = true;
                        }
                    } else {
                        $unchanged++;
                    }
                    $this->updateTargetMap($table, $values, $pkCols, $pdo);
                    continue;
                }

                // fallback: external_id or no PK
                $existing = null;
                $lookupPk = $pkValues;
                if ($hasPk && $selectByPk) {
                    $this->bindPk($selectByPk, $pkCols, $pkValues);
                    $selectByPk->execute();
                    $existing = $selectByPk->fetch(PDO::FETCH_ASSOC) ?: null;
                }
                if ($existing === null && isset($values['external_id']) && $selectByExternal) {
                    $selectByExternal->execute([':external_id' => $values['external_id']]);
                    $existing = $selectByExternal->fetch(PDO::FETCH_ASSOC) ?: null;
                    if ($existing) {
                        $lookupPk = $this->buildPkValues($pkCols, $existing);
                    }
                }
                if ($existing === null && $hasUnique && $selectByUnique) {
                    $this->bindPk($selectByUnique, $uniqueCols, $uniqueValues);
                    $selectByUnique->execute();
                    $existing = $selectByUnique->fetch(PDO::FETCH_ASSOC) ?: null;
                    if ($existing) {
                        $lookupPk = $this->buildPkValues($pkCols, $existing);
                    }
                }

                if ($existing === null) {
                    if ($uniqueCols !== [] && !$hasUnique) {
                        $unchanged++;
                        continue;
                    }
                    if ($existing === null && $hasUnique && $updateByUnique) {
                        try {
                            $this->bindValues($updateByUnique, $values, $columns, $existingCols, $uniqueCols, $autoCols, $updateByUniqueCols);
                            $this->bindPk($updateByUnique, $uniqueCols, $uniqueValues);
                            $updateByUnique->execute();
                        } catch (\Throwable $e) {
                            $this->logSqlError($table, $updateByUnique->queryString ?? 'updateByUnique', [
                                'values' => $values,
                                'unique' => $uniqueValues,
                            ], $e);
                            if ($table === 'xt_seo_url') {
                                if (str_contains($e->getMessage(), 'url_md5')) {
                                    $this->logUniqueConflict($table, ['url_md5'], $values, $pdo);
                                    $this->logWarning('WARNING table=' . $table . ' reason=unique_update_by_unique_failed error=' . $e->getMessage());
                                } else {
                                    $this->logWarning('WARNING table=' . $table . ' reason=update_by_unique_failed error=' . $e->getMessage());
                                }
                                $updateByUnique = $this->buildUpdateStatement($pdo, $table, $columns, $existingCols, $uniqueCols);
                                $unchanged++;
                                continue;
                            }
                            throw $e;
                        }
                        if ($updateByUnique->rowCount() > 0) {
                            $updated++;
                            if ($selectByUnique) {
                                $this->bindPk($selectByUnique, $uniqueCols, $uniqueValues);
                                $selectByUnique->execute();
                                $existing = $selectByUnique->fetch(PDO::FETCH_ASSOC) ?: null;
                                if ($existing) {
                                    $lookupPk = $this->buildPkValues($pkCols, $existing);
                                }
                            }
                            if ($table === 'xt_seo_url') {
                                $this->rememberSeoUrlValues($values);
                            }
                            $this->updateTargetMap($table, $values, $pkCols, $pdo);
                            continue;
                        }
                    }
                    try {
                        if ($uniqueCols !== []) {
                            $insertedOk = $this->insertRowIgnore($pdo, $table, $values, $existingCols, $pkCols, $autoCols);
                            if (!$insertedOk && $table === 'xt_seo_url') {
                                $this->logUniqueConflict($table, $uniqueCols, $values, $pdo);
                            }
                        } else {
                            $this->insertRow($pdo, $table, $values, $existingCols, $pkCols, $autoCols);
                        }
                        if (!isset($insertedOk) || $insertedOk) {
                            $inserted++;
                        } else {
                            $unchanged++;
                            continue;
                        }
                    } catch (\Throwable $e) {
                        if ($uniqueCols !== []) {
                            $this->logWarning('WARNING table=' . $table . ' reason=unique_insert_failed error=' . $e->getMessage());
                            $unchanged++;
                            continue;
                        }
                        throw $e;
                    }
                    if ($table === 'xt_categories') {
                        $needsNestedSet = true;
                    }
                    if ($table === 'xt_seo_url') {
                        $this->rememberSeoUrlValues($values);
                    }
                    $this->updateTargetMap($table, $values, $pkCols, $pdo);
                    continue;
                }

                $diff = $this->diffValues($existing, $values, $compareCols);
                if ($diff) {
                    if ($table === 'xt_seo_url') {
                        $this->logSeoDiff($primaryKey, $existing, $values);
                    }
                    $this->debugDiff($table, $primaryKey, $values, $existing, $compareCols);
                    try {
                        if ($updateStmt) {
                            $this->bindValues($updateStmt, $values, $columns, $existingCols, $pkCols, $autoCols, $updateCols);
                            $this->bindPk($updateStmt, $pkCols, $lookupPk);
                            $updateStmt->execute();
                        } else {
                            $this->updateRow($pdo, $table, $values, $existingCols, $pkCols, $lookupPk, $autoCols, $columns);
                        }
                    } catch (\Throwable $e) {
                        $this->logSqlError($table, $updateStmt?->queryString ?? 'updateRow', [
                            'values' => $values,
                            'pk' => $lookupPk,
                        ], $e);
                        if ($table === 'xt_seo_url') {
                            if (str_contains($e->getMessage(), 'url_md5')) {
                                $this->logUniqueConflict($table, ['url_md5'], $values, $pdo);
                                $this->logWarning('WARNING table=' . $table . ' reason=unique_update_failed error=' . $e->getMessage());
                            } else {
                                $this->logWarning('WARNING table=' . $table . ' reason=update_failed error=' . $e->getMessage());
                            }
                            $updateStmt = $this->buildUpdateStatement($pdo, $table, $columns, $existingCols, $pkCols);
                            $unchanged++;
                            continue;
                        }
                        throw $e;
                    }
                    $updated++;
                    if ($table === 'xt_categories') {
                        $needsNestedSet = true;
                    }
                    if ($table === 'xt_seo_url') {
                        $this->rememberSeoUrlValues($values);
                    }
                    $this->updateTargetMap($table, $values, $pkCols, $pdo);
            } else {
                $unchanged++;
                // keep target maps warm even if nothing changed
                $this->updateTargetMap($table, $values, $pkCols, $pdo);
            }
        }
            if ($useTransaction) {
                $pdo->commit();
            }
        } catch (\Throwable $e) {
            if ($useTransaction && $pdo->inTransaction()) {
                $pdo->rollBack();
            }
            throw $e;
        }

        return [
            'inserted' => $inserted,
            'updated' => $updated,
            'unchanged' => $unchanged,
            'needs_nested_set' => $needsNestedSet,
        ];
    }

    private function ensureChangedColumn(PDO $pdo, string $table): void
    {
        $info = $this->getExistingColumns($pdo, $table);
        if ($info === []) {
            return;
        }
        if (!isset($info['changed'])) {
            $pdo->exec('ALTER TABLE ' . $this->quoteIdentifier($table) . ' ADD COLUMN changed ' . ($this->isMysql($pdo) ? 'TINYINT NOT NULL DEFAULT 0' : 'INTEGER NOT NULL DEFAULT 0'));
        }
        $this->createIndexIfMissing($pdo, 'idx_' . $table . '_changed', $table, ['changed']);
        if ($table === 'xt_seo_url') {
            try {
                // log duplicate url_md5 (do not enforce UNIQUE index automatically)
                $dupStmt = $pdo->query('SELECT url_md5, COUNT(*) AS c FROM xt_seo_url GROUP BY url_md5 HAVING c > 1');
                if ($dupStmt) {
                    $dups = $dupStmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
                    if ($dups) {
                        $path = $this->getLogsDir() . '/xt_seo_dups.log';
                        foreach ($dups as $dup) {
                            $md5 = (string)($dup['url_md5'] ?? '');
                            $cnt = (int)($dup['c'] ?? 0);
                            $rows = $pdo->query('SELECT url_text, link_id, link_type, language_code, store_id FROM xt_seo_url WHERE url_md5 = ' . $pdo->quote($md5))?->fetchAll(PDO::FETCH_ASSOC) ?: [];
                            $entry = [
                                'ts' => (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM),
                                'url_md5' => $md5,
                                'count' => $cnt,
                                'rows' => $rows,
                            ];
                            @file_put_contents($path, json_encode($entry, JSON_UNESCAPED_UNICODE) . PHP_EOL, FILE_APPEND);
                        }
                    }
                }
            } catch (\Throwable $e) {
                $this->logWarning('WARNING table=xt_seo_url reason=md5_unique_failed error=' . $e->getMessage());
            }
        }
    }

    /**
     * @param array<string, mixed> $values
     * @return array<string, mixed>
     */
    private function ensureUniqueSeoUrlValues(PDO $pdo, array $values): array
    {
        $this->ensureSeoCacheLoaded($pdo);
        $base = trim((string)($values['url_text'] ?? ''));
        $languageCode = trim((string)($values['language_code'] ?? ''));
        $storeId = trim((string)($values['store_id'] ?? ''));
        $linkType = trim((string)($values['link_type'] ?? ''));
        $linkId = trim((string)($values['link_id'] ?? ''));
        if ($base === '' || $languageCode === '' || $storeId === '' || $linkType === '' || $linkId === '') {
            return $values;
        }

        $ownExisting = $this->findOwnSeoUrl($pdo, $linkType, $linkId, $languageCode, $storeId);
        if ($ownExisting !== null) {
            $ownTrimmed = trim($ownExisting);
            if ($ownTrimmed !== '' && ($ownTrimmed === $base || preg_match('/^' . preg_quote($base, '/') . '-\d+$/', $ownTrimmed))) {
                if (!$this->seoUrlExistsForOther($pdo, $ownTrimmed, $languageCode, $storeId, $linkType, $linkId)) {
                    $values['url_text'] = $ownTrimmed;
                    $values['url_md5'] = md5($ownTrimmed);
                    return $values;
                }
            }
        }

        $candidate = $base;
        $suffix = 0;
        while ($this->seoUrlExistsForOther($pdo, $candidate, $languageCode, $storeId, $linkType, $linkId)) {
            $suffix++;
            $candidate = $base . '-' . $suffix;
        }

        $values['url_text'] = $candidate;
        $values['url_md5'] = md5($candidate);
        return $values;
    }

    private function findOwnSeoUrl(PDO $pdo, string $linkType, string $linkId, string $languageCode, string $storeId): ?string
    {
        $ownerKey = $this->seoOwnerKey($linkType, $linkId, $languageCode, $storeId);
        return $this->seoOwnUrls[$ownerKey] ?? null;
    }

    private function seoUrlExistsForOther(PDO $pdo, string $urlText, string $languageCode, string $storeId, string $linkType, string $linkId): bool
    {
        $scopeKey = $this->seoScopeKey($languageCode, $storeId);
        $ownerKey = $this->seoOwnerKey($linkType, $linkId, $languageCode, $storeId);
        $existingOwner = $this->seoUrlOwners[$scopeKey][$urlText] ?? null;
        return $existingOwner !== null && $existingOwner !== $ownerKey;
    }

    private function ensureSeoCacheLoaded(PDO $pdo): void
    {
        if ($this->seoCacheLoaded || !$this->tableExists($pdo, 'xt_seo_url')) {
            $this->seoCacheLoaded = true;
            return;
        }
        $stmt = $pdo->query('SELECT url_text, language_code, store_id, link_type, link_id FROM xt_seo_url');
        $rows = $stmt ? ($stmt->fetchAll(PDO::FETCH_ASSOC) ?: []) : [];
        foreach ($rows as $row) {
            $urlText = trim((string)($row['url_text'] ?? ''));
            $languageCode = trim((string)($row['language_code'] ?? ''));
            $storeId = trim((string)($row['store_id'] ?? ''));
            $linkType = trim((string)($row['link_type'] ?? ''));
            $linkId = trim((string)($row['link_id'] ?? ''));
            if ($urlText === '' || $languageCode === '' || $storeId === '' || $linkType === '' || $linkId === '') {
                continue;
            }
            $scopeKey = $this->seoScopeKey($languageCode, $storeId);
            $ownerKey = $this->seoOwnerKey($linkType, $linkId, $languageCode, $storeId);
            $this->seoUrlOwners[$scopeKey][$urlText] = $ownerKey;
            $this->seoOwnUrls[$ownerKey] = $urlText;
        }
        $this->seoCacheLoaded = true;
    }

    /**
     * @param array<string, mixed> $values
     */
    private function rememberSeoUrlValues(array $values): void
    {
        $urlText = trim((string)($values['url_text'] ?? ''));
        $languageCode = trim((string)($values['language_code'] ?? ''));
        $storeId = trim((string)($values['store_id'] ?? ''));
        $linkType = trim((string)($values['link_type'] ?? ''));
        $linkId = trim((string)($values['link_id'] ?? ''));
        if ($urlText === '' || $languageCode === '' || $storeId === '' || $linkType === '' || $linkId === '') {
            return;
        }
        $scopeKey = $this->seoScopeKey($languageCode, $storeId);
        $ownerKey = $this->seoOwnerKey($linkType, $linkId, $languageCode, $storeId);
        $oldUrl = $this->seoOwnUrls[$ownerKey] ?? null;
        if ($oldUrl !== null && isset($this->seoUrlOwners[$scopeKey][$oldUrl]) && $this->seoUrlOwners[$scopeKey][$oldUrl] === $ownerKey) {
            unset($this->seoUrlOwners[$scopeKey][$oldUrl]);
        }
        $this->seoOwnUrls[$ownerKey] = $urlText;
        $this->seoUrlOwners[$scopeKey][$urlText] = $ownerKey;
    }

    private function seoScopeKey(string $languageCode, string $storeId): string
    {
        return $languageCode . '|' . $storeId;
    }

    private function seoOwnerKey(string $linkType, string $linkId, string $languageCode, string $storeId): string
    {
        return $linkType . '|' . $linkId . '|' . $languageCode . '|' . $storeId;
    }

    /**
     * @return array<string, mixed>
     */
    private function loadMapping(string $name): array
    {
        $path = __DIR__ . '/../../Config/mappings/' . $name . '.php.php';
        if (!is_file($path)) {
            $path = __DIR__ . '/../../Config/mappings/' . $name . '.php';
        }
        if (!is_file($path)) {
            throw new RuntimeException('Mapping nicht gefunden: ' . $name);
        }
        $mapping = require $path;
        if (!is_array($mapping)) {
            throw new RuntimeException('Mapping ungültig: ' . $name);
        }
        return $mapping;
    }

    /**
     * @param array<string, string> $columns
     */
    private function resolveBaseSource(array $columns, array $target): string
    {
        if (isset($target['base']) && is_string($target['base']) && $target['base'] !== '') {
            return $target['base'];
        }
        $entities = [];
        foreach ($columns as $expr) {
            foreach ($this->collectEntitiesFromExpr((string)$expr) as $entity) {
                $entities[$entity] = true;
            }
        }
        if (count($entities) === 1) {
            return array_key_first($entities);
        }
        // ambiguous or none -> require explicit base in mapping
        return '';
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function fetchSourceRows(PDO $pdo, string $source): iterable
    {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $source)) {
            return [];
        }
        $sql = $this->buildSourceSelectSql($source);
        $stmt = $pdo->query($sql);
        return $stmt ?: [];
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function fetchSourceRowsBatch(PDO $pdo, string $source, string $targetTable, int $limit, int $afterRowId): array
    {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $source)) {
            return [];
        }
        $limit = max(1, $limit);
        $afterRowId = max(0, $afterRowId);
        $cursor = $this->sourceCursorColumn($source);
        $sql = $this->buildSourceSelectSql($source, $targetTable) . ' AND s.' . $this->quoteIdentifier($cursor) . ' > :after_rowid ORDER BY s.' . $this->quoteIdentifier($cursor) . ' ASC LIMIT :limit';
        $stmt = $pdo->prepare($sql);
        $stmt->bindValue(':limit', $limit, PDO::PARAM_INT);
        $stmt->bindValue(':after_rowid', $afterRowId, PDO::PARAM_INT);
        $stmt->execute();
        return $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
    }

    private function buildSourceSelectSql(string $source, string $targetTable = ''): string
    {
        $table = $this->quoteIdentifier($source);
        $cursor = 's.' . $this->quoteIdentifier($this->sourceCursorColumn($source)) . ' AS __cursor';
        if ($source === 'artikel') {
            $where = 'WHERE COALESCE(s.changed, 0) = 1';
            if ($targetTable === 'xt_products_to_categories') {
                $where .= ' AND COALESCE(s.master_modell, \'\') = \'\'';
            }
            return 'SELECT ' . $cursor . ', s.*, m.meta_title AS meta_title, m.meta_description AS meta_description
                    FROM ' . $table . ' s
                    LEFT JOIN ' . $this->quoteIdentifier('Meta_Data_Artikel') . ' m ON m.afs_artikel_id = s.afs_artikel_id
                    ' . $where;
        }
        if ($source === 'warengruppe') {
            return 'SELECT ' . $cursor . ', s.*, m.meta_title AS meta_title, m.meta_description AS meta_description
                    FROM ' . $table . ' s
                    LEFT JOIN ' . $this->quoteIdentifier('Meta_Data_Waregruppen') . ' m ON m.afs_wg_id = s.afs_wg_id
                    WHERE COALESCE(s.changed, 0) = 1';
        }
        if ($this->sourceHasChangedColumn($source)) {
            return 'SELECT ' . $cursor . ', s.* FROM ' . $table . ' s WHERE COALESCE(s.changed, 0) = 1';
        }
        return 'SELECT ' . $cursor . ', s.* FROM ' . $table . ' s WHERE 1 = 1';
    }

    private function sourceHasChangedColumn(string $source): bool
    {
        $pdo = $this->pdo;
        if (!$pdo || !preg_match('/^[A-Za-z0-9_]+$/', $source)) {
            return false;
        }
        $stmt = $this->describeTable($pdo, $source);
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        foreach ($rows as $row) {
            if (strcasecmp((string)($row['name'] ?? $row['Field'] ?? ''), 'changed') === 0) {
                return true;
            }
        }
        return false;
    }

    private function loadBatchSize(PDO $pdo): int
    {
        $env = (int)env('XT_MAPPING_BATCH_SIZE', 0);
        $fallback = $env > 0 ? $env : 2000;
        try {
            $pdo->exec($this->settingsCreateSql($pdo));
            $stmt = $pdo->prepare('SELECT value FROM settings WHERE key = :key');
            $stmt->execute([':key' => 'xt_mapping_batch_size']);
            $value = $stmt->fetchColumn();
            if ($value !== false) {
                $num = (int)$value;
                if ($num >= 100 && $num <= 10000) {
                    return $num;
                }
            }
        } catch (\Throwable $e) {
            // ignore and use fallback
        }
        return $fallback;
    }

    /**
     * @return array{enabled:bool, setting:?string, env:string}
     */
    private function loadDebugEnabled(PDO $pdo): array
    {
        $env = (string)env('DEBUG', '');
        try {
            $pdo->exec($this->settingsCreateSql($pdo));
            $stmt = $pdo->prepare('SELECT value FROM settings WHERE key = :key');
            $stmt->execute([':key' => 'xt_debug_enabled']);
            $value = $stmt->fetchColumn();
            if ($value === false || $value === null || $value === '') {
                return ['enabled' => false, 'setting' => null, 'env' => $env];
            }
            return ['enabled' => (string)$value === '1', 'setting' => (string)$value, 'env' => $env];
        } catch (\Throwable $e) {
            return ['enabled' => false, 'setting' => null, 'env' => $env];
        }
    }

    private function evalExpr(string $expr, array $context, string $table, mixed $primaryKey, array $values): mixed
    {
        $expr = trim($expr);
        if ($expr === '' || strtolower($expr) === 'leer') {
            return '';
        }
        if (str_starts_with($expr, 'default:')) {
            $raw = substr($expr, 8);
            if ($this->isLiteralDefault($raw)) {
                return $this->parseDefault($raw);
            }
            return $this->evalExpr($raw, $context, $table, $primaryKey, $values);
        }
        if (str_starts_with($expr, 'calc:')) {
            return $this->calcValue(substr($expr, 5), $context);
        }
        if ($expr === 'auto') {
            return '__AUTO__';
        }
        if ($expr === 'filename') {
            return $context['media']['filename'] ?? null;
        }
        if (preg_match('/^md5\((.+)\)$/', $expr, $m)) {
            $inner = $this->evalExpr($m[1], $context, $table, $primaryKey, $values);
            return md5((string)($inner ?? ''));
        }
        if (preg_match('/^round\((.+),\s*(\d+)\)$/', $expr, $m)) {
            $inner = $this->evalExpr($m[1], $context, $table, $primaryKey, $values);
            $dec = (int)$m[2];
            if ($inner === null || $inner === '') {
                return '';
            }
            $num = is_string($inner) ? str_replace(',', '.', trim($inner)) : $inner;
            return round((float)$num, $dec);
        }
        if (str_contains($expr, ' oder ')) {
            $parts = explode(' oder ', $expr);
            foreach ($parts as $part) {
                $val = $this->evalExpr($part, $context, $table, $primaryKey, $values);
                if ($val !== null && $val !== '') {
                    return $val;
                }
            }
            return '';
        }
        if (preg_match('/^lookup\(([^,]+),([^,]+),(.+),([^,]+)\)$/', $expr, $m)) {
            $tTable = trim($m[1]);
            $tKeyCol = trim($m[2]);
            $tKeyExpr = trim($m[3]);
            $tValCol = trim($m[4]);
            $key = $this->evalExpr($tKeyExpr, $context, $table, $primaryKey, $values);
            if ($key === null || $key === '') {
                return null;
            }
            return $this->lookupTargetField($tTable, $tValCol, (string)$key, $tKeyCol);
        }
        if (preg_match('/^(.+)\+(\d+)$/', $expr, $m)) {
            $base = $this->evalExpr($m[1], $context, $table, $primaryKey, $values);
            return (int)$base + (int)$m[2];
        }
        if (preg_match('/^(xt_[a-z0-9_]+)\.([A-Za-z0-9_]+)$/i', $expr, $m)) {
            $tableRef = $m[1];
            $field = $m[2];
            return $this->resolveTargetReference($tableRef, $field, $context);
        }
        if (preg_match('/^([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)$/', $expr, $m)) {
            $entity = $m[1];
            $field = $m[2];
            if (!isset($context[$entity])) {
                $this->warn($table, $this->pkJson($primaryKey, $values), $expr, 'missing_source_field');
                return '';
            }
            if (!array_key_exists($field, $context[$entity])) {
                $this->warn($table, $this->pkJson($primaryKey, $values), $expr, 'missing_source_field');
                return '';
            }
            return $context[$entity][$field];
        }
        return $expr;
    }

    private function parseDefault(string $raw): mixed
    {
        $raw = trim($raw);
        if ($raw === '') {
            return '';
        }
        if (($raw[0] === '"' && str_ends_with($raw, '"')) || ($raw[0] === "'" && str_ends_with($raw, "'"))) {
            return substr($raw, 1, -1);
        }
        if (is_numeric($raw)) {
            return strpos($raw, '.') !== false ? (float)$raw : (int)$raw;
        }
        if (strtolower($raw) === 'true') return true;
        if (strtolower($raw) === 'false') return false;
        return $raw;
    }

    private function isLiteralDefault(string $raw): bool
    {
        $raw = trim($raw);
        if ($raw === '') {
            return true;
        }
        if (($raw[0] === '"' && str_ends_with($raw, '"')) || ($raw[0] === "'" && str_ends_with($raw, "'"))) {
            return true;
        }
        if (is_numeric($raw)) {
            return true;
        }
        $lower = strtolower($raw);
        if ($lower === 'true' || $lower === 'false' || $lower === 'null') {
            return true;
        }
        if (preg_match('/^[A-Za-z0-9_]+$/', $raw)) {
            return true;
        }
        return false;
    }

    private function calcValue(string $name, array $context = []): mixed
    {
        if ($name === 'now') {
            return (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format('Y-m-d H:i:s');
        }
        if ($name === 'nested_set_left' || $name === 'nested_set_right') {
            return null;
        }
        if ($name === 'artikel_status') {
            $row = $context['artikel'] ?? [];
            return (int)(($row['is_deleted'] ?? 0) ? 0 : ($row['Internet'] ?? 1));
        }
        if ($name === 'warengruppe_status') {
            $row = $context['warengruppe'] ?? [];
            if (($row['is_deleted'] ?? 0)) {
                return 0;
            }
            return ((string)($row['Internet'] ?? '0') === '0') ? 1 : 0;
        }
        if ($name === 'media_status') {
            $row = $context['media'] ?? [];
            return (int)(($row['is_deleted'] ?? 0) ? 0 : 1);
        }
        return null;
    }

    private function resolveTargetReference(string $table, string $field, array $context): mixed
    {
        $meta = $this->targetMetas[$table] ?? null;
        if ($meta) {
            $key = $this->resolveExternalKeyFromContext($meta, $context);
            if ($key !== null) {
                if (in_array($field, $meta['pk'], true)) {
                    $map = $this->targetMaps[$table] ?? [];
                    $id = $map[$key] ?? null;
                    if ($id !== null) {
                        return $id;
                    }
                    $id = $this->lookupTargetId($table, 'external_id', $field, $key);
                    if ($id !== null) {
                        return $id;
                    }
                } else {
                    return $this->lookupTargetField($table, $field, $key);
                }
            }
        }
        if (isset($context[$table]) && is_array($context[$table])) {
            return $context[$table][$field] ?? null;
        }
        return null;
    }

    /**
     * @param array<int, string> $pkCols
     * @param array<string, mixed> $values
     * @return array<string, mixed>
     */
    private function buildPkValues(array $pkCols, array $values): array
    {
        $out = [];
        foreach ($pkCols as $col) {
            if (array_key_exists($col, $values)) {
                $out[$col] = $values[$col];
            }
        }
        return $out;
    }

    /**
     * @return array<int, string>
     */
    private function normalizePk(mixed $primaryKey): array
    {
        if (is_array($primaryKey)) {
            return array_values(array_map('strval', $primaryKey));
        }
        if (is_string($primaryKey) && $primaryKey !== '') {
            return [$primaryKey];
        }
        return [];
    }

    /**
     * @param array<string, mixed> $values
     * @return array{row:?array, pk:array<string,mixed>}
     */
    private function findExistingRow(PDO $pdo, string $table, array $pkCols, array $pkValues, array $values): array
    {
        if ($pkCols !== [] && $pkValues !== [] && $this->hasNonAutoPk($pkValues)) {
            $where = [];
            $params = [];
            foreach ($pkCols as $col) {
                $where[] = $this->quoteIdentifier($col) . ' = :' . $col;
                $params[':' . $col] = $pkValues[$col] ?? null;
            }
            $sql = 'SELECT * FROM ' . $this->quoteIdentifier($table) . ' WHERE ' . implode(' AND ', $where) . ' LIMIT 1';
            $stmt = $pdo->prepare($sql);
            $stmt->execute($params);
            $row = $stmt->fetch(PDO::FETCH_ASSOC) ?: null;
            return ['row' => $row, 'pk' => $pkValues];
        }

        if (isset($values['external_id']) && $values['external_id'] !== null && $values['external_id'] !== '') {
            $stmt = $pdo->prepare(
                'SELECT * FROM ' . $this->quoteIdentifier($table) . ' WHERE external_id = :external_id LIMIT 1'
            );
            $stmt->execute([':external_id' => $values['external_id']]);
            $row = $stmt->fetch(PDO::FETCH_ASSOC) ?: null;
            if ($row) {
                $pk = [];
                foreach ($pkCols as $col) {
                    if (isset($row[$col])) {
                        $pk[$col] = $row[$col];
                    }
                }
                return ['row' => $row, 'pk' => $pk];
            }
        }

        return ['row' => null, 'pk' => []];
    }

    /**
     * @param array<string, mixed> $values
     */
    private function hasNonAutoPk(array $values): bool
    {
        foreach ($values as $v) {
            if ($v !== null && $v !== '' && $v !== 'auto') {
                return true;
            }
        }
        return false;
    }

    /**
     * @param array<int, string> $pkCols
     * @param array<string, mixed> $pkValues
     * @param array<string, bool> $autoCols
     */
    private function hasUsablePrimaryKey(array $pkCols, array $pkValues, array $autoCols): bool
    {
        foreach ($pkCols as $col) {
            if (isset($autoCols[$col])) {
                continue;
            }
            if (!array_key_exists($col, $pkValues)) {
                return false;
            }
            $value = $pkValues[$col];
            if ($value === null || $value === '') {
                return false;
            }
        }
        return true;
    }

    /**
     * @param array<string, string> $columns
     * @param array<string, true> $existing
     * @return array<int, string>
     */
    private function getCompareColumns(array $columns, array $existing): array
    {
        $result = [];
        foreach ($columns as $col => $expr) {
            if (strtolower($col) === 'changed') {
                continue;
            }
            if (strtolower((string)$expr) === 'auto') {
                continue;
            }
            if (!isset($existing[strtolower($col)])) {
                continue;
            }
            $result[] = $col;
        }
        return $result;
    }

    private function diffValues(array $existing, array $values, array $compareCols): bool
    {
        foreach ($compareCols as $col) {
            $old = $existing[$col] ?? null;
            $new = $values[$col] ?? null;
            if ($this->normalizeCompare($old) !== $this->normalizeCompare($new)) {
                return true;
            }
        }
        return false;
    }

    private function normalizeCompare(mixed $value): string
    {
        if ($value === null || $value === '') {
            return '';
        }
        if (is_bool($value)) {
            return $value ? '1' : '0';
        }
        $str = is_string($value) ? trim($value) : (string)$value;
        // normalize numeric strings to avoid 1 vs 1.0 vs 1.00 diffs
        if ($str !== '' && is_numeric($str)) {
            if (str_contains($str, '.') || str_contains($str, ',')) {
                $str = str_replace(',', '.', $str);
                $num = (float)$str;
                $str = rtrim(rtrim(sprintf('%.10F', $num), '0'), '.');
                if ($str === '-0') {
                    $str = '0';
                }
                return $str;
            }
            return (string)((int)$str);
        }
        return $str;
    }

    private function insertRow(PDO $pdo, string $table, array $values, array $existingCols, array $pkCols, array $autoCols): void
    {
        $cols = [];
        $params = [];
        foreach ($values as $col => $val) {
            if (!isset($existingCols[strtolower($col)])) {
                continue;
            }
            if (isset($autoCols[$col])) {
                continue;
            }
            if (in_array($col, $pkCols, true) && ($val === null || $val === '' || $val === 'auto')) {
                continue;
            }
            $cols[] = $col;
            $params[':' . $col] = $val;
        }
        $cols[] = 'changed';
        $params[':changed'] = 1;

        $sql = 'INSERT INTO ' . $this->quoteIdentifier($table) .
            ' (' . implode(',', array_map([$this, 'quoteIdentifier'], $cols)) . ')'
            . ' VALUES (' . implode(',', array_keys($params)) . ')';
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
    }

    private function insertRowIgnore(PDO $pdo, string $table, array $values, array $existingCols, array $pkCols, array $autoCols): bool
    {
        $cols = [];
        $params = [];
        foreach ($values as $col => $val) {
            if (!isset($existingCols[strtolower($col)])) {
                continue;
            }
            if (isset($autoCols[$col])) {
                continue;
            }
            if (in_array($col, $pkCols, true) && ($val === null || $val === '' || $val === 'auto')) {
                continue;
            }
            $cols[] = $col;
            $params[':' . $col] = $val;
        }
        $cols[] = 'changed';
        $params[':changed'] = 1;

        $sql = ($this->isMysql($pdo) ? 'INSERT IGNORE INTO ' : 'INSERT OR IGNORE INTO ') . $this->quoteIdentifier($table) .
            ' (' . implode(',', array_map([$this, 'quoteIdentifier'], $cols)) . ')'
            . ' VALUES (' . implode(',', array_keys($params)) . ')';
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
        return $stmt->rowCount() > 0;
    }

    private function logUniqueConflict(string $table, array $uniqueCols, array $values, PDO $pdo): void
    {
        $payload = [
            'ts' => date('c'),
            'table' => $table,
            'unique' => $uniqueCols,
            'attempted' => $this->pickColumns($values, $uniqueCols),
            'attempted_row' => $values,
            'existing_rows' => [],
        ];

        try {
            $where = [];
            $params = [];
            foreach ($uniqueCols as $col) {
                $where[] = $this->quoteIdentifier($col) . ' = :' . $col;
                $params[':' . $col] = $values[$col] ?? null;
            }
            $sql = 'SELECT * FROM ' . $this->quoteIdentifier($table) . ' WHERE ' . implode(' AND ', $where);
            $stmt = $pdo->prepare($sql);
            $stmt->execute($params);
            $payload['existing_rows'] = $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (\Throwable $e) {
            $payload['error'] = $e->getMessage();
        }

        $this->appendLog('xt_unique_conflicts.log', json_encode($payload, JSON_UNESCAPED_UNICODE));
    }

    private function updateRow(PDO $pdo, string $table, array $values, array $existingCols, array $pkCols, array $pkValues, array $autoCols, array $columns): void
    {
        $sets = [];
        $params = [];
        foreach ($values as $col => $val) {
            if (!isset($existingCols[strtolower($col)])) {
                continue;
            }
            if (isset($autoCols[$col])) {
                continue;
            }
            if (isset($columns[$col]) && $this->isDefaultExpr((string)$columns[$col])) {
                continue;
            }
            if (in_array($col, $pkCols, true)) {
                continue;
            }
            $sets[] = $this->quoteIdentifier($col) . ' = :' . $col;
            $params[':' . $col] = $val;
        }
        $sets[] = 'changed = 1';

        $where = [];
        foreach ($pkValues as $col => $val) {
            $where[] = $this->quoteIdentifier($col) . ' = :pk_' . $col;
            $params[':pk_' . $col] = $val;
        }
        if ($where === []) {
            return;
        }
        $sql = 'UPDATE ' . $this->quoteIdentifier($table) . ' SET ' . implode(',', $sets) . ' WHERE ' . implode(' AND ', $where);
        $stmt = $pdo->prepare($sql);
        try {
            $stmt->execute($params);
        } catch (\Throwable $e) {
            $this->logSqlError($table, $sql, $params, $e);
            throw $e;
        }
    }

    private function setChanged(PDO $pdo, string $table, array $pkCols, array $pkValues): void
    {
        if ($pkCols === [] || $pkValues === []) {
            return;
        }
        $where = [];
        $params = [];
        foreach ($pkValues as $col => $val) {
            $where[] = $this->quoteIdentifier($col) . ' = :' . $col;
            $params[':' . $col] = $val;
        }
        $sql = 'UPDATE ' . $this->quoteIdentifier($table) . ' SET changed = 1 WHERE ' . implode(' AND ', $where);
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
    }

    private function updateTargetMap(string $table, array $values, array $pkCols, PDO $pdo): void
    {
        $meta = $this->targetMetas[$table] ?? null;
        if (!$meta || !isset($values['external_id'])) {
            return;
        }
        $external = (string)$values['external_id'];
        if ($external === '' || $meta['pk'] === []) {
            return;
        }
        $pk = $meta['pk'][0];
        $id = $values[$pk] ?? null;
        if ($id === null || $id === '' || $id === 'auto') {
            $stmt = $pdo->prepare('SELECT ' . $this->quoteIdentifier($pk) . ' FROM ' . $this->quoteIdentifier($table) . ' WHERE external_id = :ext LIMIT 1');
            $stmt->execute([':ext' => $external]);
            $id = $stmt->fetchColumn();
        }
        if ($id !== null && $id !== '') {
            $this->targetMaps[$table][(string)$external] = (int)$id;
        }
    }

    private function loadTargetMapFromDb(PDO $pdo, string $table, string $keyCol, string $idCol): void
    {
        if (isset($this->targetMapsLoaded[$table])) {
            return;
        }
        $cols = $this->getExistingColumns($pdo, $table);
        if ($cols === [] || !isset($cols[strtolower($keyCol)]) || !isset($cols[strtolower($idCol)])) {
            $this->targetMapsLoaded[$table] = true;
            return;
        }
        $sql = 'SELECT ' . $this->quoteIdentifier($keyCol) . ' AS k, ' . $this->quoteIdentifier($idCol) . ' AS v'
            . ' FROM ' . $this->quoteIdentifier($table)
            . ' WHERE ' . $this->quoteIdentifier($keyCol) . ' IS NOT NULL AND ' . $this->quoteIdentifier($keyCol) . " != ''";
        $stmt = $pdo->query($sql);
        if ($stmt) {
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
            $map = [];
            foreach ($rows as $row) {
                $k = (string)($row['k'] ?? '');
                $v = $row['v'] ?? null;
                if ($k !== '' && $v !== null && $v !== '') {
                    $map[$k] = (int)$v;
                }
            }
            $this->targetMaps[$table] = $map;
        }
        $this->targetMapsLoaded[$table] = true;
    }

    /**
     * @param array<string, mixed> $target
     * @return array<string, array{table:string, pk:array<int,string>, ext_entity:?string, ext_field:?string}>
     */
    private function buildTargetMetas(array $targets): array
    {
        $metas = [];
        foreach ($targets as $targetName => $target) {
            if (!is_array($target)) {
                continue;
            }
            $table = (string)($target['table'] ?? $targetName);
            if ($table === '') {
                continue;
            }
            $pk = $this->normalizePk($target['primary_key'] ?? null);
            $columns = $target['columns'] ?? [];
            $extEntity = null;
            $extField = null;
            if (is_array($columns) && isset($columns['external_id'])) {
                $ref = $this->parseEntityFieldExpr((string)$columns['external_id']);
                if ($ref) {
                    $extEntity = $ref['entity'];
                    $extField = $ref['field'];
                }
            }
            $metas[$table] = [
                'table' => $table,
                'pk' => $pk,
                'ext_entity' => $extEntity,
                'ext_field' => $extField,
            ];
        }
        return $metas;
    }

    private function parseEntityFieldExpr(string $expr): ?array
    {
        $expr = trim($expr);
        if (preg_match('/^([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)$/', $expr, $m)) {
            if (str_starts_with(strtolower($m[1]), 'xt_')) {
                return null;
            }
            return ['entity' => $m[1], 'field' => $m[2]];
        }
        return null;
    }

    private function parseTargetFieldExpr(string $expr): ?array
    {
        $expr = trim($expr);
        if (preg_match('/^(xt_[A-Za-z0-9_]+)\.([A-Za-z0-9_]+)$/', $expr, $m)) {
            return ['table' => $m[1], 'field' => $m[2]];
        }
        return null;
    }

    /**
     * @return array<int, string>
     */
    private function collectEntitiesFromExpr(string $expr): array
    {
        $expr = trim($expr);
        if ($expr === '' || str_starts_with($expr, 'default:') || str_starts_with($expr, 'calc:')) {
            return [];
        }
        if ($expr === 'auto' || $expr === 'filename') {
            return [];
        }
        if (preg_match('/^md5\((.+)\)$/', $expr, $m)) {
            return $this->collectEntitiesFromExpr($m[1]);
        }
        if (str_contains($expr, ' oder ')) {
            $out = [];
            foreach (explode(' oder ', $expr) as $part) {
                foreach ($this->collectEntitiesFromExpr($part) as $e) {
                    $out[$e] = true;
                }
            }
            return array_keys($out);
        }
        // collect all non-xt entity.field occurrences
        if (preg_match_all('/([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)/', $expr, $m, PREG_SET_ORDER)) {
            $out = [];
            foreach ($m as $match) {
                $entity = $match[1];
                if (str_starts_with(strtolower($entity), 'xt_')) {
                    continue;
                }
                $out[$entity] = true;
            }
            return array_keys($out);
        }
        return [];
    }

    private function resolveExternalKeyFromContext(array $meta, array $context): ?string
    {
        $entity = $meta['ext_entity'] ?? null;
        $field = $meta['ext_field'] ?? null;
        if ($entity && $field && isset($context[$entity]) && is_array($context[$entity])) {
            $val = $context[$entity][$field] ?? null;
            if ($val !== null && $val !== '') {
                return trim((string)$val);
            }
        }
        // heuristic fallback: try common field names on the base row
        foreach ($context as $row) {
            if (!is_array($row)) continue;
            foreach ([$field, 'warengruppe_id', 'Warengruppe', 'afs_wg_id', 'afs_id', 'afs_artikel_id', 'Artikel', 'id'] as $cand) {
                if ($cand && isset($row[$cand]) && $row[$cand] !== null && $row[$cand] !== '') {
                    return trim((string)$row[$cand]);
                }
            }
        }
        return null;
    }

    private function lookupTargetField(string $table, string $field, string $externalKey, string $keyCol = 'external_id'): mixed
    {
        if ($this->pdo === null) {
            return null;
        }
        $cacheKey = $keyCol . '|' . $externalKey;
        if (isset($this->targetRowCache[$table][$cacheKey]) && array_key_exists($field, $this->targetRowCache[$table][$cacheKey])) {
            return $this->targetRowCache[$table][$cacheKey][$field];
        }
        try {
            $stmt = $this->pdo->prepare(
                'SELECT ' . $this->quoteIdentifier($field) . ' FROM ' . $this->quoteIdentifier($table) .
                ' WHERE ' . $this->quoteIdentifier($keyCol) . ' = :key LIMIT 1'
            );
            $stmt->execute([':key' => $externalKey]);
            $val = $stmt->fetchColumn();
            $this->targetRowCache[$table][$cacheKey][$field] = $val;
            return $val !== false ? $val : null;
        } catch (\Throwable $e) {
            return null;
        }
    }

    private function lookupTargetId(string $table, string $keyCol, string $idCol, string $key): ?int
    {
        if ($this->pdo === null || $key === '') {
            return null;
        }
        try {
            $stmt = $this->pdo->prepare(
                'SELECT ' . $this->quoteIdentifier($idCol) . ' FROM ' . $this->quoteIdentifier($table) .
                ' WHERE ' . $this->quoteIdentifier($keyCol) . ' = :key LIMIT 1'
            );
            $stmt->execute([':key' => $key]);
            $id = $stmt->fetchColumn();
            if ($id !== false && $id !== null && $id !== '') {
                $this->targetMaps[$table][$key] = (int)$id;
                return (int)$id;
            }
        } catch (\Throwable $e) {
            // ignore
        }
        return null;
    }

    private function rebuildNestedSet(PDO $pdo): int
    {
        $stmt = $pdo->prepare('UPDATE xt_categories SET categories_left = :l, categories_right = :r WHERE categories_id = :id');
        $childrenStmt = $pdo->prepare('SELECT categories_id, sort_order FROM xt_categories WHERE parent_id = :pid ORDER BY sort_order ASC, categories_id ASC');
        $rootStmt = $pdo->prepare('SELECT categories_id, sort_order FROM xt_categories WHERE parent_id IS NULL ORDER BY sort_order ASC, categories_id ASC');
        $counter = 1;
        $updated = 0;

        $stack = [
            ['id' => null, 'idx' => 0, 'started' => false, 'left' => 0, 'children' => null],
        ];

        while ($stack !== []) {
            $topIndex = count($stack) - 1;
            $node = &$stack[$topIndex];
            if (!$node['started']) {
                $node['left'] = $counter++;
                $node['started'] = true;
            }

            if ($node['children'] === null) {
                if ($node['id'] === null) {
                    $rootStmt->execute();
                    $rows = $rootStmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
                } else {
                    $childrenStmt->execute([':pid' => $node['id']]);
                    $rows = $childrenStmt->fetchAll(PDO::FETCH_ASSOC) ?: [];
                }
                $childIds = [];
                foreach ($rows as $row) {
                    $childIds[] = (int)($row['categories_id'] ?? 0);
                }
                $node['children'] = $childIds;
            }

            $children = $node['children'];
            if ($node['idx'] < count($children)) {
                $childId = $children[$node['idx']];
                $node['idx']++;
                $stack[] = ['id' => $childId, 'idx' => 0, 'started' => false, 'left' => 0, 'children' => null];
                unset($node);
                continue;
            }

            $right = $counter++;
            if ($node['id'] !== 0) {
                $stmt->execute([':l' => $node['left'], ':r' => $right, ':id' => $node['id']]);
                $updated++;
            }
            array_pop($stack);
            unset($node);
        }

        return $updated;
    }

    private function xtCategoriesNeedNestedSetRepair(PDO $pdo): bool
    {
        $stmt = $pdo->query('SELECT 1 FROM xt_categories WHERE categories_left IS NULL OR categories_right IS NULL LIMIT 1');
        return $stmt ? ($stmt->fetchColumn() !== false) : false;
    }

    private function repairXtCategoriesDefaults(PDO $pdo): void
    {
        $pdo->exec("UPDATE xt_categories SET categories_status = '1' WHERE COALESCE(categories_status, '') IN ('', '0')");
        $pdo->exec("UPDATE xt_categories SET category_custom_link_id = '0' WHERE category_custom_link_id IS NULL OR category_custom_link_id = ''");
        $pdo->exec("UPDATE xt_categories SET google_product_cat = '0' WHERE google_product_cat IS NULL OR google_product_cat = ''");
        $pdo->exec("UPDATE xt_categories SET changed = 1 WHERE categories_status = '1' OR category_custom_link_id = '0' OR google_product_cat = '0'");
    }

    private function createTable(PDO $pdo, string $table, array $columns, mixed $primaryKey): void
    {
        $defs = [];
        $pk = $this->normalizePk($primaryKey);
        $autoPk = null;
        if (count($pk) === 1) {
            $pkCol = $pk[0];
            $pkExpr = $columns[$pkCol] ?? null;
            if (is_string($pkExpr) && strtolower(trim($pkExpr)) === 'auto') {
                $autoPk = $pkCol;
            }
        }
        foreach ($columns as $col => $expr) {
            if (!preg_match('/^[A-Za-z0-9_]+$/', $col)) {
                continue;
            }
            if ($autoPk !== null && $col === $autoPk) {
                $defs[] = $this->quoteIdentifier($col) . ' ' . ($this->isMysql($pdo) ? 'INT AUTO_INCREMENT PRIMARY KEY' : 'INTEGER PRIMARY KEY AUTOINCREMENT');
                continue;
            }
            $defs[] = $this->quoteIdentifier($col) . ' ' . $this->inferCreateColumnType($pdo, $col, in_array($col, $pk, true));
        }
        $defs[] = 'changed ' . ($this->isMysql($pdo) ? 'TINYINT NOT NULL DEFAULT 0' : 'INTEGER NOT NULL DEFAULT 0');
        if ($autoPk === null && count($pk) === 1 && $pk[0] !== '') {
            $defs[] = 'PRIMARY KEY (' . $this->quoteIdentifier($pk[0]) . ')';
        }
        $suffix = $this->isMysql($pdo) ? ' ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci' : '';
        $pdo->exec('CREATE TABLE IF NOT EXISTS ' . $this->quoteIdentifier($table) . ' (' . implode(',', $defs) . ')' . $suffix);
    }

    private function ensureColumns(PDO $pdo, string $table, array $columns, array $existing): void
    {
        foreach ($columns as $col => $expr) {
            if (!preg_match('/^[A-Za-z0-9_]+$/', $col)) {
                continue;
            }
            if (isset($existing[strtolower($col)])) {
                continue;
            }
            $pdo->exec('ALTER TABLE ' . $this->quoteIdentifier($table) . ' ADD COLUMN ' . $this->quoteIdentifier($col) . ' ' . $this->inferCreateColumnType($pdo, $col, false));
        }
    }

    /**
     * @return array<string, true>
     */
    private function getExistingColumns(PDO $pdo, string $table): array
    {
        if (!$this->tableExists($pdo, $table)) {
            return [];
        }
        $stmt = $this->describeTable($pdo, $table);
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        $cols = [];
        foreach ($rows as $row) {
            $name = (string)($row['name'] ?? $row['Field'] ?? '');
            if ($name !== '') {
                $cols[strtolower($name)] = true;
            }
        }
        return $cols;
    }

    private function warn(string $table, string $pkJson, string $mapping, string $reason): void
    {
        $key = $table . '|' . $mapping . '|' . $reason;
        if (isset($this->loggedMissing[$key])) {
            return;
        }
        $this->loggedMissing[$key] = true;
        $line = 'WARNING table=' . $table . ' pk=' . $pkJson . ' mapping=' . $mapping . ' reason=' . $reason;
        $this->logWarning($line);
    }

    private function pkJson(mixed $primaryKey, array $values): string
    {
        $pkCols = $this->normalizePk($primaryKey);
        $data = [];
        foreach ($pkCols as $col) {
            if (array_key_exists($col, $values)) {
                $data[$col] = $values[$col];
            }
        }
        return json_encode($data);
    }

    private function quoteIdentifier(string $name): string
    {
        return '`' . str_replace('`', '``', $name) . '`';
    }

    private function buildUpsertStatement(PDO $pdo, string $table, array $columns, array $existingCols, array $pkCols, array $compareCols): ?\PDOStatement
    {
        if ($pkCols === []) {
            return null;
        }
        $cols = [];
        foreach ($columns as $col => $expr) {
            if (!isset($existingCols[strtolower($col)])) {
                continue;
            }
            if (strtolower((string)$expr) === 'auto') {
                continue;
            }
            $cols[] = $col;
        }
        $cols[] = 'changed';
        $params = array_map(static fn(string $c): string => ':' . $c, $cols);
        $insert = 'INSERT INTO ' . $this->quoteIdentifier($table) .
            ' (' . implode(',', array_map([$this, 'quoteIdentifier'], $cols)) . ')' .
            ' VALUES (' . implode(',', $params) . ')';

        $setParts = [];
        foreach ($columns as $col => $expr) {
            if (!in_array($col, $cols, true)) {
                continue;
            }
            if ($col === 'changed') {
                continue;
            }
            if ($this->isDefaultExpr((string)$expr)) {
                continue;
            }
            $setParts[] = $this->quoteIdentifier($col) . ' = excluded.' . $this->quoteIdentifier($col);
        }
        $setParts[] = 'changed = 1';

        $whereParts = [];
        foreach ($compareCols as $col) {
            $whereParts[] = 'COALESCE(excluded.' . $this->quoteIdentifier($col) . ', \'\') <> COALESCE(' . $this->quoteIdentifier($col) . ', \'\')';
        }
        $where = $whereParts !== [] ? (' WHERE ' . implode(' OR ', $whereParts)) : '';

        if ($this->isMysql($pdo)) {
            $mysqlSetParts = [];
            foreach ($columns as $col => $expr) {
                if (!in_array($col, $cols, true) || $col === 'changed' || $this->isDefaultExpr((string)$expr)) {
                    continue;
                }
                $mysqlSetParts[] = $this->quoteIdentifier($col) . ' = VALUES(' . $this->quoteIdentifier($col) . ')';
            }
            $mysqlSetParts[] = 'changed = 1';
            $sql = $insert . ' ON DUPLICATE KEY UPDATE ' . implode(', ', $mysqlSetParts);
        } else {
            $sql = $insert . ' ON CONFLICT(' . implode(',', array_map([$this, 'quoteIdentifier'], $pkCols)) . ') DO UPDATE SET ' .
                implode(', ', $setParts) . $where;
        }
        return $pdo->prepare($sql);
    }

    /**
     * @return array<int, string>
     */
    private function getUpsertColumns(string $table, array $columns, array $existingCols): array
    {
        $cols = [];
        foreach ($columns as $col => $expr) {
            if (!isset($existingCols[strtolower($col)])) continue;
            if (strtolower((string)$expr) === 'auto') continue;
            $cols[] = $col;
        }
        $cols[] = 'changed';
        return $cols;
    }

    /**
     * @return array<int, string>
     */
    private function getUpdateColumns(string $table, array $columns, array $existingCols, array $pkCols): array
    {
        $cols = [];
        foreach ($columns as $col => $expr) {
            if (!isset($existingCols[strtolower($col)])) continue;
            if (strtolower((string)$expr) === 'auto') continue;
            if (in_array($col, $pkCols, true)) continue;
            $cols[] = $col;
        }
        $cols[] = 'changed';
        return $cols;
    }

    private function isDefaultExpr(string $expr): bool
    {
        $expr = trim($expr);
        return str_starts_with($expr, 'default:');
    }

    private function ensurePkIndex(PDO $pdo, string $table, array $pkCols): void
    {
        $idx = 'idx_' . $table . '_pk';
        if (!$this->indexExists($pdo, $table, $idx)) {
            $cols = implode(',', array_map([$this, 'quoteIdentifier'], $pkCols));
            $sql = ($this->isMysql($pdo) ? 'CREATE UNIQUE INDEX ' : 'CREATE UNIQUE INDEX IF NOT EXISTS ') . $this->quoteIdentifier($idx) . ' ON ' . $this->quoteIdentifier($table) . '(' . $cols . ')';
            $pdo->exec($sql);
        }
    }

    private function buildSelectByPk(PDO $pdo, string $table, array $pkCols, array $compareCols): \PDOStatement
    {
        $cols = array_values(array_unique(array_merge($pkCols, $compareCols)));
        $select = 'SELECT ' . implode(',', array_map([$this, 'quoteIdentifier'], $cols)) .
            ' FROM ' . $this->quoteIdentifier($table) .
            ' WHERE ' . implode(' AND ', array_map(fn(string $c): string => $this->quoteIdentifier($c) . ' = :' . $c, $pkCols)) .
            ' LIMIT 1';
        return $pdo->prepare($select);
    }

    private function buildSelectByExternal(PDO $pdo, string $table, array $compareCols, array $existingCols): ?\PDOStatement
    {
        if (!isset($existingCols['external_id'])) {
            return null;
        }
        $cols = array_values(array_unique(array_merge(['external_id'], $compareCols)));
        $select = 'SELECT ' . implode(',', array_map([$this, 'quoteIdentifier'], $cols)) .
            ' FROM ' . $this->quoteIdentifier($table) . ' WHERE external_id = :external_id LIMIT 1';
        return $pdo->prepare($select);
    }

    private function buildUpdateStatement(PDO $pdo, string $table, array $columns, array $existingCols, array $pkCols): ?\PDOStatement
    {
        if ($pkCols === []) {
            return null;
        }
        $sets = [];
        foreach ($columns as $col => $expr) {
            if (!isset($existingCols[strtolower($col)])) continue;
            if (strtolower((string)$expr) === 'auto') continue;
            if (in_array($col, $pkCols, true)) continue;
            $sets[] = $this->quoteIdentifier($col) . ' = :' . $col;
        }
        $sets[] = 'changed = 1';
        $where = implode(' AND ', array_map(fn(string $c): string => $this->quoteIdentifier($c) . ' = :pk_' . $c, $pkCols));
        if ($sets === ['changed = 1']) {
            return null;
        }
        $sql = 'UPDATE ' . $this->quoteIdentifier($table) . ' SET ' . implode(', ', $sets) . ' WHERE ' . $where;
        return $pdo->prepare($sql);
    }

    private function isMysql(PDO $pdo): bool
    {
        return (string)$pdo->getAttribute(PDO::ATTR_DRIVER_NAME) === 'mysql';
    }

    private function settingsCreateSql(PDO $pdo): string
    {
        return $this->isMysql($pdo)
            ? 'CREATE TABLE IF NOT EXISTS settings (`key` VARCHAR(190) PRIMARY KEY, `value` TEXT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            : 'CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)';
    }

    private function sourceCursorColumn(string $source): string
    {
        return match ($source) {
            'attributes' => 'attributes_id',
            default => 'id',
        };
    }

    private function describeTable(PDO $pdo, string $table): ?\PDOStatement
    {
        return $this->isMysql($pdo)
            ? $pdo->query('DESCRIBE ' . $this->quoteIdentifier($table))
            : $pdo->query('PRAGMA table_info(' . $this->quoteIdentifier($table) . ')');
    }

    private function tableExists(PDO $pdo, string $table): bool
    {
        if ($this->isMysql($pdo)) {
            $stmt = $pdo->prepare('SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :table');
            $stmt->execute([':table' => $table]);
            return (int)$stmt->fetchColumn() > 0;
        }
        $stmt = $pdo->query('SELECT name FROM sqlite_master WHERE type = "table" AND name = ' . $pdo->quote($table));
        return (bool)($stmt ? $stmt->fetchColumn() : false);
    }

    private function indexExists(PDO $pdo, string $table, string $index): bool
    {
        if ($this->isMysql($pdo)) {
            $stmt = $pdo->prepare('SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = :table AND index_name = :idx');
            $stmt->execute([':table' => $table, ':idx' => $index]);
            return (int)$stmt->fetchColumn() > 0;
        }
        $list = $pdo->query('PRAGMA index_list(' . $this->quoteIdentifier($table) . ')');
        $rows = $list ? $list->fetchAll(PDO::FETCH_ASSOC) : [];
        foreach ($rows as $row) {
            if (($row['name'] ?? '') === $index) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param array<int,string> $columns
     */
    private function createIndexIfMissing(PDO $pdo, string $index, string $table, array $columns): void
    {
        if ($this->indexExists($pdo, $table, $index)) {
            return;
        }
        $cols = implode(',', array_map([$this, 'quoteIdentifier'], $columns));
        $sql = ($this->isMysql($pdo) ? 'CREATE INDEX ' : 'CREATE INDEX IF NOT EXISTS ') . $this->quoteIdentifier($index) . ' ON ' . $this->quoteIdentifier($table) . '(' . $cols . ')';
        $pdo->exec($sql);
    }

    private function inferCreateColumnType(PDO $pdo, string $column, bool $isKey): string
    {
        if (!$this->isMysql($pdo)) {
            return 'TEXT';
        }
        $lower = strtolower($column);
        if ($isKey || $lower === 'external_id' || str_ends_with($lower, '_id') || str_ends_with($lower, '_code') || $lower === 'language_code' || $lower === 'store_id' || $lower === 'url_md5') {
            return 'VARCHAR(255)';
        }
        if ($lower === 'changed') {
            return 'TINYINT';
        }
        if (str_contains($lower, 'status') || str_contains($lower, 'sort_order') || str_contains($lower, 'left') || str_contains($lower, 'right')) {
            return 'INT';
        }
        return 'LONGTEXT';
    }

    private function bindValues(\PDOStatement $stmt, array $values, array $columns, array $existingCols, array $pkCols, array $autoCols, array $allowedCols = []): void
    {
        $allowed = $allowedCols !== [] ? array_flip($allowedCols) : null;
        foreach ($columns as $col => $expr) {
            if (!isset($existingCols[strtolower($col)])) continue;
            if (isset($autoCols[$col])) continue;
            if (in_array($col, $pkCols, true) && ($values[$col] ?? null) === null) continue;
            if ($allowed !== null && !isset($allowed[$col])) continue;
            $stmt->bindValue(':' . $col, $values[$col] ?? null);
        }
        if (str_contains($stmt->queryString, ':changed')) {
            $stmt->bindValue(':changed', 1, PDO::PARAM_INT);
        }
    }

    private function bindPk(\PDOStatement $stmt, array $pkCols, array $pkValues): void
    {
        $sql = $stmt->queryString ?? '';
        foreach ($pkCols as $pk) {
            $val = $pkValues[$pk] ?? null;
            if (str_contains($sql, ':pk_' . $pk)) {
                $stmt->bindValue(':pk_' . $pk, $val);
            }
            if (str_contains($sql, ':' . $pk)) {
                $stmt->bindValue(':' . $pk, $val);
            }
        }
    }

    private function logWarning(string $line): void
    {
        $path = $this->getLogsDir() . '/xt_mapping_warnings.log';
        @file_put_contents($path, $line . PHP_EOL, FILE_APPEND);
    }

    private function appendLog(string $filename, string $line): void
    {
        $path = $this->getLogsDir() . '/' . $filename;
        @file_put_contents($path, $line . PHP_EOL, FILE_APPEND);
    }

    private function logSqlError(string $table, string $sql, array $params, \Throwable $e): void
    {
        $entry = [
            'ts' => date('c'),
            'table' => $table,
            'sql' => $sql,
            'params' => $params,
            'error' => $e->getMessage(),
        ];
        $this->appendLog('xt_sql_errors.log', json_encode($entry, JSON_UNESCAPED_UNICODE));
    }

    private function pickColumns(array $values, array $cols): array
    {
        $out = [];
        foreach ($cols as $col) {
            $out[$col] = $values[$col] ?? null;
        }
        return $out;
    }

    private function getLogsDir(): string
    {
        return __DIR__ . '/../../../logs';
    }

    private function logSeoDiff(mixed $primaryKey, array $existing, array $values): void
    {
        $keys = array_unique(array_merge(array_keys($existing), array_keys($values)));
        $diff = [];
        foreach ($keys as $k) {
            $old = $existing[$k] ?? null;
            $new = $values[$k] ?? null;
            if ($this->normalizeCompare($old) !== $this->normalizeCompare($new)) {
                $diff[$k] = ['old' => $old, 'new' => $new];
            }
        }
        if ($diff === []) {
            return;
        }
        $entry = [
            'ts' => (new DateTimeImmutable('now', new DateTimeZone('UTC')))->format(DATE_ATOM),
            'pk' => json_decode($this->pkJson($primaryKey, $values), true),
            'diff' => $diff,
        ];
        $path = $this->getLogsDir() . '/xt_seo_diff.log';
        @file_put_contents($path, json_encode($entry, JSON_UNESCAPED_UNICODE) . PHP_EOL, FILE_APPEND);
    }

    private function debugDiff(string $table, mixed $primaryKey, array $values, array $existing, array $compareCols): void
    {
        if (!$this->debugEnabled) {
            return;
        }
        $count = $this->debugDiffCount[$table] ?? 0;
        if ($count >= 3) {
            return;
        }
        foreach ($compareCols as $col) {
            $old = $existing[$col] ?? null;
            $new = $values[$col] ?? null;
            if ($this->normalizeCompare($old) !== $this->normalizeCompare($new)) {
                $pk = $this->pkJson($primaryKey, $values);
                $line = 'DIFF table=' . $table . ' pk=' . $pk . ' col=' . $col
                    . ' old=' . $this->normalizeCompare($old) . ' new=' . $this->normalizeCompare($new);
                $path = $this->getLogsDir() . '/xt_mapping_debug_diff.log';
                @file_put_contents($path, $line . PHP_EOL, FILE_APPEND);
                $this->debugDiffCount[$table] = $count + 1;
                break;
            }
        }
    }

}
