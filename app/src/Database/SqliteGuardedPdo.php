<?php
declare(strict_types=1);

namespace Welafix\Database;

use PDO;
use RuntimeException;

final class SqliteGuardedPdo extends PDO
{
    private string $component = '';

    public function setComponent(string $component): void
    {
        $this->component = $component;
    }

    public function exec(string $statement): int|false
    {
        $this->guardDialect((string)$statement);
        return parent::exec($statement);
    }

    public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): \PDOStatement|false
    {
        $this->guardDialect($query);
        return parent::query($query, $fetchMode, ...$fetchModeArgs);
    }

    public function prepare(string $statement, array $options = []): \PDOStatement|false
    {
        $this->guardDialect($statement);
        return parent::prepare($statement, $options);
    }

    private function guardDialect(string $sql): void
    {
        $sql = trim($sql);
        if ($sql === '') {
            return;
        }
        $mssqlHit = false;
        $patterns = [
            '/\bSELECT\s+TOP\b/i',
            '/\bFROM\s+dbo\./i',
            '/\bFROM\s+\[dbo\]\./i',
            '/\b@@\w+/i',
            '/\bWITH\s*\(\s*NOLOCK\s*\)/i',
            '/\bISNULL\s*\(/i',
            '/\bGETDATE\s*\(/i',
            '/\bCONVERT\s*\(/i',
            '/\bCAST\s*\([^\)]*\bAS\s+N?VARCHAR\b/i',
            '/\bOFFSET\s+\d+\s+ROWS\s+FETCH\b/i',
        ];
        foreach ($patterns as $pattern) {
            if (preg_match($pattern, $sql)) {
                $mssqlHit = true;
                break;
            }
        }

        if (!$mssqlHit && preg_match('/\[[^\]]+\]/', $sql)) {
            if (preg_match('/\bTOP\b|\bNOLOCK\b|\bISNULL\b|\bGETDATE\b|\bCONVERT\b|\bOFFSET\b|\bFETCH\b/i', $sql)) {
                $mssqlHit = true;
            }
        }

        if ($mssqlHit) {
            $driver = $this->getAttribute(PDO::ATTR_DRIVER_NAME);
            $sqlitePath = getenv('SQLITE_PATH') ?: '';
            $callsite = $this->findCallsite();
            $component = $this->component !== '' ? $this->component : 'unbekannt';
            $message = 'MSSQL SQL auf SQLite ist nicht erlaubt.'
                . ' Component=' . $component
                . ', Driver=' . $driver;
            if ($sqlitePath !== '') {
                $message .= ', SQLITE_PATH=' . $sqlitePath;
            }
            if ($callsite !== '') {
                $message .= ', Callsite=' . $callsite;
            }
            $message .= ', Query=' . $this->truncate($sql);
            throw new RuntimeException($message);
        }
    }

    private function truncate(string $sql): string
    {
        $sql = trim(preg_replace('/\s+/', ' ', $sql) ?? $sql);
        if (strlen($sql) > 200) {
            return substr($sql, 0, 200) . '...';
        }
        return $sql;
    }

    private function findCallsite(): string
    {
        $trace = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 8);
        $self = __FILE__;
        foreach ($trace as $frame) {
            $file = $frame['file'] ?? '';
            if ($file === '' || $file === $self) {
                continue;
            }
            $line = isset($frame['line']) ? (string)$frame['line'] : '';
            return $file . ($line !== '' ? ':' . $line : '');
        }
        return '';
    }
}
