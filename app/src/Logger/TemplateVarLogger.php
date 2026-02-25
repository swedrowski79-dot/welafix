<?php
declare(strict_types=1);

namespace Welafix\Logger;

final class TemplateVarLogger
{
    public function __construct(private ?string $logPath = null)
    {
        if ($this->logPath === null) {
            $this->logPath = __DIR__ . '/../../../storage/logs/template_vars.log';
        }
    }

    /**
     * @param array<string, mixed> $rowIdentity
     * @param array<int, string> $missingKeys
     */
    public function logMissing(string $tableName, array $rowIdentity, array $missingKeys): void
    {
        if ($missingKeys === []) {
            return;
        }

        $dir = dirname($this->logPath);
        if (!is_dir($dir)) {
            @mkdir($dir, 0777, true);
        }

        $identity = json_encode($rowIdentity, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($identity === false) {
            $identity = '{}';
        }

        $lines = '';
        foreach ($missingKeys as $key) {
            $lines .= 'MISSING_TEMPLATE_VAR table=' . $tableName . ' identity=' . $identity . ' var=' . $key . "\n";
        }

        @file_put_contents($this->logPath, $lines, FILE_APPEND);
    }
}
