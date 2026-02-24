<?php
declare(strict_types=1);

namespace Welafix\Config;

use RuntimeException;

final class MappingLoader
{
    private string $directory;

    public function __construct(?string $directory = null)
    {
        $this->directory = $directory ?? (__DIR__ . '/mappings');
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    public function loadAll(): array
    {
        if (!is_dir($this->directory)) {
            throw new RuntimeException("Mapping-Verzeichnis nicht gefunden: {$this->directory}");
        }

        $files = glob($this->directory . '/*.php');
        if ($files === false) {
            throw new RuntimeException("Mapping-Verzeichnis konnte nicht gelesen werden: {$this->directory}");
        }

        sort($files);

        $mappings = [];
        foreach ($files as $file) {
            $name = basename($file, '.php');

            $mapping = require $file;
            if (!is_array($mapping)) {
                throw new RuntimeException("Mapping-Datei '{$file}' muss ein Array zurueckgeben.");
            }

            $mappings[$name] = $this->validateMapping($mapping, $name, $file);
        }

        return $mappings;
    }

    /**
     * @param array<string, mixed> $mapping
     * @return array<string, mixed>
     */
    private function validateMapping(array $mapping, string $name, string $file): array
    {
        if (!isset($mapping['source']) || !is_array($mapping['source'])) {
            throw new RuntimeException("Mapping '{$name}': Feld 'source' fehlt oder ist kein Array ({$file}).");
        }

        $source = $mapping['source'];

        if (!isset($source['table']) || !is_string($source['table']) || $source['table'] === '') {
            throw new RuntimeException("Mapping '{$name}': Pflichtfeld 'source.table' fehlt oder ist kein String ({$file}).");
        }

        if (!isset($source['key']) || !is_string($source['key']) || $source['key'] === '') {
            throw new RuntimeException("Mapping '{$name}': Pflichtfeld 'source.key' fehlt oder ist kein String ({$file}).");
        }

        if (!isset($mapping['select']) || !is_array($mapping['select']) || $mapping['select'] === []) {
            throw new RuntimeException("Mapping '{$name}': Pflichtfeld 'select' fehlt oder ist leer ({$file}).");
        }

        if (!isset($source['where'])) {
            $source['where'] = '1=1';
        } elseif (!is_string($source['where']) || $source['where'] === '') {
            throw new RuntimeException("Mapping '{$name}': Feld 'source.where' muss ein String sein ({$file}).");
        }

        $mapping['source'] = $source;

        return $mapping;
    }
}
