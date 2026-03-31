<?php
declare(strict_types=1);

namespace Welafix\Domain\Attribute;

use PDO;

final class AttributesBuilder
{
    private PDO $pdo;
    /** @var array<string, int> */
    private array $parentMap = [];
    /** @var array<string, int> */
    private array $childMap = [];
    private bool $loaded = false;

    public int $parentsCreated = 0;
    public int $childrenCreated = 0;

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    public function ingestRow(array $row): void
    {
        $this->ingestRowWithAssignments($row);
    }

    /**
     * @param array<string, mixed> $row
     * @return array<int, array{position:int,parent_id:int,attribute_id:int,name:string,value:string}>
     */
    public function ingestRowWithAssignments(array $row): array
    {
        $pairs = [
            ['Zusatzfeld03', 'Zusatzfeld15'],
            ['Zusatzfeld04', 'Zusatzfeld16'],
            ['Zusatzfeld05', 'Zusatzfeld17'],
        ];

        $assignments = [];

        foreach ($pairs as $index => [$nameField, $valueField]) {
            $name = trim((string)($row[$nameField] ?? ''));
            $value = trim((string)($row[$valueField] ?? ''));
            if ($name === '' || $value === '') {
                continue;
            }

            $parentId = $this->ensureParent($name);
            $attributeId = $this->ensureChild($parentId, $value);
            if ($attributeId <= 0) {
                continue;
            }
            $assignments[] = [
                'position' => $index + 1,
                'parent_id' => $parentId,
                'attribute_id' => $attributeId,
                'name' => $name,
                'value' => $value,
            ];
        }

        return $assignments;
    }

    private function ensureParent(string $name): int
    {
        $this->loadExisting();
        $key = strtolower($name);
        if (isset($this->parentMap[$key])) {
            return $this->parentMap[$key];
        }

        $stmt = $this->pdo->prepare(
            'INSERT OR IGNORE INTO attributes (attributes_parent, attributes_model, changed)
             VALUES (0, :model, 1)'
        );
        $stmt->execute([':model' => $name]);

        $id = $this->fetchAttributeId(0, $name);
        $this->parentMap[$key] = $id;
        $this->parentsCreated++;
        return $id;
    }

    private function ensureChild(int $parentId, string $value): int
    {
        $this->loadExisting();
        $key = $parentId . '|' . strtolower($value);
        if (isset($this->childMap[$key])) {
            return $this->childMap[$key];
        }

        $stmt = $this->pdo->prepare(
            'INSERT OR IGNORE INTO attributes (attributes_parent, attributes_model, changed)
             VALUES (:parent, :model, 1)'
        );
        $stmt->execute([':parent' => $parentId, ':model' => $value]);

        $id = $this->fetchAttributeId($parentId, $value);
        $this->childMap[$key] = $id;
        $this->childrenCreated++;
        return $id;
    }

    private function fetchAttributeId(int $parentId, string $model): int
    {
        $stmt = $this->pdo->prepare(
            'SELECT attributes_id FROM attributes
             WHERE attributes_parent = :parent AND lower(attributes_model) = lower(:model)
             LIMIT 1'
        );
        $stmt->execute([':parent' => $parentId, ':model' => $model]);
        $id = $stmt->fetchColumn();
        return $id ? (int)$id : 0;
    }

    private function loadExisting(): void
    {
        if ($this->loaded) {
            return;
        }
        $this->loaded = true;
        $stmt = $this->pdo->query('SELECT attributes_id, attributes_parent, attributes_model FROM attributes');
        $rows = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
        foreach ($rows as $row) {
            $id = (int)($row['attributes_id'] ?? 0);
            $parent = (int)($row['attributes_parent'] ?? 0);
            $model = (string)($row['attributes_model'] ?? '');
            if ($model === '') {
                continue;
            }
            if ($parent === 0) {
                $this->parentMap[strtolower($model)] = $id;
            } else {
                $this->childMap[$parent . '|' . strtolower($model)] = $id;
            }
        }
    }
}
