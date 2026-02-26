<?php
declare(strict_types=1);

namespace Welafix\Domain\Media;

use Welafix\Database\ConnectionFactory;

final class MediaSyncService
{
    public function __construct(private ConnectionFactory $factory) {}

    /**
     * @return array<string, mixed>
     */
    public function run(): array
    {
        $importer = new MediaFilenameImporter($this->factory);
        $checker = new MediaStorageChecker($this->factory);

        $step1 = $importer->importFromAfs();
        $stepDocs = $importer->importFromDocuments();
        $step2 = $checker->check();

        return [
            'ok' => true,
            'filenames' => $step1,
            'documents' => $stepDocs,
            'storage_check' => $step2,
        ];
    }
}
