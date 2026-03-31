<?php
declare(strict_types=1);

namespace Welafix\Domain\Sync;

use Welafix\Database\ConnectionFactory;
use Welafix\Domain\Afs\AfsUpdateResetService;
use Welafix\Domain\Afs\AfsVisibilityReconcileService;
use Welafix\Domain\Artikel\ArtikelSyncService;
use Welafix\Domain\Dokument\DokumentSyncService;
use Welafix\Domain\Export\TemplateExportService;
use Welafix\Domain\FileDb\FileDbTemplateApplier;
use Welafix\Domain\Media\MediaSyncService;
use Welafix\Domain\Meta\MetaFillService;
use Welafix\Domain\Warengruppe\WarengruppeSyncService;
use Welafix\Domain\Xt\XtApiApplyService;
use Welafix\Domain\Xt\XtMappingSyncService;

final class DailyDeltaSyncService
{
    public function __construct(private ConnectionFactory $factory) {}

    /**
     * @return array<string, mixed>
     */
    public function run(int $artikelBatchSize = 500): array
    {
        $stats = [
            'ok' => true,
            'steps' => [],
        ];

        $start = microtime(true);
        $pdo = $this->factory->sqlite();
        $pdo->exec('PRAGMA busy_timeout = 5000');
        $applier = new FileDbTemplateApplier();
        $fileDbStats = [
            'ok' => true,
            'filedb_mode' => strtolower((string)env('FILEDB_MODE', 'read')),
        ];
        $pdo->beginTransaction();
        try {
            $pdo->exec('DELETE FROM artikel_extra_data');
            $pdo->exec('DELETE FROM warengruppe_extra_data');
            $fileDbStats['artikel'] = $applier->importArtikelDirectories($pdo);
            $fileDbStats['warengruppe'] = $applier->importWarengruppeDirectories($pdo);
            $pdo->commit();
        } catch (\Throwable $e) {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            throw $e;
        }
        $this->markSourcesChangedAfterFileDb($pdo, $fileDbStats);
        $fileDbStats['duration_ms'] = (int)round((microtime(true) - $start) * 1000);
        $stats['steps']['filedb'] = $fileDbStats;

        $templateExport = new TemplateExportService();

        $start = microtime(true);
        $warengruppe = new WarengruppeSyncService($this->factory);
        $stats['steps']['warengruppe'] = $warengruppe->runImportAndBuildPaths();
        try {
            $templateExport->exportWarengruppeTemplates();
        } catch (\Throwable $e) {
            $stats['steps']['warengruppe']['template_export_error'] = $e->getMessage();
        }
        $stats['steps']['warengruppe']['duration_ms'] = (int)round((microtime(true) - $start) * 1000);

        $start = microtime(true);
        $artikel = new ArtikelSyncService($this->factory);
        $after = '';
        $loops = 0;
        do {
            $artikelStats = $artikel->processBatch($after, $artikelBatchSize);
            $after = (string)($artikelStats['last_key'] ?? '');
            $loops++;
        } while (empty($artikelStats['done']));
        $artikelStats['loops'] = $loops;
        try {
            $templateExport->exportArtikelTemplates();
        } catch (\Throwable $e) {
            $artikelStats['template_export_error'] = $e->getMessage();
        }
        $artikelStats['duration_ms'] = (int)round((microtime(true) - $start) * 1000);
        $stats['steps']['artikel'] = $artikelStats;

        $start = microtime(true);
        $dokument = new DokumentSyncService();
        $stats['steps']['dokument'] = $dokument->run();
        $stats['steps']['dokument']['duration_ms'] = (int)round((microtime(true) - $start) * 1000);

        $start = microtime(true);
        $media = new MediaSyncService($this->factory);
        $stats['steps']['media'] = $media->run();
        $stats['steps']['media']['duration_ms'] = (int)round((microtime(true) - $start) * 1000);

        $start = microtime(true);
        $meta = new MetaFillService($this->factory);
        $stats['steps']['meta'] = $meta->run();
        $stats['steps']['meta']['duration_ms'] = (int)round((microtime(true) - $start) * 1000);

        $start = microtime(true);
        $reconcile = new AfsVisibilityReconcileService($this->factory);
        $stats['steps']['reconcile_deletes'] = $reconcile->run();
        $stats['steps']['reconcile_deletes']['duration_ms'] = (int)round((microtime(true) - $start) * 1000);

        $start = microtime(true);
        $xtMapping = new XtMappingSyncService();
        $stats['steps']['xt_mapping'] = $xtMapping->run('welafix_xt');
        $stats['steps']['xt_mapping']['duration_ms'] = (int)round((microtime(true) - $start) * 1000);

        $start = microtime(true);
        $xtApply = new XtApiApplyService($this->factory);
        $stats['steps']['xt_apply'] = $xtApply->run('welafix_xt');
        $stats['steps']['xt_apply']['duration_ms'] = (int)round((microtime(true) - $start) * 1000);

        $start = microtime(true);
        $reset = new AfsUpdateResetService($this->factory);
        $stats['steps']['reset'] = $reset->run();
        $stats['steps']['reset']['duration_ms'] = (int)round((microtime(true) - $start) * 1000);

        return $stats;
    }

    /**
     * @param array<string, mixed> $fileDbStats
     */
    private function markSourcesChangedAfterFileDb(\PDO $pdo, array $fileDbStats): void
    {
        $artikelImported = (int)($fileDbStats['artikel']['imported'] ?? 0);
        $warengruppeImported = (int)($fileDbStats['warengruppe']['imported'] ?? 0);

        if ($artikelImported > 0) {
            $pdo->exec('UPDATE artikel SET changed = 1 WHERE COALESCE(is_deleted, 0) = 0');
        }
        if ($warengruppeImported > 0) {
            $pdo->exec('UPDATE warengruppe SET changed = 1 WHERE COALESCE(is_deleted, 0) = 0');
        }
    }
}
