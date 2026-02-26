<?php
declare(strict_types=1);

require __DIR__ . '/../app/src/Bootstrap/autoload.php';

$tests = [
    [
        'input' => 'C:\\x\\y\\pic.JPG',
        'expected' => 'pic.JPG',
    ],
    [
        'input' => '/img/pic.jpg?v=123',
        'expected' => 'pic.jpg',
    ],
    [
        'input' => '0',
        'expected' => null,
    ],
    [
        'input' => 'NULL',
        'expected' => null,
    ],
];

foreach ($tests as $idx => $t) {
    $actual = normalizeMediaFilename($t['input']);
    $ok = $actual === $t['expected'];
    echo 'Test ' . ($idx + 1) . ': ' . ($ok ? 'OK' : 'FAIL') . "\n";
    if (!$ok) {
        echo 'Expected: ' . var_export($t['expected'], true) . "\n";
        echo 'Actual:   ' . var_export($actual, true) . "\n";
    }
}
