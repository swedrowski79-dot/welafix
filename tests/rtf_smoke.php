<?php
declare(strict_types=1);

require __DIR__ . '/../app/src/Bootstrap/autoload.php';

$tests = [
    [
        'name' => 'rtf-basic',
        'input' => "{\\rtf1\\ansi E-Stutzen \\'e4\\par Zeile2}",
        'expect_contains' => 'E-Stutzen ä<br>Zeile2',
    ],
    [
        'name' => 'plaintext',
        'input' => "Line 1\nLine 2 & <tag>",
        'expect_contains' => 'Line 1<br />Line 2 &amp; &lt;tag&gt;',
    ],
    [
        'name' => 'rtf-umlaut',
        'input' => "{\\rtf1\\ansi verzinkt \\'fcberzug}",
        'expect_contains' => 'verzinkt überzug',
    ],
];

foreach ($tests as $idx => $t) {
    $out = rtfToHtmlSimple($t['input']);
    $ok = strpos($out, $t['expect_contains']) !== false;
    echo 'Test ' . ($idx + 1) . ' (' . $t['name'] . '): ' . ($ok ? 'OK' : 'FAIL') . "\n";
    if (!$ok) {
        echo 'Expected contains: ' . $t['expect_contains'] . "\n";
        echo 'Actual: ' . $out . "\n";
    }
}
