<?php
declare(strict_types=1);

require __DIR__ . '/../app/src/Bootstrap/autoload.php';

function buildSeoUrl(string $lang, array $categoryNames, string $productName): string
{
    $segments = [];
    foreach ($categoryNames as $categoryName) {
        $slug = xt_filterAutoUrlText_inline($categoryName, $lang);
        $slug = strtolower($slug);
        if ($slug !== '') {
            $segments[] = $slug;
        }
    }
    $prodSlug = xt_filterAutoUrlText_inline($productName, $lang, '-', 'product', '0');
    $prodSlug = strtolower($prodSlug);
    $path = implode('/', $segments);
    if ($path !== '') {
        $path = $path . '/' . $prodSlug;
    } else {
        $path = $prodSlug;
    }
    return rtrim($lang, '/') . '/' . ltrim($path, '/');
}

$tests = [
    [
        'lang' => 'de',
        'categories' => ['TV & HiFi','Zubehör & Kabel'],
        'product' => 'HDMI Kabel 2m (Gold) – Übertragungs-Set',
        'expected' => 'de/tv-hifi/zubehoer-kabel/hdmi-kabel-2m-gold-uebertragungs-set',
    ],
    [
        'lang' => 'de',
        'categories' => ['Überwachung & Alarm','Außen-Kamera'],
        'product' => 'Kamera – Größe L',
        'expected' => 'de/ueberwachung-alarm/aussen-kamera/kamera-groesse-l',
    ],
];

foreach ($tests as $idx => $t) {
    $actual = buildSeoUrl($t['lang'], $t['categories'], $t['product']);
    $ok = $actual === $t['expected'];
    echo 'Test ' . ($idx + 1) . ': ' . ($ok ? 'OK' : 'FAIL') . "\n";
    if (!$ok) {
        echo 'Expected: ' . $t['expected'] . "\n";
        echo 'Actual:   ' . $actual . "\n";
    }
}
