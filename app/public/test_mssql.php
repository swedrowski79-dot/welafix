<?php
$host = getenv('MSSQL_HOST');
$port = getenv('MSSQL_PORT') ?: '1433';
$db   = getenv('MSSQL_DB');
$user = getenv('MSSQL_USER');
$pass = getenv('MSSQL_PASS');

$encrypt = getenv('MSSQL_ENCRYPT') === 'true' ? 'yes' : 'no';
$trust   = getenv('MSSQL_TRUST_CERT') === 'true' ? 'yes' : 'no';

$dsn = "sqlsrv:Server={$host},{$port};Database={$db};Encrypt={$encrypt};TrustServerCertificate={$trust}";

try {
  $pdo = new PDO($dsn, $user, $pass, [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
  ]);
  $stmt = $pdo->query("SELECT TOP 5 name FROM sys.tables ORDER BY name");
  echo "<pre>OK\n";
  print_r($stmt->fetchAll(PDO::FETCH_ASSOC));
  echo "</pre>";
} catch (Throwable $e) {
  http_response_code(500);
  echo "<pre>FAIL\n".$e->getMessage()."\n</pre>";
}
