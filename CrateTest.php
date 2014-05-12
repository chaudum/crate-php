<?php

include_once('Crate.php');

$hosts = array('127.0.0.1:4200', '127.0.0.1:4201', '127.0.0.1:4202');

$conn = connect($hosts);
$c = $conn->cursor();

$c->execute('CREATE TABLE "userinfo" ("username" string, "info" string)');
$result = $c->fetchone();
var_dump($result);

$c->execute('INSERT INTO "userinfo" ("username", "info") values (\'christian\',\'beer\')');
$result = $c->fetchone();
var_dump($result);

$c->execute('INSERT INTO "userinfo" ("username", "info") values (\'christian\',\'club mate\')');
$result = $c->fetchone();
var_dump($result);

$c->execute('INSERT INTO "userinfo" ("username", "info") values (\'christian\',\'dubstep\')');
$result = $c->fetchone();
var_dump($result);

sleep(1);

$c->execute('SELECT * from "userinfo"');
$result = $c->fetchall();
var_dump($result);

$c->execute('DROP TABLE "userinfo"');

$conn->close();

?>
