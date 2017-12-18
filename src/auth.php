<?php
require '../vendor/autoload.php';
include './server/index.php';

$auth = new Authenticate();
if (file_get_contents('php://input') != null) {
    $postContent = file_get_contents('php://input');
    if (json_decode($postContent)) {
        $decoded = json_decode($postContent);
        if (isset($decoded->usr) && isset($decoded->pw)) {

            return $auth->authenticate($decoded->usr, $decoded->pw);
        } else {
            http_response_code(401);
        }
    } else {
        http_response_code(401);
    }

} else {
    http_response_code(401);
}