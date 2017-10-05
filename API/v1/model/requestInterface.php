<?php
class requestInterface {
    public $requestType;
    public $tableName;

    static function getReserved() {
        return ['requestType', 'tableNumber'];
    }
}
