<?php
class RequestModel {
    /** @var string */
    public $requestType;
    /** @var integer */
    public $variableID;
    /** @var integer */
    public $tableNumber;
    /** @var string */
    public $tableName;
    /** @var  string[][] */
    public $constraints;

    public function __construct() {
        $this->requestType = '';
        $this->variableID = -1;
        $this->tableNumber = -1;
        $this->tableName = '';
        $this->constraints = [];
    }
}
