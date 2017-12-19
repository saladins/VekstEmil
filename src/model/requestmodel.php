<?php
class RequestModel {
    /** @var integer */
    public $requestType;
    /** @var integer */
    public $variableID;
    /** @var string */
    public $tableName;
    /** @var  string[][] */
    public $constraints;

    public function __construct() {
        $this->requestType = RequestMap::Unknown;
        $this->variableID = -1;
        $this->tableName = '';
        $this->constraints = [];
    }
}
