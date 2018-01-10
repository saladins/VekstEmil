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
        $this->requestType = RequestType::Unknown;
        $this->variableID = -1;
        $this->tableName = '';
        $this->constraints = [];
    }
}

class InsertRequestModel extends RequestModel {
    /** @var integer */
    public $providerID;
    public function __construct() {
        parent::__construct();
    }
}
