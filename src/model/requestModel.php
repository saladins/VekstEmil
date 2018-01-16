<?php
class RequestModel {
    /** @var integer */
    public $requestType;
    /** @var integer */
    public $variableID;
    /** @var string */
    public $tableName;
    /** @var string */
    public $searchTerm;
    /** @var  string[][] */
    public $constraints;

    public function __construct() {
        $this->requestType = RequestType::Unknown;
        $this->variableID = -1;
        $this->tableName = '';
        $this->searchTerm = '';
        $this->constraints = [];
    }
}

class InsertRequestModel extends RequestModel {
    /** @var integer */
    public $providerID;
    /** @var string */
    public $providerCode;
    /** @var boolean */
    public $forceReplace;
    /** @var string */
    public $sourceCode;
    public function __construct() {
        parent::__construct();
    }
}
