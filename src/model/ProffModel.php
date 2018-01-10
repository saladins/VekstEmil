<?php
include_once 'requestModel.php';

class ProffModel extends InsertRequestModel {
    /** @var string */
    public $sourceCode;
    /** @var mixed */
    public $meta;
    /** @var ProffRequestModelDataSet[] */
    public $dataSet;

    public function __construct() {
        parent::__construct();
    }
}

class ProffRequestModelDataSet {
    /** @var integer */
    public $organizationNumber;
    /** @var string */
    public $municipalityName;
    /** @var string */
    public $nace;
    /** @var string */
    public $organizationType;
    /** @var integer */
    public $employees;
    /** @var string */
    public $name;
    /** @var Entry[] */
    public $entry;
}
//
class Entry {
    /** @var string  */
    public $type;
    /** @var integer */
    public $year;
    /** @var double */
    public $value;

}