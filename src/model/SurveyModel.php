<?php

require_once './requestModel.php';

class SurveyRequestModel extends RequestModel {
    /** @var boolean */
    public $forceReplace;
    /** @var integer */
    public $providerID;
    /** @var string */
    public $sourceCode;
    /** @var mixed */
    public $meta;
    /** @var SurveyRequestModelDataSet[] */
    public $dataSet;

    public function __construct() {
        parent::__construct();
    }
}

class SurveyRequestModelDataSet {
    /** @var integer */
    public $organizationNumber;
    /** @var DateTime */
    public $dateAnswered;
    /** @var AnswerModel[] */
    public $answers;
}

class AnswerModel {
    /** @var string */
    public $question;
    /** @var integer|string*/
    public $answer;
}