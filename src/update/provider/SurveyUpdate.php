<?php
include __DIR__ . '/../helpers/CoreMethods.php';
include __DIR__ . '/../../model/SurveyModel.php';

class SurveyUpdate {
    /** @var Logger */
    private $logger;
    /** @var CoreMethods  */
    private $core;
    /** @var DatabaseHandler  */
    private $db;

    private $companies  = [];
    private $questions  = [];

    public function __construct($db, $logger) {
        $this->db = $db;
        $this->logger = $logger;
        $this->core = new CoreMethods($db, $logger);
        $this->companies = $this->fetchCompaniesFromDb();
        $this->questions = $this->fetchQuestionsFromDb();
    }

    /**
     * @param SurveyRequestModel $request
     * @return string
     */
    function updateTable($request) {
        $startTime = $this->logger->microTimeFloat();
        try {
            $surveyID = $this->getSurveyID($request->meta->startDate, $request->meta->endDate, $request->meta->title);
            $this->createSurveyData($request->dataSet, $surveyID);
            $date = new DateTime();
//            $this->core->setLastUpdatedTime($variableID, $date->getTimestamp());
            $message = 'Successfully updated: Survey data. Elapsed time: ' . date('i:s:u', (integer)($this->logger->microTimeFloat() - $startTime));
            return $message;
        } catch (PDOException $PDOException) {
            $message = 'PDO error when performing database write on Survey data: '
                . $PDOException->getMessage() . ' '
                . $PDOException->getTraceAsString();
            $this->logger->log($message);
            return '{'.$message.'}';
        }
    }

    /**
     * @param SurveyRequestModelDataSet[] $dataSet
     * @param integer $surveyID
     * @return Exception|PDOException
     */
    private function createSurveyData($dataSet, $surveyID) {
//        $this->db->beginTransaction();
        try {
            $this->logger->log(print_r($dataSet, true));
            /** @var SurveyRequestModelDataSet $item */
            foreach ($dataSet as $item) {
                $currentCompanyID = $this->getCompanyID($item->organizationNumber);
                if (is_null($currentCompanyID)) {
                    // TODO current company not in main list
                }
                foreach ($item->answers as $answer) {
                    $questionID = $this->getQuestionIdByText($answer->question);
                    $this->linkSurveyIdAndQuestionIdOrFailSilently($surveyID, $questionID);
                    $givenAnswerID = $this->insertAnswerToGivenQuestion($surveyID, $questionID, $answer->answer);
                    if (!is_null($currentCompanyID)) {
                        // TODO remove me once we've previously made sure there always is a company ID. Either we get it from the DB or we create it.
                        $this->insertSurveyAnswerForCompany($surveyID, $questionID, $givenAnswerID, $currentCompanyID);
                    }
                }

            }
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;;
        }
    }

    /**
     * @param integer $surveyID
     * @param integer $questionID
     * @param integer $givenAnswerID
     * @param integer $companyID
     */
    private function insertSurveyAnswerForCompany($surveyID, $questionID, $givenAnswerID, $companyID) {
        $sql = 'INSERT INTO Survey_Answer (surveyID, questionID, givenAnswerID, companyID) VALUES (:surveyID, :questionID, :givenAnswerID, :companyID)';
        $this->db->prepare($sql);
        $this->db->bind(':surveyID', $surveyID);
        $this->db->bind(':questionID', $questionID);
        $this->db->bind(':givenAnswerID', $givenAnswerID);
        $this->db->bind(':companyID', $companyID);
        $this->db->execute();
    }

    /**
     * @param integer $surveyID
     * @param integer $questionID
     * @param string $answer
     * @return integer
     */
    private function insertAnswerToGivenQuestion($surveyID, $questionID, $answer) {
        $sql = 'INSERT INTO Survey_GivenAnswer (answerText) VALUE (:answer)';
        $this->db->prepare($sql);
        $this->db->bind(':answer', $answer);
        $this->db->execute();
        $givenAnswerID = $this->db->getLastInsertID();
        $sql2 = 'INSERT INTO SurveyQuestionAnswer (surveyID, questionID, givenAnswerID) VALUES (:surveyID, :questionID, :givenAnswerID)';
        $this->db->prepare($sql2);
        $this->db->bind(':surveyID', $surveyID);
        $this->db->bind(':questionID', $questionID);
        $this->db->bind(':givenAnswerID', $givenAnswerID);
        $this->db->execute();
        return $givenAnswerID;
    }

    /**
     * @param $surveyID
     * @param $questionID
     */
    private function linkSurveyIdAndQuestionIdOrFailSilently($surveyID, $questionID) {
        $sql = 'INSERT INTO Survey_SurveyQuestion (surveyID, questionID) VALUES (:surveyID, :questionID)';
        try {
            $this->db->prepare($sql);
            $this->db->bind(':surveyID', $surveyID);
            $this->db->bind(':questionID', $questionID);
            $this->db->execute();
        } catch (PDOException $ex) {
            return;
        }
    }

    /**
     * @param string $questionText
     * @return integer
     */
    private function getQuestionIdByText($questionText) {
        foreach ($this->questions as $key => $question) {
            if (strpos($question, $questionText) !== false) {
                return $key;
            }
        }
        $sql = 'INSERT INTO Survey_Question (questionText) VALUES (:questionText)';
        $this->db->prepare($sql);
        $this->db->bind(':questionText', $questionText);
        $this->db->execute();
        $id = $this->db->getLastInsertID();
        $this->questions[$id] = $questionText;
        return $id;
    }

    /**
     * @param integer $organizationNumber
     * @return integer|null
     */
    private function getCompanyID($organizationNumber) {
        foreach ($this->companies as $key => $company) {
            if ($company === $organizationNumber) {
                return $key;
            }
        }
        return null;
    }

    /**
     * @param $startDate
     * @param $endDate
     * @param $description
     * @return integer
     */
    private function getSurveyID($startDate, $endDate, $description) {
        $sql = 'SELECT surveyID FROM Survey WHERE startDate = :startDate AND endDate = :endDate';
        $this->db->prepare($sql);
        $this->db->bind(':startDate', $startDate);
        $this->db->bind(':endDate', $endDate);
        $resultSet = $this->db->getResultSet();
        if (!isset($resultSet['surveyID'])) {
           return $this->insertSurvey($startDate, $endDate, $description);
        } else {
            return $resultSet['surveyID'];
        }

    }

    /**
     * @param $startDate
     * @param $endDate
     * @param $description
     * @return integer
     */
    private function insertSurvey($startDate, $endDate, $description) {
        $sql = 'INSERT INTO SURVEY (description, startDate, endDate) VALUES (:description, :startDate, :endDate)';
        $this->db->prepare($sql);
        $this->db->bind(':description', $description);
        $this->db->bind('startDate', $startDate);
        $this->db->bind(':endDate', $endDate);
        $this->db->execute();
        return $this->db->getLastInsertID();
    }

    /**
     * @return array
     */
    private function fetchCompaniesFromDb() {
        $sql = 'SELECT enterpriseID, organizationNumber FROM Enterprise';
        $this->db->query($sql);
        $resultSet = $this->db->getResultSet();
        $temp = [];
        foreach ($resultSet as $item) {
            $temp[$item['enterpriseID']] = $item['organizationNumber'];
        }
        return $temp;
    }

    /**
     * @return array
     */
    private function fetchQuestionsFromDb() {
        $sql = 'SELECT questionID, questionText FROM Survey_Question';
        $this->db->query($sql);
        $resultSet = $this->db->getResultSet();
        $temp = [];
        foreach ($resultSet as $item) {
            $temp[$item['questionID']] = $item['questionText'];
        }
        return $temp;
    }
}