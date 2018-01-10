<?php

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
            $surveyID = $this->getSurveyID($request->meta->startDate, $request->meta->endDate);
            if ($request->forceReplace) {
                $this->removeOldData($surveyID);
            }
            if ($surveyID === null) {
                $surveyID = $this->insertSurvey($request->meta->startDate, $request->meta->endDate, $request->meta->title);
            }
            $this->createSurveyData($request->dataSet, $surveyID);
//            $date = new DateTime();
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
     * @throws PDOException
     */
    private function createSurveyData($dataSet, $surveyID) {
        $this->db->beginTransaction();
        try {
            /** @var SurveyRequestModelDataSet $item */
            foreach ($dataSet as $item) {
                $currentEnterpriseID = $this->getCompanyID($item->organizationNumber);
                if (is_null($currentEnterpriseID)) {
                    if (Globals::debugging) {
                        $this->logger->log('Warning: Unable to find enterprise ID for ' . $item->organizationNumber);
                    }
                    // TODO current company not in main list
                }
                foreach ($item->answers as $answer) {
                    $questionID = $this->getQuestionIdByText($answer->question);
                    $this->linkSurveyIdAndQuestionIdOrFailSilently($surveyID, $questionID);
                    $givenAnswerID = $this->insertAnswerToGivenQuestion($surveyID, $questionID, $answer->answer);
                    if (!is_null($currentEnterpriseID)) {
                        // TODO remove me once we've previously made sure there always is a company ID. Either we get it from the DB or we create it.
                        $this->insertSurveyAnswerForCompany($surveyID, $questionID, $givenAnswerID, $currentEnterpriseID);
                    }
                }
            }
            $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            throw $ex;
        }
    }

    private function removeOldData($surveyID) {
        $removeSurvey_Answer = 'DELETE FROM Survey_Answer WHERE surveyID = :surveyID';
        $this->db->prepare($removeSurvey_Answer);
        $this->db->bind(':surveyID', $surveyID);
        $this->db->execute();
        $removeSurveyQuestionAnswer = 'DELETE FROM SurveyQuestionAnswer WHERE surveyID = :surveyID';
        $this->db->prepare($removeSurveyQuestionAnswer);
        $this->db->bind(':surveyID', $surveyID);
        $this->db->execute();
        $removeSurvey_SurveyQuestion = 'DELETE FROM Survey_SurveyQuestion WHERE surveyID = :surveyID';
        $this->db->prepare($removeSurvey_SurveyQuestion);
        $this->db->bind(':surveyID', $surveyID);
        $this->db->execute();
        $removeSurvey = 'DELETE FROM Survey WHERE surveyID = :surveyID';
        $this->db->prepare($removeSurvey);
        $this->db->bind(':surveyID', $surveyID);
        $this->db->execute();
        if (Globals::debugging) {
            $this->logger->log('DB: Removed survey data for ID ' . $surveyID);
            $this->logger->log('DB: Remove request given by ' . $_SERVER['REMOTE_ADDR']);
        }
    }

    /**
     * @param integer $surveyID
     * @param integer $questionID
     * @param integer $givenAnswerID
     * @param integer $enterpriseID
     * @throws PDOException
     */
    private function insertSurveyAnswerForCompany($surveyID, $questionID, $givenAnswerID, $enterpriseID) {
        $sql = 'INSERT INTO Survey_Answer (surveyID, questionID, givenAnswerID, enterpriseID) VALUES (:surveyID, :questionID, :givenAnswerID, :enterpriseID)';
        $this->db->prepare($sql);
        $this->db->bind(':surveyID', $surveyID);
        $this->db->bind(':questionID', $questionID);
        $this->db->bind(':givenAnswerID', $givenAnswerID);
        $this->db->bind(':enterpriseID', $enterpriseID);
        $this->db->execute();
    }

    /**
     * @param integer $surveyID
     * @param integer $questionID
     * @param string $answer
     * @return integer
     * @throws PDOException
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
     * @throws PDOException
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
     * @throws PDOException
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
     * @return integer
     * @throws PDOException
     */
    private function getSurveyID($startDate, $endDate) {
        $sql = 'SELECT surveyID FROM Survey WHERE startDate = :startDate AND endDate = :endDate';
        $this->db->prepare($sql);
        $this->db->bind(':startDate', $startDate);
        $this->db->bind(':endDate', $endDate);
        $resultSet = $this->db->getSingleResult();
        return $resultSet['surveyID'];
    }

    /**
     * @param $startDate
     * @param $endDate
     * @param $description
     * @return integer
     * @throws PDOException
     */
    private function insertSurvey($startDate, $endDate, $description) {
        $sql = 'INSERT INTO SURVEY (description, startDate, endDate) VALUES (:title, :startDate, :endDate)';
        $this->db->prepare($sql);
        $this->db->bind(':title', $description);
        $this->db->bind('startDate', $startDate);
        $this->db->bind(':endDate', $endDate);
        $this->db->execute();
        return $this->db->getLastInsertID();
    }

    /**
     * @return array
     * @throws PDOException
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