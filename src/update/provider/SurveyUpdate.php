<?php
include __DIR__ . '/../helpers/CoreMethods.php';

class SurveyUpdate {
    /** @var Logger */
    private $logger;
    /** @var CoreMethods  */
    private $core;
    /** @var DatabaseHandler  */
    private $db;

    public function __construct($db, $logger) {
        $this->db = $db;
        $this->logger = $logger;
        $this->core = new CoreMethods($db, $logger);
    }

    function updateTable($request) {
        $startTime = $this->logger->microTimeFloat();
        try {
            $this->insertSurvey($request->dataSet);
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

    private function insertSurvey($dataSet) {
        $companies = $this->getCompanyIds();
        $questions = $this->getQuestions();
        $this->db->beginTransaction();
        try {
            $this->logger->log(print_r($dataSet, true));
            foreach ($dataSet as $company) {

            }
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;;
        }
    }

    private function getCompanyIds() {
        $sql = 'SELECT enterpriseID, organizationNumber FROM Enterprise';
        $this->db->query($sql);
        return $this->db->getResultSet(PDO::FETCH_NUM);
    }

    private function getQuestions() {
        $sql = 'SELECT questionID, questionText FROM Survey_Question';
        $this->db->query($sql);
        return $this->db->getResultSet(PDO::FETCH_NUM);
    }
}