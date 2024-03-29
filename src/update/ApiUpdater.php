<?php
include_once 'helpers/CoreMethods.php';

/** Handles database updates */
class ApiUpdate {
    /** @var DatabaseHandler  */
    private $db;
    /** @var Logger  */
    private $logger;


    public function __construct() {
        $this->db = DatabaseHandlerFactory::getDatabaseHandler();
        $this->logger = new Logger();
    }

    /**
     * Public entry point for database updates.
     * Determines which provider the data is from.
     * @param InsertRequestModel $request
     * @return array|string
     */
    public function update($request) {
        //Extending max executing time
        ini_set('max_execution_time', 2600);

        // TODO Must ensure we use proper account for these operations
        switch ($request->providerID) {
            case 1:
                return $this->updateSSB($request);
            case 2:
                return $this->updateProff($request);
            case 3:
                return $this->updateSurvey($request);
            default:
                return ['{unhandled update source type}'];
        }
    }

    /**
     * Checks whether or not the variable exists in the Variable data table.
     * Handles SSB data only
     * @param InsertRequestModel $request
     * @return string
     */
    private function updateSSB($request) {
        $sql = 'SELECT variableID, tableName FROM Variable WHERE providerCode = :providerCode ';
        $this->db->prepare($sql);
        $this->db->bind(':providerCode', $request->providerCode);
        $res = $this->db->getSingleResult();
        if (!$res) {
           return 'No variable entry found for request';
        } else {
            $variableID = $res['variableID'];
            $tableName = $res['tableName'];
            if ($request->forceReplace) {
                $this->truncateTable($tableName);
            }
        }
        include 'provider/SsbUpdate.php';
        $ssbUpdater = new SsbUpdate($this->db, $this->logger);
        return $ssbUpdater->updateTable($request, $tableName, $variableID);
    }

    /**
     * @param InsertRequestModel $request
     * @return string
     */
    private function updateProff($request) {
        $sql = <<<SQL
SELECT variableID, tableName FROM Variable WHERE providerCode LIKE CONCAT(:providerCode, '%');
SQL;
        $this->db->prepare($sql);
        $this->db->bind(':providerCode', $request->providerCode);
        $res = $this->db->getSingleResult();
        if (!$res) {
            return 'No variable entry found for request';
        } else {
            $variableID = $res['variableID'];
            $tableName = $res['tableName'];
            if ($request->forceReplace) {
                try {
                    if ($tableName === 'Enterprise') {
                        $this->truncateTable('EnterpriseEntry');
                    }
                } catch (PDOException $ex) {
                    return 'Failed to delete from ' . $tableName . '. Reason given by database: ' . $ex->getMessage();
                }
            }
        }
        include 'provider/ProffUpdate.php';
        $xlsUpdate = new ProffUpdate($this->db, $this->logger);
        return $xlsUpdate->updateTable($request, $tableName, $variableID);
    }

    private function updateSurvey($request) {
//        if ($request->forceReplace) {
//            try {
//
//            } catch (PDOException $ex) {
//                return 'Failed to delete existing survey data. Reason given by database: ' . $ex->getMessage();
//            }
//        }
        include 'provider/SurveyUpdate.php';
        $surveyUpdate = new SurveyUpdate($this->db, $this->logger);
        return $surveyUpdate->updateTable($request);
    }

    /**
     * Removes table contents
     * @param string $tableName
     * @throws PDOException
     * @return void
     */
    private function truncateTable($tableName) {
        $this->db->beginTransaction();
        $sql = 'DELETE FROM ' . $tableName;
        $this->db->query($sql);
        $queryResult = $this->db->execute();
        $this->db->commit();
        if (Globals::debugging) {
            $this->logger->log('DB: Forcing replacement of table ' . $tableName);
            $this->logger->log('DB: Force request given by ' . $_SERVER['REMOTE_ADDR']);
            $this->logger->log('DB: Force delete request of table ' . $tableName . ' was a ' . ($queryResult ? 'success.' : 'failure.'));
        }
    }

}
