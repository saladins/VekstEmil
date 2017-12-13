<?php
include '../helpers/CoreMethods.php';
class XlsUpdater {
    /** @var Logger */
    private $logger;
    /** @var CoreMethods  */
    private $core;
    /** @var DatabaseHandler  */
    private $db;

    /**
     * XlsUpdater constructor.
     * @param DatabaseHandler $db
     * @param Logger $logger
     */
    public function __construct($db, $logger) {
        $this->db = $db;
        $this->logger = $logger;
        $this->core = new CoreMethods($db, $logger);
    }

    /**
     * Invokes correct database table update method based on table name.
     * @param $request
     * @param string $tableName
     * @param integer $variableID
     * @return string
     */
    function updateTable($request, $tableName, $variableID) {
        $startTime = $this->logger->microTimeFloat();
        try {
            switch ($tableName) {
                case 'Enterprise':
                    $this->insertEnterprise($request->dataSet, $variableID);
                    break;
                default:
                    return 'Unhandled variable type';
            }
            $date = new DateTime();
            $this->core->setLastUpdatedTime($variableID, $date->getTimestamp());
            $message = 'Successfully updated: ' . $tableName . '. Elapsed time: ' . date('i:s:u', (integer)($this->logger->microTimeFloat() - $startTime));
            return $message;
        } catch (PDOException $PDOException) {
            $message = 'PDO error when performing database write on ' . $tableName . ': '
                . $PDOException->getMessage() . ' '
                . $PDOException->getTraceAsString();
            $this->logger->log($message);
            return '{'.$message.'}';
        }
    }

    /**
     * @param $dataSet
     * @param integer $variableID
     * @return bool|PDOException
     */
    private function insertEnterprise($dataSet, $variableID) {
        set_time_limit(120);
        $this->db->beginTransaction();
        try {
            foreach ($dataSet as $enterprise) {
                $sqlGetEnterpriseID = 'SELECT enterpriseID FROM Enterprise WHERE organizationNumber = :organizationNumber;';
                $this->db->prepare($sqlGetEnterpriseID);
                $this->db->bind(':organizationNumber', $enterprise->organizationNumber);
                $res = $this->db->getSingleResult();
                if ($res) {
                    $enterpriseID = $res['enterpriseID'];
                } else {
                    $municipalityID = $this->core->getMunicipalityID($this->core->getMunicipalityRegionCode($enterprise->municipalityName));
                    $naceID = $this->core->getNaceID($enterprise->nace);
                    $organizationTypeID = $this->core->getOrganizationTypeID($enterprise->organizationType);
                    $employees = (sizeof($enterprise->employees) > 0 ? $enterprise->employees : null);
                    $name = $this->db->quote($enterprise->name);
                    if (strlen($name) > 63) {
                        $name = substr($name, 0, 63);
                    }
                    $organizationNumber = $enterprise->organizationNumber;
                    $sql = <<<SQL
INSERT INTO Enterprise (variableID, municipalityID, naceID, organizationTypeID, employees, enterpriseName, organizationNumber)
VALUES (:variableID, :municipalityID, :naceID, :organizationTypeID, :employees, :businessName, :organizationNumber);
SQL;
                    $this->db->prepare($sql);
                    $this->db->bind(':variableID', $variableID);
                    $this->db->bind(':municipalityID', $municipalityID);
                    $this->db->bind(':naceID', $naceID);
                    $this->db->bind(':employees', $employees);
                    $this->db->bind(':businessName', $name);
                    $this->db->bind(':organizationNumber', $organizationNumber);
                    $this->db->bind(':organizationTypeID', $organizationTypeID);
                    $this->db->execute();
                    $enterpriseID = $this->db->getLastInsertID();
                }
                $sqlInsertEntry = "INSERT INTO EnterpriseEntry (enterpriseID, enterprisePostCategoryID, pYear, valueInNOK) 
                             VALUES (:enterpriseID, :enterprisePostCategoryID, :pYear, :valueInNOK)";
                foreach ($enterprise->entry as $entry) {
                    $enterprisePostCategoryID = $this->core->getEnterprisePostCategory($entry->type);
                    $pYear = $entry->year;
                    $value = ($entry->value == null ? 0 : $entry->value);
                    $this->db->prepare($sqlInsertEntry);
                    $this->db->bind(':enterpriseID', $enterpriseID);
                    $this->db->bind(':enterprisePostCategoryID', $enterprisePostCategoryID);
                    $this->db->bind(':pYear', $pYear);
                    $this->db->bind(':valueInNOK', $value);
                    $this->db->execute();
                }
            }
            return $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }
}