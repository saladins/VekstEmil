<?php
include '../helpers/CoreMethods.php';
class SsbUpdate {
    /** @var Logger */
    private $logger;
    /** @var CoreMethods  */
    private $core;
    /** @var DatabaseHandler  */
    private $db;

    /**
     * XlsUpdate constructor.
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
        if (isset($request->dataSet[0]->Alder) && $request->dataSet[0]->Alder != null) {
            $request->dataSet = $this->core->mapAgeAndReplace($request->dataSet);
        }
        try {
            switch ($tableName) {
                case 'PopulationChange':
                    $this->insertPopulationChange($request->dataSet, $variableID);
                    break;
                case 'Movement':
                    $this->insertMovement($request->dataSet, $variableID);
                    break;
                case 'Employment':
                    $this->insertEmployment($request->dataSet, $variableID);
                    break;
                case 'EmploymentDetailed':
                    $this->insertEmploymentDetailed($request->dataSet, $variableID);
                    break;
                case 'CommuteBalance':
                    $this->insertCommuteBalance($request->dataSet, $variableID);
                    break;
                case 'Unemployment':
                    $this->insertUnemployment($request->dataSet, $variableID);
                    break;
                case 'HomeBuildingArea':
                case 'FunctionalBuildingArea':
                    $this->insertBuildingArea($request->dataSet, $tableName, $variableID);
                    break;
                case 'HouseholdIncome':
                    $this->insertHouseholdIncome($request->dataSet, $variableID);
                    break;
                case 'Proceeding':
                    $this->insertProceeding($request->dataSet, $variableID);
                    break;
                case 'Education':
                    $this->insertEducation($request->dataSet, $variableID);
                    break;
                case 'RegionalCooperation':
                    $this->insertRegionalCooperation($request->dataSet, $variableID);
                    break;
                case 'NewEnterprise':
                    $this->insertNewEnterprise($request->dataSet, $variableID);
                    break;
                case 'ClosedEnterprise':
                    $this->insertClosedEnterprise($request->dataSet, $variableID);
                    break;
                case 'EmploymentSector':
                    $this->insertEmploymentSector($request->dataSet, $variableID);
                    break;
                default:
                    $testTime = $this->logger->microTimeFloat();
                    $this->insertGeneric($request->dataSet, $tableName, $variableID);
                    $this->logger->log('Insert generic took ' . ($this->logger->microTimeFloat() - $testTime) . ' seconds');
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
//            $this->db->rollbackTransaction();
        }
    }

    /**
     * @param $dataSet
     * @param $variableID
     * @return bool|PDOException
     */
    private function insertEmploymentSector($dataSet, $variableID) {
        $sql = 'INSERT INTO EmploymentSector (variableID, municipalityID, naceID, sectorID, pYear, workplaceValue, livingplaceValue) 
                VALUES (:varID, :munID, :naceID, :sectorID, :pYear, :workplaceValue, :livingplaceValue);';
        $data = array();
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $municipalityID = $this->core->getMunicipalityID($item->Region);
                $naceID = $this->core->getNaceID($item->NACE2007);
                $sectorID = $this->core->getSectorID($item->Sektor710);
                $pYear = $item->Tid;
                if (!isset($data[$municipalityID])) {$data[$municipalityID] = []; }
                if (!isset($data[$municipalityID][$naceID])) {$data[$municipalityID][$naceID] = []; }
                if (!isset($data[$municipalityID][$naceID][$sectorID])) {$data[$municipalityID][$naceID][$sectorID] = []; }
                if (!isset($data[$municipalityID][$naceID][$sectorID][$pYear])) {$data[$municipalityID][$naceID][$sectorID][$pYear] = []; }
                if ($item->ContentsCode === 'SysselEtterBoste') {
                    $data[$municipalityID][$naceID][$sectorID][$pYear]['living'] = $item->value;
                } else {
                    $data[$municipalityID][$naceID][$sectorID][$pYear]['working'] = $item->value;
                }
                if (isset($data[$municipalityID][$naceID][$sectorID][$pYear]['living']) &&
                    isset($data[$municipalityID][$naceID][$sectorID][$pYear]['working'])) {
                    $living = $data[$municipalityID][$naceID][$sectorID][$pYear]['living'];
                    $working = $data[$municipalityID][$naceID][$sectorID][$pYear]['working'];

                    $this->db->prepare($sql);
                    $this->db->bind(':varID', $variableID);
                    $this->db->bind(':munID', $municipalityID);
                    $this->db->bind(':naceID', $naceID);
                    $this->db->bind(':sectorID', $sectorID);
                    $this->db->bind(':pYear', $pYear);
                    $this->db->bind(':workplaceValue', $working);
                    $this->db->bind(':livingplaceValue', $living);
                    $this->db->execute();
                }
            }
            return $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }

    }

    /**
     * Inserts into ClosedEnterprise table
     * @param $dataSet
     * @param integer $variableID
     * @return bool|PDOException
     */
    private function insertClosedEnterprise($dataSet, $variableID) {
        $sql = 'INSERT INTO ClosedEnterprise (variableID, municipalityID, naceID, pYear, closedEnterprises) 
                VALUES (:varID, :munID, :naceID, :pYear, :closedEnterprises);';
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $this->db->prepare($sql);
                $this->db->bind(':varID', $variableID);
                $this->db->bind(':munID', $this->core->getMunicipalityID($item->Region));
                $this->db->bind(':naceID', $this->core->getNaceID($item->NACE2007));
                $this->db->bind(':pYear', $item->Tid);
                $this->db->bind(':closedEnterprises', $item->value);
                $this->db->execute();
            }
            return $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }


    }

    /**
     * Inserts into NewEnterprise table
     * @param $dataSet
     * @param integer $variableID
     * @return bool|PDOException
     */
    private function insertNewEnterprise($dataSet, $variableID) {
        $enterpriseCategories = array();
        $sqlEnterpriseCategories = 'SELECT enterpriseCategoryID, enterpriseCategoryCodeNew FROM EnterpriseCategory';
        $this->db->query($sqlEnterpriseCategories);
        foreach ($this->db->getResultSet() as $result) {
            $enterpriseCategories[strval($result['enterpriseCategoryCodeNew'])] = $result['enterpriseCategoryID'];
        }
        $employeeCountRangeCodes = array();
        $sqlEmployeeCountRangeCodes = 'SELECT employeeCountRangeID, employeeCountRangeCode FROM EmployeeCountRange';
        $this->db->query($sqlEmployeeCountRangeCodes);
        foreach ($this->db->getResultSet() as $result) {
            $employeeCountRangeCodes[strval($result['employeeCountRangeCode'])] = $result['employeeCountRangeID'];
        }
        $sql = 'INSERT INTO NewEnterprise (variableID, municipalityID, enterpriseCategoryID, employeeCountRangeID, pYear, newEnterprises) 
                VALUES (:varID, :munID, :enterpriseCategoryID, :employeeCountRangeID, :pYear, :newEnterprises)';
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $this->db->prepare($sql);
                $this->db->bind(':varID', $variableID);
                $this->db->bind(':munID', $this->core->getMunicipalityID($item->Region));
                $this->db->bind(':enterpriseCategoryID', $enterpriseCategories[strval($item->NyregBed)]);
                $this->db->bind(':employeeCountRangeID', $employeeCountRangeCodes[strval($item->AntAnsatte)]);
                $this->db->bind(':pYear', $item->Tid);
                $this->db->bind(':newEnterprises', $item->value);
                $this->db->execute();
            }
            return $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    /**
     * Inserts into RegionalCooperation table
     * @param $dataSet
     * @param integer $variableID
     * @return bool|PDOException
     */
    private function insertRegionalCooperation($dataSet, $variableID) {
        $kostraCategories = array();
        $sqlKostraCategories = 'SELECT kostraCategoryID, kostraCategoryCode FROM KostraCategory';
        $this->db->query($sqlKostraCategories);
        foreach ($this->db->getResultSet() as $result) {
            $kostraCategories[strval($result['kostraCategoryCode'])] = $result['kostraCategoryID'];
        }
        $municipalExpenseCategories = array();
        $sqlMunicipalExpenseCategories = 'SELECT municipalExpenseCategoryID, municipalExpenseCategoryCode FROM MunicpalExpenseCategory';
        $this->db->query($sqlMunicipalExpenseCategories);
        foreach ($this->db->getResultSet() as $result) {
            $municipalExpenseCategories[strval($result['municipalExpenseCategoryCode'])] = $result['municipalExpenseCategoryID'];
        }
        $sql = <<<SQL
INSERT INTO RegionalCooperation (variableID, municipalityID, kostraCategoryID, municipalExpenseCategoryID, pYear, expense) VALUES 
(:variableID, :municipalityID, :kostraID, :muexID, :pYear, :val)
SQL;
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                if ($item->value == null) {continue; }
                $this->db->prepare($sql);
                $this->db->bind(':variableID', $variableID);
                $this->db->bind(':municipalityID', $this->core->getMunicipalityID($item->Region));
                $this->db->bind(':kostraID', $kostraCategories[strval($item->FunksjonKostra)]);
                $this->db->bind(':muexID', $municipalExpenseCategories[strval($item->ArtGruppe)]);
                $this->db->bind(':pYear', $item->Tid);
                $this->db->bind(':val', $item->value);
                $this->db->execute();
            }
            return $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    /**
     * Inserts into Education table
     * @param $dataSet
     * @param integer $variableID
     * @return bool|PDOException
     */
    private function insertEducation($dataSet, $variableID) {
        $grades = array();
        $sqlGrades = 'SELECT gradeID, gradeCode FROM Grade';
        $this->db->query($sqlGrades);
        foreach ($this->db->getResultSet() as $result) {
            $grades[strval($result['gradeCode'])] = $result['gradeID'];
        }
        $sql = 'INSERT INTO Education (variableID, municipalityID, genderID, gradeID, pYear, percentEducated) 
                VALUES (:varID, :munID, :genderID, :gradeID, :pYear, :percentEducated)';
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $this->db->prepare($sql);
                $this->db->bind(':varID', $variableID);
                $this->db->bind(':munID', $this->core->getMunicipalityID($item->Region));
                $this->db->bind(':genderID', $this->core->getGenderID($item->Kjonn));
                $this->db->bind(':gradeID', $grades[strval($item->Nivaa)]);
                $this->db->bind(':percentEducated', $item->value);
                $this->db->execute();
            }
            return $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    /**
     * Inserts data into Proceeding table
     * @param $dataSet
     * @param integer $variableID
     * @return bool|PDOException
     */
    private function insertProceeding($dataSet, $variableID) {
        $proceedingCategories = array();
        $applicationTypes = array();
        $sqlProceedingCategory = 'SELECT proceedingCategoryID, proceedingCode FROM ProceedingCategory';
        $this->db->query($sqlProceedingCategory);
        foreach ($this->db->getResultSet() as $result) {
            $proceedingCategories[strval($result['proceedingCode'])] = $result['proceedingCategoryID'];
        }
        $sqlApplicationType = 'SELECT applicationTypeID, applicationTypeCode FROM ApplicationType';
        $this->db->query($sqlApplicationType);
        foreach ($this->db->getResultSet() as $result) {
            $applicationTypes[strval($result['applicationTypeCode'])] = $result['applicationTypeID'];
        }
        $sql = 'INSERT INTO Proceeding (variableID, municipalityID, proceedingCategoryID, applicationTypeID, pYear, proceedingValue) 
                VALUES (:varID, :munID, :proceedingCategoryID, :applicationTypeID, :pYear, :proceedingValue)';
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $this->db->prepare($sql);
                $this->db->bind(':varID', $variableID);
                $this->db->bind(':munID', $this->core->getMunicipalityID($item->Region));
                $this->db->bind(':proceedingCategoryID', $proceedingCategories[strval($item->ContentsCode)]);
                $this->db->bind(':applicationTypeID', $applicationTypes[strval($item->RammevilkSoknad)]);
                $this->db->bind(':pYear', $item->Tid);
                $this->db->bind(':proceedingValue', $item->value);
                $this->db->execute();
            }
            return $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    /**
     * Inserts into HouseholdIncome
     * @param $dataSet
     * @param $variableID
     * @return bool|PDOException
     */
    private function insertHouseholdIncome($dataSet, $variableID) {
        $householdTypes = array();
        $sqlHouseholdType = 'SELECT householdTypeID, householdTypeCode FROM HouseholdType';
        $this->db->query($sqlHouseholdType);
        foreach($this->db->getResultSet() as $result) {
            $householdTypes[strval($result['householdTypeCode'])] = $result['householdTypeID'];
        }
        $sql = 'INSERT INTO HouseholdIncome (variableID, municipalityID, householdTypeID, pYear, householdIncomeAvg) 
                VALUES (:varID, :munID, :householdTypeID, :pYear, :householdIncomeAvg);';
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $this->db->prepare($sql);
                $this->db->bind(':varID', $variableID);
                $this->db->bind(':munID', $this->core->getMunicipalityID($item->Region));
                $this->db->bind(':householdTypeID', $householdTypes[strval($item->HusholdType)]);
                $this->db->bind(':pYear', $item->Tid);
                $this->db->bind(':householdIncomeAvg', $item->value);
                $this->db->execute();
            }
            return $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    /**
     * Inserts into both HomeBuildingArea and FunctionalBuildingArea tables
     * @param $dataSet
     * @param $tableName
     * @param $variableID
     * @return bool|PDOException
     */
    private function insertBuildingArea($dataSet, $tableName, $variableID) {
        $buildingStatusCodes = array();
        $sqlBuildingStatus = 'SELECT buildingStatusID, buildingStatusCode, buildingStatusText FROM BuildingStatus';
        $this->db->query($sqlBuildingStatus);
        foreach ($this->db->getResultSet() as $result) {
            $buildingStatusCodes[strval($result['buildingStatusCode'])] = $result['buildingStatusID'];
        }
        $buildingCategories = array();
        $sqlBuildingCategory = 'SELECT buildingCategoryID, buildingCategoryCode FROM BuildingCategory';
        $this->db->query($sqlBuildingCategory);
        foreach ($this->db->getResultSet() as $result) {
            $buildingCategories[strval($result['buildingCategoryCode'])] = strval($result['buildingCategoryID']);
        }
        $sql = /** @lang text Hack to stop invalid table error */
            'INSERT INTO ' . $tableName . ' (variableID, municipalityID, buildingStatusID, buildingCategoryID, pYear, pQuarter, buildingValue) VALUES 
        (:varID, :munID, :buildingStatusID, :buildingCategoryID, :pYear, :pQuarter, :buildingValue)';
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $pYear = substr($item->Tid, 0, 4);
                $pQuarter = substr($item->Tid, 5);
                $this->db->prepare($sql);
                $this->db->bind(':varID', $variableID);
                $this->db->bind(':munID', $this->core->getMunicipalityID($item->Region));
                $this->db->bind(':buildingStatusID', $buildingStatusCodes[strval($item->ContentsCode)]);
                $this->db->bind(':buildingCategoryID', $buildingCategories[strval($item->Byggeareal)]);
                $this->db->bind(':pYear', $pYear);
                $this->db->bind(':pQuarter', $pQuarter);
                $this->db->bind(':buildingValue', $item->value);
                $this->db->execute();
            }
            return $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    /**
     * Inserts into Unemployment table
     * @param $dataSet
     * @param $variableID
     * @return bool|PDOException
     */
    private function insertUnemployment($dataSet, $variableID) {
        $sql = <<<SQL
INSERT INTO Unemployment (variableID, municipalityID, ageRangeID, pYear, pMonth, unemploymentPercent)
VALUES (:varID, :munID, :ageRangeID, :pYear, :pMonth, :unemploymentPercent);
SQL;
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $pYear = substr($item->Tid, 0, 4);
                $pMonth = substr($item->Tid, 5);
                $this->db->prepare($sql);
                $this->db->bind(':varID', $variableID);
                $this->db->bind(':munID', $this->core->getMunicipalityID($item->Region));
                $this->db->bind(':ageRangeID', $this->core->getAgeRangeID($item->Alder));
                $this->db->bind(':pYear', $pYear);
                $this->db->bind(':pMonth', $pMonth);
                $this->db->bind(':unemploymentPercent', $item->value);
                $this->db->execute();
            }
            return $this->db->endTransaction();

        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    /**
     * Inserts into CommuteBalance table
     * @param $dataSet
     * @param $variableID
     * @return bool|PDOException
     */
    private function insertCommuteBalance($dataSet, $variableID) {
        $sql = 'INSERT INTO CommuteBalance (variableID, municipalityID, workingMunicipalityID, pYear, commuters)
                VALUES (:varID, :munID, :workMunID, :pYear, :commuters);';
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $this->db->prepare($sql);
                $this->db->bind(':varID', $variableID);
                $this->db->bind(':munID', $this->core->getMunicipalityID($item->Bokommuen));
                $this->db->bind(':workMunID', $this->core->getMunicipalityID($item->ArbstedKomm));
                $this->db->bind(':pYear', $item->Tid);
                $this->db->bind(':commuters', $item->value);
                $this->db->execute();
            }
            return $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }

    }

    /**
     * Inserts into Employment table
     * @param $dataSet
     * @param $variableID
     * @return bool|PDOException
     */
    private function insertEmployment($dataSet, $variableID) {
        $sql = <<<SQL
INSERT INTO Employment (variableID, municipalityID, naceID, genderID, pYear, workplaceValue, livingplaceValue, employmentBalance)
VALUES (:varID, :munID, :naceID, :genderID, :pYear, :workVal, :livVal, :balance);
SQL;
        $data = [];
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $municipalityID = $this->core->getMunicipalityID($item->Region);
                $naceID = $this->core->getNaceID($item->NACE2007);
                $genderID = $this->core->getGenderID($item->Kjonn);
                $ageRangeID = $this->core->getAgeRangeID($item->Alder);
                $pYear = $item->Tid;
                $value = $item->value;
                if (!isset($data[$municipalityID])) {$data[$municipalityID] = []; }
                if (!isset($data[$municipalityID][$naceID])) {$data[$municipalityID][$naceID] = []; }
                if (!isset($data[$municipalityID][$naceID][$genderID])) {$data[$municipalityID][$naceID][$genderID] = []; }
                if (!isset($data[$municipalityID][$naceID][$genderID][$ageRangeID])) {$data[$municipalityID][$naceID][$genderID][$ageRangeID] = []; }
                if (!isset($data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear])) {$data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear] = []; }
                if ($item->ContentsCode == 'Sysselsatte' || $item->ContentsCode == 'SysselBosted') {
                    $data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear]['living'] = $value;
                } else {
                    $data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear]['working'] = $value;
                }
                if (isset($data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear]['living']) &&
                    isset($data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear]['working'])) {
                    $balance =
                        $data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear]['living']
                        -
                        $data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear]['working'];
                    $this->db->prepare($sql);
                    $this->db->bind(':varID', $variableID);
                    $this->db->bind(':munID', $municipalityID);
                    $this->db->bind(':naceID', $naceID);
                    $this->db->bind(':genderID', $genderID);
                    $this->db->bind(':pYear', $pYear);
                    $this->db->bind(':workVal', $data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear]['working']);
                    $this->db->bind(':livVal', $data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear]['living']);
                    $this->db->bind(':balance', $balance);
                    $this->db->execute();
                }
            }
            return $this->db->endTransaction();

        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    /**
     * Inserts into EmpolymentDetailed
     * @param $dataSet
     * @param $variableID
     * @return bool|PDOException
     */
    private function insertEmploymentDetailed($dataSet, $variableID) {
        $sql = <<<SQL
INSERT INTO EmploymentDetailed (variableID, municipalityID, naceID, genderID, pYear, workplaceValue, livingplaceValue, employmentBalance)
VALUES (:variableID, :mun, :nace, :gend, :yr, :wo, :li, :ba)
SQL;
        $data = [];
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                if (!isset($data[$item->Region])) {
                    $data[$item->Region] = [];
                }
                if (!isset($data[$item->Region][$item->NACE2007])) {
                    $data[$item->Region][$item->NACE2007] = [];
                }
                if (!isset($data[$item->Region][$item->NACE2007][$item->Kjonn])) {
                    $data[$item->Region][$item->NACE2007][$item->Kjonn] = [];
                }
                if (!isset($data[$item->Region][$item->NACE2007][$item->Kjonn][$item->Tid])) {
                    $data[$item->Region][$item->NACE2007][$item->Kjonn][$item->Tid] = [];
                }
                if ($item->ContentsCode == 'SysselBosted') {
                    $data[$item->Region][$item->NACE2007][$item->Kjonn][$item->Tid]['living'] = $item->value;
                } else {
                    $data[$item->Region][$item->NACE2007][$item->Kjonn][$item->Tid]['working'] = $item->value;
                }
                if (isset($data[$item->Region][$item->NACE2007][$item->Kjonn][$item->Tid]['living']) &&
                    isset($data[$item->Region][$item->NACE2007][$item->Kjonn][$item->Tid]['working'])) {
                    $living = $data[$item->Region][$item->NACE2007][$item->Kjonn][$item->Tid]['living'];
                    $working = $data[$item->Region][$item->NACE2007][$item->Kjonn][$item->Tid]['working'];
                    $balance = $living - $working;
                    $this->db->prepare($sql);
                    $this->db->bind(':variableID', $variableID);
                    $this->db->bind(':mun', $this->core->getMunicipalityID($item->Region));
                    $this->db->bind(':nace', $this->core->getNaceID($item->NACE2007));
                    $this->db->bind(':gend', $this->core->getGenderID($item->Kjonn));
                    $this->db->bind(':yr', $item->Tid);
                    $this->db->bind(':wo', $working);
                    $this->db->bind(':li', $living);
                    $this->db->bind(':ba', $balance);
                    $this->db->execute();
                }
            }
            return $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    /**
     * Inserts into Movement table
     * @param $dataSet
     * @param $variableID
     * @return bool|PDOException
     */
    private function insertMovement($dataSet, $variableID) {
        $res = [];
        foreach ($dataSet as $item) {
            $res[$item->Tid][$this->core->getMunicipalityID(strval($item->Region))][$item->ContentsCode] = $item->value;
        }
        $sql = 'INSERT INTO Movement (variableID, municipalityID, pYear, incomingAll, outgoingAll, sumAll) VALUES (:varID, :munID, :pYear, :incoming, :outgoing, :sum)';
        try {
            $this->db->beginTransaction();
            foreach ($res as $year => $outer1) {
                foreach ($outer1 as $munic => $data) {
                    $this->db->prepare($sql);
                    $this->db->bind(':varID', $variableID);
                    $this->db->bind(':munID', $munic);
                    $this->db->bind(':pYear', $year);
                    $this->db->bind(':incoming', $data['Innflytting']);
                    $this->db->bind(':outgoing', $data['Utflytting']);
                    $this->db->bind(':sum', $data['Netto']);
                    $this->db->execute();
                }
            }
            return $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    /**
     * Inserts into PopulationChange table
     * @param array $dataSet
     * @param integer $variableID
     * @return bool|PDOException
     */
    private function insertPopulationChange($dataSet, $variableID) {
        $temp = [];
        $sql = <<<'SQL'
INSERT INTO PopulationChange (variableID, municipalityID, pYear, pQuarter, born, dead, totalPopulation)
VALUES(:variableID, :munID, :pYear, :pQuarter, :born, :dead, :total);
SQL;
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $year = substr($item->Tid,0, 4);
                $quarter = substr($item->Tid, 5);
                $municipalityID = $this->core->getMunicipalityID($item->Region);
                if (!isset($temp[$municipalityID])) {$temp[$municipalityID] = []; }
                if (!isset($temp[$municipalityID][$year])) {$temp[$municipalityID][$year] = []; }
                $content = new stdClass();
                if (!isset($temp[$municipalityID][$year][$quarter])) {$temp[$municipalityID][$year][$quarter] = $content; }
                switch ($item->ContentsCode) {
                    case 'Dode3':
                        $temp[$municipalityID][$year][$quarter]->dead = $item->value; break;
                    case 'Fodte2':
                        $temp[$municipalityID][$year][$quarter]->born = $item->value; break;
                    case 'Folketallet1':
                        $temp[$municipalityID][$year][$quarter]->total = $item->value; break;
                }
                if (isset($temp[$municipalityID][$year][$quarter]->dead) &&
                    isset($temp[$municipalityID][$year][$quarter]->born) &&
                    isset($temp[$municipalityID][$year][$quarter]->total)) {
                    $this->db->prepare($sql);
                    $this->db->bind(':variableID', $variableID);
                    $this->db->bind(':munID', $municipalityID);
                    $this->db->bind(':pYear', $year);
                    $this->db->bind(':pQuarter', $quarter);
                    $this->db->bind(':born', $temp[$municipalityID][$year][$quarter]->born);
                    $this->db->bind(':dead', $temp[$municipalityID][$year][$quarter]->dead);
                    $this->db->bind(':total', $temp[$municipalityID][$year][$quarter]->total);
                    $this->db->execute();
                }
            }
            return $this->db->endTransaction();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    /**
     * Default method. Determines valid table columns and values. Then inserts data into the provided table.
     * @param $dataSet
     * @param $tableName
     * @param $variableID
     * @return bool|PDOException
     */
    private function insertGeneric($dataSet, $tableName, $variableID) {
        $sql = /** @lang text */
            'INSERT INTO ' . $tableName . '(';
        $municipalityID = 'municipalityID';
        $nace2007 = 'naceID';
        $ageRangeID = 'ageRangeID';
        $genderID = 'genderID';
        $pYear = 'pYear';
        $pQuarter = 'pQuarter';
        $primaryValueName = $this->core->getPrimaryValueName($tableName);
        $columns = [];
        $values = [];
        array_push($columns, 'variableID');
        $counter = 0;
        foreach ($dataSet as $item) {
            $values[$counter] = '(' . $variableID;
            if (isset($item->Region)) {
                if (!in_array($municipalityID, $columns)) array_push($columns, $municipalityID);
                $values[$counter] .= ',' . $this->core->getMunicipalityID($item->Region);
            }
            if (isset($item->NACE2007)) {
                if (!in_array($nace2007, $columns)) array_push($columns, $nace2007);
                $values[$counter] .= ',' .$this->core->getNaceID($item->NACE2007);
            }
            if (isset($item->Alder)) {
                if (!in_array($ageRangeID, $columns)) array_push($columns, $ageRangeID);
                $values[$counter] .= ',' . $this->core->getAgeRangeID($item->Alder);
            }
            if (isset($item->Kjonn)) {
                if (!in_array($genderID, $columns)) array_push($columns, $genderID);
                $values[$counter] .= ',' . $this->core->getGenderID($item->Kjonn);
            }
            if (isset($item->Tid)) {
                if (!in_array($pYear, $columns)) array_push($columns, $pYear);
                if (strchr($item->Tid, 'K')) {
                    $values[$counter] .= ',' . substr($item->Tid, 0, 4);
                    if (!in_array($pQuarter, $columns)) array_push($columns, $pQuarter);
                    $values[$counter] .= ',' . substr($item->Tid, 5);
                } else {
                    $values[$counter] .= ',' . $item->Tid;
                }
            }
            if (isset($item->value)) {
                if (!in_array($primaryValueName, $columns)) array_push($columns, $primaryValueName);
                $values[$counter] .= ',' . $item->value;
            } else {
                $values[$counter] .= ',null';
            }
            $values[$counter] .= ')';
            $counter++;
        }
        $sql .= implode(',', $columns);
        $sql .= ') VALUES' . implode(',', $values);
        $this->db->query($sql);
        return $this->db->execute();
    }
}