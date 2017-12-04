<?php
/** Handles database updates */
class ApiUpdate {
    private $db;
    private $logger;
    private $mysqltime;
    private $municipalityMap;
    private $naceMap;
    private $ageRangeMap;
    private $genderMap;
    private $sectorMap;
    private $primaryValueMap;
    private $organizationTypeMap;
    private $enterprisePostCategoryMap;
    public function __construct($initializeDB = true) {
        $this->mysqltime = date('Y-m-d H:i:s');
        if ($this->logger == null) {
            $this->logger = new Logger();
        }
        if ($initializeDB) {
            $this->db = DatabaseHandlerFactory::getDatabaseHandler();
        }
    }

    /**
     * Public entry point for database updates.
     * Determines which provider the data is from.
     * @param $request
     * @return array|string
     */
    public function update($request) {
        switch ($request->providerID) {
            case 1:
                return $this->updateSSB($request);
            case 2:
                return $this->updateProff($request);
            default:
                return ['{unhandled update source type}'];
        }
    }

    /**
     * Checks whether or not the variable exists in the Variable data table.
     * Handles
     * @param $request
     * @return string
     */
    private function updateSSB($request) {
        set_time_limit(120);
        $sql = 'SELECT variableID, tableName FROM Variable WHERE providerCode =\'' . $request->sourceCode . '\'';
        $this->db->query($sql);
        $res = $this->db->getSingleResult();
        $variableID = 0;
        $tableName = '';
        if (!$res) { // Does not exist
            // TODO create entries if it does not exist. Requires incoming info
        } else {
            $variableID = $res['variableID'];
            $tableName = $res['tableName'];
            if ($request->forceReplace) {
                $this->truncateTable($tableName, $variableID);
            }
        }
        return $this->updateTable($request, $tableName, $variableID);
    }

    private function updateProff($request) {
        $sql = <<<SQL
SELECT variableID, tableName FROM Variable WHERE providerCode LIKE '%$request->sourceCode%';
SQL;
        $this->db->query($sql);
        $res = $this->db->getSingleResult();
        $variableID = 0;
        $tableName = '';
        if (!$res) {
            // TODO Entry does not exist
        } else {
            $variableID = $res['variableID'];
            $tableName = $res['tableName'];
            if ($request->forceReplace) {
                $this->truncateTable('EnterpriseEntry', $variableID);
                $this->truncateTable($tableName, $variableID);
            }
        }
        return $this->updateTable($request, $tableName, $variableID);
    }

    /**
     * Removes table contents
     * @param $tableName string
     * @param $variableID integer
     */
    private function truncateTable($tableName, $variableID) {
        $this->logger->log('DB: Forcing replacement of table ' . $tableName);
        $this->logger->log('DB: Force request given by ' . $_SERVER['REMOTE_ADDR']);
        $sql = 'DELETE FROM ' . $tableName;
        $this->db->query($sql);
        $queryResult = $this->db->execute();
        $this->logger->log('DB: Force delete request of table ' . $tableName . ' was a ' . ($queryResult ? 'success.' : 'failure.'));
        $this->logDb(VariableUpdateReason::a()->forceReplaceFull->key, $variableID, $_SERVER['REMOTE_ADDR']);
    }

    /**
     * Invokes correct database table update method based on table name.
     * @param $request mixed
     * @param $tableName string
     * @param $variableID number
     * @return string
     */
    private function updateTable($request, $tableName, $variableID) {
        $startTime = $this->logger->microTimeFloat();
        if (isset($request->dataSet[0]->Alder) && $request->dataSet[0]->Alder != null) {
            $request->dataSet = $this->mapAgeAndReplace($request->dataSet);
        }
//        $this->db->beginTransaction();
        try {
//            var_dump($variableID);
//            var_dump($request->dataSet[0]);
//            var_dump($tableName);
//            die;
            switch ($tableName) {
                case 'PopulationChange':
                    $this->insertPopulationChange($request->dataSet, $tableName, $variableID);
                    break;
                case 'Movement':
                    $this->insertMovement($request->dataSet, $tableName, $variableID);
                    break;
                case 'Employment':
                    $this->insertEmployment($request->dataSet, $tableName, $variableID);
                    break;
                case 'CommuteBalance':
                    $this->insertCommuteBalance($request->dataSet, $tableName, $variableID);
                    break;
                case 'Unemployment':
                    $this->insertUnemployment($request->dataSet, $tableName, $variableID);
                    break;
                case 'HomeBuildingArea':
                case 'FunctionalBuildingArea':
                    $this->insertBuildingArea($request->dataSet, $tableName, $variableID);
                    break;
                case 'HouseholdIncome':
                    $this->insertHouseholdIncome($request->dataSet, $tableName, $variableID);
                    break;
                case 'Proceeding':
                    $this->insertProceeding($request->dataSet, $tableName, $variableID);
                    break;
                case 'Education':
                    $this->insertEducation($request->dataSet, $tableName, $variableID);
                    break;
                case 'RegionalCooperation':
                    $this->insertRegionalCooperation($request->dataSet, $tableName, $variableID);
                    break;
                case 'NewEnterprise':
                    $this->insertNewEnterprise($request->dataSet, $tableName, $variableID);
                    break;
                case 'ClosedEnterprise':
                    $this->insertClosedEnterprise($request->dataSet, $tableName, $variableID);
                    break;
                case 'EmploymentSector':
                    $this->insertEmploymentSector($request->dataSet, $tableName, $variableID);
                    break;
                case 'Enterprise':
                    $this->insertEnterprise($request->dataSet, $tableName, $variableID);
                    break;
                default:
                    $testTime = $this->logger->microTimeFloat();
                    $this->insertGeneric($request->dataSet, $tableName, $variableID);
                    $this->logger->log('Insert generic took ' . ($this->logger->microTimeFloat() - $testTime) . ' seconds');
            }
            $sql = "UPDATE Variable SET lastUpdatedDate='$this->mysqltime' WHERE variableID=$variableID";
            $this->db->query($sql);
            $this->db->execute();
            $message = 'Successfully updated: ' . $tableName . '. Elapsed time: ' . date('i:s:u', $this->logger->microTimeFloat()-$startTime);
            return $message;
        } catch (PDOException $PDOException) {
         $message = 'PDO error when performing database write on ' . $tableName . ': '
             . $PDOException->getMessage() . ' '
             . $PDOException->getTraceAsString();
            $this->logger->log($message);
            return '{'.$message.'}';
//            $this->db->rollbackTransaction();
        }
//        $this->db->endTransaction();
    }

    private function insertEnterprise($dataSet, $tableName, $variableID) {
        set_time_limit(120);
        foreach ($dataSet as $enterprise) {
            $sql = <<<SQL
SELECT enterpriseID FROM Enterprise WHERE organizationNumber = $enterprise->organizationNumber;
SQL;
            $this->db->query($sql);
            $res = $this->db->getSingleResult();
            if ($res) {
                $enterpriseID = $res['enterpriseID'];
            } else {
                $municipalityID = $this->getMunicipalityID($this->getMunicipalityRegionCode($enterprise->municipalityName));
                $naceID = $this->getNaceID($enterprise->nace);
                $organizationTypeID = $this->getOrganizationTypeID($enterprise->organizationType);
                $employees = (sizeof($enterprise->employees) > 0 ? $enterprise->employees : null);
                $name = $this->db->quote($enterprise->name);
                if (strlen($name) > 63) { $name = substr($name, 0, 63); }
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
//            $insertString = "INSERT INTO EnterpriseEntry (enterpriseID, enterprisePostCategoryID, pYear, valueInNOK)
//                             VALUES (:enterpriseID, :catID, :pYear, :val)";
//            $this->db->prepare($insertString);
            $insertString = /** @lang text */
                "INSERT INTO EnterpriseEntry (enterpriseID, enterprisePostCategoryID, pYear, valueInNOK) VALUES ";
            $valueArray = array();
            foreach ($enterprise->entry as $entry) {
                $enterprisePostCategoryID = $this->getEnterprisePostCategory($entry->type);
                $pYear = $entry->year;
                $value = ($entry->value == null ? 0 : $entry->value);
                array_push($valueArray, "($enterpriseID, $enterprisePostCategoryID, $pYear, $value)");
//                $this->db->bind(':enterpriseID', $enterpriseID);
//                $this->db->bind(':catID', $enterprisePostCategoryID);
//                $this->db->bind(':pYear', $pYear);
//                $this->db->bind(':val', $value);
//                $this->db->execute();
            }
            $insertString .= implode(',', $valueArray);
            $this->db->query($insertString);
            $this->db->execute();
        }
    }

    private function insertEmploymentSector($dataSet, $tableName, $variableID) {
        $insertString = /** @lang text */
            "INSERT INTO $tableName (variableID, municipalityID, naceID, sectorID, pYear, workplaceValue, livingplaceValue) VALUES";
        $valueArray = array();
        $data = array();
        foreach ($dataSet as $item) {
            $municipalityID = $this->getMunicipalityID($item->Region);
            $naceID = $this->getNaceID($item->NACE2007);
            $sectorID = $this->getSectorID($item->Sektor710);
            $pYear = $item->Tid;
            $value = $item->value;
            if (!isset($data[$municipalityID])) {$data[$municipalityID] = []; }
            if (!isset($data[$municipalityID][$naceID])) {$data[$municipalityID][$naceID] = []; }
            if (!isset($data[$municipalityID][$naceID][$sectorID])) {$data[$municipalityID][$naceID][$sectorID] = []; }
            if (!isset($data[$municipalityID][$naceID][$sectorID][$pYear])) {$data[$municipalityID][$naceID][$sectorID][$pYear] = []; }
            if ($item->ContentsCode === 'SysselEtterBoste') {
                $data[$municipalityID][$naceID][$sectorID][$pYear]['living'] = $value;
            } else {
                $data[$municipalityID][$naceID][$sectorID][$pYear]['working'] = $value;
            }
            if (isset($data[$municipalityID][$naceID][$sectorID][$pYear]['living']) &&
                isset($data[$municipalityID][$naceID][$sectorID][$pYear]['working'])) {
                $living = $data[$municipalityID][$naceID][$sectorID][$pYear]['living'];
                $working = $data[$municipalityID][$naceID][$sectorID][$pYear]['working'];
                array_push($valueArray, "($variableID, $municipalityID, $naceID, $sectorID, $pYear, $working, $living)");
            }
        }
        $insertString .= implode(',', $valueArray);
        $this->db->query($insertString);
        return $this->db->execute();
    }

    /**
     * @param $dataSet mixed
     * @param $tableName string
     * @param $variableID integer
     * @return bool Return true on success
     */
    private function insertClosedEnterprise($dataSet, $tableName, $variableID) {
        $insertString = /** @lang text */
            "INSERT INTO $tableName (variableID, municipalityID, naceID, pYear, closedEnterprises) VALUES ";
        $valueArray = array();
        foreach ($dataSet as $item) {
            $municipalityID = $this->getMunicipalityID($item->Region);
            $naceID = $this->getNaceID($item->NACE2007);
            $pYear = $item->Tid;
            $value = $item->value;
            array_push($valueArray, "($variableID, $municipalityID, $naceID, $pYear, $value)");
        }
        $insertString .= implode(',', $valueArray);
        $this->db->query($insertString);
        return $this->db->execute();

    }

    /**
     * @param $dataSet mixed
     * @param $tableName string
     * @param $variableID integer
     * @return bool Returns true on success
     */
    private function insertNewEnterprise($dataSet, $tableName, $variableID) {
        $employeeCountRangeCodes = array();
        $sql = 'SELECT employeeCountRangeID, employeeCountRangeCode FROM EmployeeCountRange';
        $this->db->query($sql);
        foreach ($this->db->getResultSet() as $result) {
            $employeeCountRangeCodes[strval($result['employeeCountRangeCode'])] = $result['employeeCountRangeID'];
        }
        $enterpriseCategories = array();
        $sql = 'SELECT enterpriseCategoryID, enterpriseCategoryCodeNew FROM EnterpriseCategory';
        $this->db->query($sql);
        foreach ($this->db->getResultSet() as $result) {
            $enterpriseCategories[strval($result['enterpriseCategoryCodeNew'])] = $result['enterpriseCategoryID'];
        }
        $insertString = "INSERT INTO $tableName (variableID, municipalityID, enterpriseCategoryID, employeeCountRangeID, pYear, newEnterprises) VALUES ";
        $valueArray = array();
        foreach ($dataSet as $item) {
            $municipalityID = $this->getMunicipalityID($item->Region);
            $enterpriseCategoryID = $enterpriseCategories[strval($item->NyregBed)];
            $employeeCountRangeID = $employeeCountRangeCodes[strval($item->AntAnsatte)];
            $pYear = $item->Tid;
            $value = $item->value;
            array_push($valueArray, "($variableID, $municipalityID, $enterpriseCategoryID, $employeeCountRangeID, $pYear, $value)");
        }
        $insertString .= implode(',', $valueArray);
        $this->db->query($insertString);
        return $this->db->execute();
    }

    /**
     * @param $dataSet mixed
     * @param $tableName string
     * @param $variableID integer
     * @return bool Returns true on success
     */
    private function insertRegionalCooperation($dataSet, $tableName, $variableID) {
        $kostraCategories = array();
        $sql = 'SELECT kostraCategoryID, kostraCategoryCode FROM KostraCategory';
        $this->db->query($sql);
        foreach ($this->db->getResultSet() as $result) {
            $kostraCategories[strval($result['kostraCategoryCode'])] = $result['kostraCategoryID'];
        }
        $municipalExpenseCategories = array();
        $sql = 'SELECT municipalExpenseCategoryID, municipalExpenseCategoryCode FROM MunicpalExpenseCategory';
        $this->db->query($sql);
        foreach ($this->db->getResultSet() as $result) {
            $municipalExpenseCategories[strval($result['municipalExpenseCategoryCode'])] = $result['municipalExpenseCategoryID'];
        }
        $insertString = "INSERT INTO $tableName (variableID, municipalityID, kostraCategoryID, municipalExpenseCategoryID, pYear, expense) VALUES ";
        $valueArray = array();
        foreach ($dataSet as $item) {
            $municipalityID = $this->getMunicipalityID($item->Region);
            $kostraCategoryID = $kostraCategories[strval($item->FunksjonKostra)];
            $municipalExpenseCategoryID = $municipalExpenseCategories[strval($item->ArtGruppe)];
            $pYear = $item->Tid;
            if ($item->value == null) {continue; }
            $expense = $item->value;
            array_push($valueArray, "($variableID, $municipalityID, $kostraCategoryID, $municipalExpenseCategoryID, $pYear, $expense)");
        }
        $insertString .= implode(',', $valueArray);
        $this->db->query($insertString);
        return $this->db->execute();
    }

    /**
     * @param $dataSet mixed
     * @param $tableName string
     * @param $variableID integer
     * @return bool Returns true on success
     */
    private function insertEducation($dataSet, $tableName, $variableID) {
        $grades = array();
        $sql = 'SELECT gradeID, gradeCode FROM Grade';
        $this->db->query($sql);
        foreach ($this->db->getResultSet() as $result) {
            $grades[strval($result['gradeCode'])] = $result['gradeID'];
        }
        $insertString = "INSERT INTO $tableName (variableID, municipalityID, genderID, gradeID, pYear, percentEducated) VALUES ";
        $valueArray = array();
        foreach ($dataSet as $item) {
            $year = $item->Tid;
            $municipalityID = $this->getMunicipalityID($item->Region);
            $genderID = $this->getGenderID($item->Kjonn);
            $gradeID = $grades[strval($item->Nivaa)];
            $value = (($item->value == null) ? 'null' : $item->value);
            array_push($valueArray, "($variableID, $municipalityID, $genderID, $gradeID, $year, $value)");
        }
        $insertString .= implode(',', $valueArray);
        $this->db->query($insertString);
        return $this->db->execute();
    }

    /**
     * Inserts data into Proceeding table
     * @param $dataSet mixed
     * @param $tableName string
     * @param $variableID integer
     */
    private function insertProceeding($dataSet, $tableName, $variableID) {
        $proceedingCategories = array();
        $applicationTypes = array();
        $sql = 'SELECT proceedingCategoryID, proceedingCode FROM ProceedingCategory';
        $this->db->query($sql);
        foreach ($this->db->getResultSet() as $result) {
            $proceedingCategories[strval($result['proceedingCode'])] = $result['proceedingCategoryID'];
        }
        $sql = 'SELECT applicationTypeID, applicationTypeCode FROM ApplicationType';
        $this->db->query($sql);
        foreach ($this->db->getResultSet() as $result) {
            $applicationTypes[strval($result['applicationTypeCode'])] = $result['applicationTypeID'];
        }
        $insertString = "INSERT INTO $tableName (variableID, municipalityID, proceedingCategoryID, applicationTypeID, pYear, proceedingValue) VALUES";
        $valueArray = array();
        foreach ($dataSet as $item) {
            $year = $item->Tid;
            $proceedingCode = strval($item->ContentsCode);
            if (!isset($proceedingCategories[$proceedingCode])) {
                $sql = "INSERT INTO ProceedingCategory (proceedingCode, proceedingText) VALUES ('$proceedingCode', 'unknown')";
                $this->db->query($sql);
                $this->db->execute();
                $proceedingCategories[$proceedingCode] = $this->db->getLastInsertID();
                $proceedingID = $this->db->getLastInsertID();
            } else {
                $proceedingID = $proceedingCategories[$proceedingCode];
            }
            $applicationCode = strval($item->RammevilkSoknad);
            if (!isset($applicationTypes[$applicationCode])) {
                $sql = "INSERT INTO ApplicationType (applicationTypeCode, applicationTypeText) VALUES ('$applicationCode', 'unknown')";
                $this->db->query($sql);
                $this->db->execute();
                $applicationTypes[$applicationCode] = $this->db->getLastInsertID();
                $applicationID = $this->db->getLastInsertID();
            } else {
                $applicationID = $applicationTypes[$applicationCode];
            }
            $municipalityID = $this->getMunicipalityID($item->Region);
            $value = (isset($item->value) ? $item->value : 'null');
            array_push($valueArray, "($variableID, $municipalityID, $proceedingID, $applicationID, $year, $value)");
        }
        $insertString .= implode(',', $valueArray);
        $this->db->query($insertString);
        $this->db->execute();
    }

    /**
     * @param $dataSet
     * @param $tableName
     * @param $variableID
     */
    private function insertHouseholdIncome($dataSet, $tableName, $variableID) {
        $householdTypes = array();
        $sql = 'SELECT householdTypeID, householdTypeCode FROM HouseholdType';
        $this->db->query($sql);
        foreach($this->db->getResultSet() as $result) {
            $householdTypes[strval($result['householdTypeCode'])] = $result['householdTypeID'];
        }
        $insertString = 'INSERT INTO HouseholdIncome (variableID, municipalityID, householdTypeID, pYear, householdIncomeAvg) VALUES ';
        $valueArray = array();
        foreach ($dataSet as $item) {
            $year = $item->Tid;
            $householdType = strval($item->HusholdType);
            $householdTypeID = $householdTypes[$householdType];
            $region = $item->Region;
            $municipalityID = $this->getMunicipalityID($region);
            array_push($valueArray, "($variableID, $municipalityID, $householdTypeID, $year, $item->value)");
        }
        $insertString .= implode(',', $valueArray);
        $this->db->query($insertString);
        $this->db->execute();
    }

    /**
     * @param $dataSet
     * @param $tableName
     * @param $variableID
     */
    private function insertBuildingArea($dataSet, $tableName, $variableID) {
        $buildingCategories = array();
        $sql = 'SELECT buildingCategoryID, buildingCategoryCode FROM BuildingCategory';
        $this->db->query($sql);
        foreach ($this->db->getResultSet() as $result) {
            $buildingCategories[strval($result['buildingCategoryCode'])] = strval($result['buildingCategoryID']);
        }
        $buildingStatusCodes = array();
        $sql = 'SELECT buildingStatusID, buildingStatusCode, buildingStatusText FROM BuildingStatus';
        $this->db->query($sql);
        foreach ($this->db->getResultSet() as $result) {
            $buildingStatusCodes[strval($result['buildingStatusCode'])] = $result['buildingStatusID'];
        }
        $insertString = 'INSERT INTO ' . $tableName . ' (variableID, municipalityID, buildingStatusID, buildingCategoryID, pYear, pQuarter, buildingValue) VALUES ';
        $valueArray = array();
        foreach ($dataSet as $item) {
            $year = substr($item->Tid, 0, 4);
            $quarter = substr($item->Tid, 5);
            $buildingCode = strval($item->Byggeareal);
            $region = strval($item->Region);
            $municipalityID = $this->getMunicipalityID($region);
            $buildingStatusCode = strval($item->ContentsCode);
            if (!isset($buildingCategories[strval($buildingCode)])) {
                $sql = "INSERT INTO BuildingCategory (buildingCategoryCode, buildingCategoryText) VALUES('$buildingCode', 'unknown')";
                $this->db->query($sql);
                $this->db->execute();
                $buildingCategories[strval($buildingCode)] = $this->db->getLastInsertID();
                $buildingCategoryID = $this->db->getLastInsertID();
            } else {
                $buildingCategoryID = $buildingCategories[$buildingCode];
            }
            if (!isset($buildingStatusCodes[strval($buildingStatusCode)])) {
                $sql = "INSERT INTO BuildingStatus (buildingStatusCode, buildingStatusText) VALUES('$buildingStatusCode', '$buildingStatusCode')";
                $this->db->query($sql);
                $this->db->execute();
                $buildingStatusCodes[strval($buildingStatusCode)] = $this->db->getLastInsertID();
                $buildingStatusID = $this->db->getLastInsertID();
            } else {
                $buildingStatusID = $buildingStatusCodes[strval($buildingStatusCode)];
            }
            array_push($valueArray, "($variableID, $municipalityID, $buildingStatusID, $buildingCategoryID, $year, $quarter, $item->value)");
        }
        $insertString .= implode(',', $valueArray);
//        var_dump($insertString); die;
        $this->db->query($insertString);
        $this->db->execute();
    }

    /**
     * @param $dataSet
     * @param $tableName
     * @param $variableID
     * @return bool
     */
    private function insertUnemployment($dataSet, $tableName, $variableID) {
        $sql = <<<SQL
INSERT INTO Unemployment (variableID, municipalityID, ageRangeID, pYear, pMonth, unemploymentPercent)
VALUES 
SQL;
        foreach ($dataSet as $item) {
            $valueSet = [];
            array_push($valueSet, $variableID);
            array_push($valueSet, $this->getMunicipalityID($item->Region));
            array_push($valueSet, $this->getAgeRangeID($item->Alder));
            $year = substr($item->Tid, 0, 4);
            $month = substr($item->Tid, 5);
            array_push($valueSet, $year);
            array_push($valueSet, $month);
            array_push($valueSet, $item->value);
            $sql .= '(' . implode(',', $valueSet) . ')';
            if (end($dataSet) != $item) { $sql .= ','; }
        }
        $this->db->query($sql);
        return $this->db->execute();

    }

    /**
     * @param $dataSet
     * @param $tableName
     * @param $variableID
     * @return bool
     */
    private function insertCommuteBalance($dataSet, $tableName, $variableID) {
        $sql = <<<SQL
INSERT INTO CommuteBalance (variableID, municipalityID, workingMunicipalityID, pYear, commuters)
VALUES 
SQL;
        foreach ($dataSet as $item) {
            $valuesSet = [];
            array_push($valuesSet, $variableID);
            array_push($valuesSet, $this->getMunicipalityID($item->Bokommuen));
            array_push($valuesSet, $this->getMunicipalityID($item->ArbstedKomm));
            array_push($valuesSet, $item->Tid);
            array_push($valuesSet, $item->value);
            $sql .= '('. implode(',', $valuesSet) . ')';
            if (end($dataSet) != $item) { $sql .= ','; }
        }
        $this->db->query($sql);
        return $this->db->execute();
    }

    /**
     * @param $dataSet
     * @param $tableName
     * @param $variableID
     * @return bool
     */
    private function insertEmployment($dataSet, $tableName, $variableID) {
        $sql = <<<SQL
INSERT INTO Employment (variableID, municipalityID, naceID, genderID, pYear, workplaceValue, livingplaceValue, employmentBalance)
VALUES 
SQL;
        $data = [];
        $valueArray = array();
        foreach ($dataSet as $item) {
            $municipalityID = $this->getMunicipalityID($item->Region);
            $naceID = $this->getNaceID($item->NACE2007);
            $genderID = $this->getGenderID($item->Kjonn);
            $ageRangeID = $this->getAgeRangeID($item->Alder);
            $pYear = $item->Tid;
            $value = $item->value;
            if (!isset($data[$municipalityID])) {$data[$municipalityID] = []; }
            if (!isset($data[$municipalityID][$naceID])) {$data[$municipalityID][$naceID] = []; }
            if (!isset($data[$municipalityID][$naceID][$genderID])) {$data[$municipalityID][$naceID][$genderID] = []; }
            if (!isset($data[$municipalityID][$naceID][$genderID][$ageRangeID])) {$data[$municipalityID][$naceID][$genderID][$ageRangeID] = []; }
            if (!isset($data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear])) {$data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear] = []; }
            if ($item->ContentsCode == 'Sysselsatte') {
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
                $working = $data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear]['working'];
                $living = $data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear]['living'];
                array_push($valueArray, "($variableID, $municipalityID, $naceID, $genderID, $pYear, $working, $living, $balance)");
            }
        }
        $sql .= implode(',', $valueArray);
        $this->db->query($sql);
        return $this->db->execute();
    }

    /**
     * @param $dataSet
     * @param $tableName
     * @param $variableID
     */
    private function insertMovement($dataSet, $tableName, $variableID) {
        $res = [];
        foreach ($dataSet as $item) {
            $res[$item->Tid][$this->getMunicipalityID(strval($item->Region))][$item->ContentsCode] = $item->value;
        }
        foreach ($res as $year => $outer1) {
            foreach ($outer1 as $munic => $data) {
                $incomingAll = (is_null($data['Innflytting']) ? 'null' : $data['Innflytting']);
                $outgoingAll = (is_null($data['Utflytting']) ? 'null' : $data['Utflytting']);
                $sumAll = (is_null($data['Netto']) ? 'null' : $data['Netto']);
                $sql = <<<SQL
INSERT INTO Movement (variableID, municipalityID, pYear, incomingAll, outgoingAll, sumAll)
VALUES($variableID, $munic, $year, $incomingAll, $outgoingAll, $sumAll);
SQL;
                $this->db->query($sql);
                $this->db->execute();
            }
        }
    }

    /**
     * @param $dataSet
     * @param $tableName
     * @param $variableID
     */
    private function insertPopulationChange($dataSet, $tableName, $variableID) {
        $res = [];
        foreach ($dataSet as $value) {
            $year = substr($value->Tid,0, 4);
            $quarter = substr($value->Tid, 5);
            $municipalityID = $this->getMunicipalityID($value->Region);
            $res[$year][$quarter][$municipalityID][$value->ContentsCode] = $value->value;
        }
        foreach($res as $year => $outer1) {
            foreach ($outer1 as $quarter => $outer2) {
                foreach ($outer2 as $munic => $data) {
                    $dead = $data['Dode3'];
                    $born = $data['Fodte2'];
                    $totalPopulation = $data['Folketallet1'];
                    $sql = <<<SQL
INSERT INTO PopulationChange (variableID, municipalityID, pYear, pQuarter, born, dead, totalPopulation)
VALUES($variableID, $munic, $year, $quarter, $born, $dead, $totalPopulation);
SQL;
                    $this->db->query($sql);
                    $this->db->execute();
                }
            }
        }
    }

    /**
     * Default method. Determines valid table columns and values. Then inserts data into the provided table.
     * @param $dataSet
     * @param $tableName
     * @param $variableID
     * @return bool
     */
    private function insertGeneric($dataSet, $tableName, $variableID) {
        $sql = 'INSERT INTO ' . $tableName . '(';
        $municipalityID = 'municipalityID';
        $nace2007 = 'naceID';
        $ageRangeID = 'ageRangeID';
        $genderID = 'genderID';
        $pYear = 'pYear';
        $pQuarter = 'pQuarter';
        $primaryValueName = $this->getPrimaryValueName($tableName);
        $colNames = [];
        $values = [];
        array_push($colNames, 'variableID');
        $counter = 0;
        foreach ($dataSet as $item) {
            $values[$counter] = '(' . $variableID;
            if (isset($item->Region)) {
                if (!in_array($municipalityID, $colNames)) array_push($colNames, $municipalityID);
                $values[$counter] .= ',' . $this->getMunicipalityID($item->Region);
            }
            if (isset($item->NACE2007)) {
                if (!in_array($nace2007, $colNames)) array_push($colNames, $nace2007);
                $values[$counter] .= ',' .$this->getNaceID($item->NACE2007);
            }
            if (isset($item->Alder)) {
                if (!in_array($ageRangeID, $colNames)) array_push($colNames, $ageRangeID);
                $values[$counter] .= ',' . $this->getAgeRangeID($item->Alder);
            }
            if (isset($item->Kjonn)) {
                if (!in_array($genderID, $colNames)) array_push($colNames, $genderID);
                $values[$counter] .= ',' . $this->getGenderID($item->Kjonn);
            }
            if (isset($item->Tid)) {
                if (!in_array($pYear, $colNames)) array_push($colNames, $pYear);
                if (strchr($item->Tid, 'K')) {
                    $values[$counter] .= ',' . substr($item->Tid, 0, 4);
                    if (!in_array($pQuarter, $colNames)) array_push($colNames, $pQuarter);
                    $values[$counter] .= ',' . substr($item->Tid, 5);
                } else {
                    $values[$counter] .= ',' . $item->Tid;
                }
            }
            if (isset($item->value)) {
                if (!in_array($primaryValueName, $colNames)) array_push($colNames, $primaryValueName);
                $values[$counter] .= ',' . $item->value;
            } else {
                $values[$counter] .= ',null';
            }
            $values[$counter] .= ')';
            $counter++;
        }
        $sql .= implode(',', $colNames);
        $sql .= ') VALUES' . implode(',', $values);
        $this->db->query($sql);
        return $this->db->execute();
    }

    /**
     * TEMPORARY helper method to insert municipalities.
     * To be removed at a later time
     * TODO REMOVE ME
     */
    private function insertMunicipalities() {
        $arry = [];
        $arry['0605'] = 'Ringerike';
        $arry['0612'] = 'Hole';
        $arry['0532'] = 'Jevnaker';
        $arry['0533'] = 'Lunner';
        $arry['0534'] = 'Gran';
        $arry['0626'] = 'Lier';
        $arry['0624'] = 'Øvre Eiker';
        $arry['0625'] = 'Nedre Eiker';
        $arry['0602'] = 'Drammen';
        $arry['0219'] = 'Bærum';
        $arry['0220'] = 'Asker';
        $arry['0301'] = 'Oslo';
        $arry['0104'] = 'Moss';
        $arry['0604'] = 'Kongsberg';
        $arry['0238'] = 'Nannestad';
        $arry['0231'] = 'Skedsmo';
        $arry['0217'] = 'Oppegård';
        $arry['0213'] = 'Ski';
        $arry['0214'] = 'Ås';
        $arry['0211'] = 'Vestby';
        $arry['0704'] = 'Tønsberg';
        foreach($arry as $key => $value) {
            $sql = "SELECT COUNT(*) as c FROM Municipality WHERE municipalityCode='$key'";
            $this->db->query($sql);
            if ($this->db->getSingleResult()['c'] < 1) {
                $sql = "INSERT INTO Municipality (municipalityCode, municipalityName) VALUES('$key', '$value')";
                $this->db->query($sql);
                $this->db->execute();
            }
        }
    }

    /**
     * Gets the interal ID for provided municipality code.
     * Generates a local cache of municipality code and internal IDs.
     * @param $regionCode
     * @param bool $rewind
     * @return string
     */
    private function getMunicipalityID($regionCode, $rewind = false) {
        if ($this->municipalityMap == null) {
            $sql = 'SELECT municipalityID, municipalityCode FROM Municipality';
            $this->db->query($sql);
            foreach ($this->db->getResultSet() as $result) {
                $this->municipalityMap[strval($result['municipalityCode'])] = $result['municipalityID'];
            }
        }
        if ($rewind) {
            $this->municipalityMap = null;
            $sql = 'SELECT municipalityID, municipalityCode FROM Municipality';
            $this->db->query($sql);
            foreach ($this->db->getResultSet() as $result) {
                $this->municipalityMap[$result['municipalityCode']] = $result['municipalityID'];
            }
            if (!isset($this->municipalityMap[$regionCode])) {
                $sql = "INSERT INTO Municipality (municipalityCode, municipalityName) VALUES ('$regionCode', '$regionCode')";
                $this->db->query($sql);
                $this->db->execute();
                $this->municipalityMap[$regionCode] = $this->db->getLastInsertID();
                return $this->db->getLastInsertID();
            } else {
                return $this->municipalityMap[$regionCode];
            }
        }
        if (!isset($this->municipalityMap[$regionCode])) {
            $this->insertMunicipalities();
           return $this->getMunicipalityID($regionCode, true);
        } else {
            return $this->municipalityMap[$regionCode];
        }
    }

    private function getMunicipalityRegionCode($municipalityName) {
        $sql = "SELECT municipalityCode FROM Municipality WHERE municipalityName LIKE '%$municipalityName%'";
        $this->db->query($sql);
        $res = strval($this->db->getSingleResult()['municipalityCode']);
        if (sizeof($res) == 0) {
            return '-1';
        } else {
            return strval($this->db->getSingleResult()['municipalityCode']);
        }
    }

    private function getEnterprisePostCategory($enterprisePostCategoryCode) {
        if ($this->enterprisePostCategoryMap == null) {
            $sql = 'SELECT enterprisePostCategoryID, enterprisePostCategoryCode FROM EnterprisePostCategory';
            $this->db->query($sql);
            foreach ($this->db->getResultSet() as $item) {
                $this->enterprisePostCategoryMap[strval($item['enterprisePostCategoryCode'])] = $item['enterprisePostCategoryID'];
            }
        }
        if (!isset($this->enterprisePostCategoryMap[strval($enterprisePostCategoryCode)])) {
            $sql = "INSERT INTO EnterprisePostCategory (enterprisePostCategoryCode, enterprisePostCategoryText) 
VALUES ('$enterprisePostCategoryCode', '$enterprisePostCategoryCode')";
            $this->db->query($sql);
            $this->db->execute();
            $this->enterprisePostCategoryMap[strval($enterprisePostCategoryCode)] = $this->db->getLastInsertID();
//            return $this->db->getLastInsertID();
        }
        return $this->enterprisePostCategoryMap[strval($enterprisePostCategoryCode)];
    }

    private function getOrganizationTypeID($organizationTypeCode) {
        if ($this->organizationTypeMap == null) {
            $sql = 'SELECT organizationTypeID, organizationTypeCode FROM OrganizationType';
            $this->db->query($sql);
            foreach ($this->db->getResultSet() as $item) {
                $this->organizationTypeMap[strval($item['organizationTypeCode'])] = $item['organizationTypeID'];
            }
        }
        if (!isset($this->organizationTypeMap[strval($organizationTypeCode)])) {
            $sql = "INSERT INTO OrganizationType (organizationTypeCode, organizationTypeText) VALUES ('$organizationTypeCode', '$organizationTypeCode')";
            $this->db->query($sql);
            $this->db->execute();
            $this->organizationTypeMap[strval($organizationTypeCode)] = $this->db->getLastInsertID();
            return $this->db->getLastInsertID();
        }
        return $this->organizationTypeMap[strval($organizationTypeCode)];
    }

    /**
     * Gets the internal ID for provided NACE code.
     * Generates a local cache of NACE2007 codes and internal IDs.
     * @param $naceCode
     * @return mixed
     */
    private function getNaceID($naceCode) {
        if ($this->naceMap == null) {
            $sql = 'SELECT naceID, naceCodeStart, naceCodeEnd FROM Nace2007';
            $this->db->query($sql);
            foreach ($this->db->getResultSet() as $result) {
                $start = $result['naceCodeStart'];
                if (strlen($start) == 1) $start = '0' . $start;
                $end = $result['naceCodeEnd'];
                if (strlen($end) == 1) $end = '0' . $end;
                $constructedCode = $start . '-' . $end;
                $this->naceMap[$constructedCode] = $result['naceID'];
            }
        }
        if (!strpos(strval($naceCode), '-')) {
            $naceCode = strval($naceCode) . '-' . strval($naceCode);
        }
        if (!isset($this->naceMap[$naceCode])) {
            $explodedCode = explode('-', $naceCode);
            if (!isset($explodedCode[1])) {
                $explodedCode[1] = $naceCode;
            }
            $sql = <<<SQL
INSERT INTO Nace2007 (naceCodeStart, naceCodeEnd, naceText)
VALUES($explodedCode[0], $explodedCode[1], 'Unknown NACE')
SQL;
            $this->db->query($sql);
            $this->db->execute();
            $constructedCode = $explodedCode[0] . '-' . $explodedCode[1];
            $this->naceMap[$constructedCode] = $this->db->getLastInsertID();
        }
        return $this->naceMap[$naceCode];
    }


    private function getSectorID($sectorCode) {
        if ($this->sectorMap == null) {
            $sql = 'SELECT sectorID, sectorCode FROM Sector';
            $this->db->query($sql);
            foreach ($this->db->getResultSet() as $result) {
                $this->sectorMap[strval($result['sectorCode'])] = $result['sectorID'];
            }
        }
        if (!isset($this->sectorMap[strval($sectorCode)])) {
            $sql = "INSERT INTO Sector (sectorCode, sectorID) VALUES ('$sectorCode', '$sectorCode')";
            $this->db->query($sql);
            $this->db->execute();
            $this->sectorMap[strval($sectorCode)] = $this->db->getLastInsertID();
        }
        return $this->sectorMap[strval($sectorCode)];
    }


    /**
     * Gets the interal ID for provided age range code.
     * Generates a local cache of age range codes and internal IDs.
     * @param $ageRange
     * @return mixed
     */
    private function getAgeRangeID($ageRange) {
        if ($this->ageRangeMap == null) {
            $sql = 'SELECT AgeRangeID, AgeRangeStart, AgeRangeEnd from AgeRange';
            $this->db->query($sql);
            foreach($this->db->getResultSet() as $result) {
                $ageRangeString = $result['AgeRangeStart'] . '-' . $result['AgeRangeEnd'];
                if (strlen($ageRangeString) < 5) $ageRangeString = '0'.$ageRangeString;
                $this->ageRangeMap[$ageRangeString] = $result['AgeRangeID'];
            }
        }
        return $this->ageRangeMap[$ageRange];
    }

    /**
     * Gets the internal ID for provided gender code.
     * Generates a local cache of gender codes and internal IDs.
     * @param $gender
     * @return mixed
     */
    private function getGenderID($gender) {
        if ($this->genderMap == null) {
            $sql = 'SELECT GenderID FROM Gender';
            $this->db->query($sql);
            foreach($this->db->getResultSet() as $result) {
                $this->genderMap[$result['GenderID']] = $result['GenderID'];
            }
        }
        return $this->genderMap[$gender];
    }

    /**
     * Gets the primary value column name for generic table insertion
     * @param $tableName
     * @return mixed
     */
    private function getPrimaryValueName($tableName) {
        if ($this->primaryValueMap == null) {
            $this->primaryValueMap['PopulationAge'] = 'population';
            $this->primaryValueMap['CommuteBalance'] = 'commuters';
            $this->primaryValueMap['Unemployment'] = 'unemploymentPercent';
            $this->primaryValueMap['EmploymentRatio'] = 'employmentPercent';
            $this->primaryValueMap['Bankruptcy'] = 'bankruptcies';
        }
        return $this->primaryValueMap[$tableName];
    }

    /**
     * Maps incoming age value to age range.
     * @param $dataSet
     * @return mixed
     */
    private function mapAgeAndReplace($dataSet) {
        if (!strpos($dataSet[0]->Alder, '-')) { //Checking if alder is interval, not range
            $startTime = $this->logger->microTimeFloat();
            $temp = array();
            foreach ($dataSet as $item) {
                if (!isset($temp[$item->Tid])) {$temp[$item->Tid] = []; }
                if (!isset($temp[$item->Tid][$item->Region])) {$temp[$item->Tid][$item->Region] = []; }
                if (!isset($temp[$item->Tid][$item->Region][$item->Kjonn])) {$temp[$item->Tid][$item->Region][$item->Kjonn] = []; }
                $staticAge = $item->Alder;
                switch ($staticAge) {
                    case in_array($staticAge, range(0,14)):
                        $range = '00-14';
                        break;
                    case in_array($staticAge, range(15,19)):
                        $range = '15-19';
                        break;
                    case in_array($staticAge, range(20,24)):
                        $range = '20-24';
                        break;
                    case in_array($staticAge, range(25,39)):
                        $range = '25-39';
                        break;
                    case in_array($staticAge, range(40,54)):
                        $range = '40-54';
                        break;
                    case in_array($staticAge, range(55,66)):
                        $range = '55-66';
                        break;
                    case in_array($staticAge, range(67,74)):
                        $range = '67-74';
                        break;
                    default:
                        $range = '75-127';
                        break;
                }
                if (!isset($temp[$item->Tid][$item->Region][$item->Kjonn][$range])) {$temp[$item->Tid][$item->Region][$item->Kjonn][$range] = 0; }
                $temp[$item->Tid][$item->Region][$item->Kjonn][$range] += $item->value;
            }
            $retSet = [];
            foreach ($temp as $timeKey => $timeValue) {
                foreach ($timeValue as $regionKey => $regionValue) {
                    foreach ($regionValue as $kjonnKey => $kjonnValue) {
                        foreach ($kjonnValue as $ageKey => $ageValue) {
                            $obj = new stdClass();
                            $obj->value = $ageValue;
                            $obj->Tid = strval($timeKey);
                            $obj->ContentsCode = 'Personer1';
                            $obj->Alder = strval($ageKey);
                            $obj->Kjonn = strval($kjonnKey);
                            $obj->Region = strval($regionKey);
                            array_push($retSet, $obj);
                        }
                    }
                }
            }
            $this->logger->log('Time elapsed executing mapAlderAndReplace was ' . ($this->logger->microTimeFloat() - $startTime));
            return $retSet;
        } else {
            return $dataSet;
        }
    }

    /**
     * @param integer $reason
     * @param string $variableID
     * @param string|null $updateSource
     */
    private function logDb($reason, $variableID, $updateSource = null) {
        $mysqltime = date('Y-m-d H:i:s');
        $sql = "INSERT into VariableUpdateLog (variableID, updateDate, updateReasonID, updateSource) VALUES($variableID, '$mysqltime', $reason, '$updateSource')";
        $this->db->query($sql);
        $this->db->execute();

    }
}
