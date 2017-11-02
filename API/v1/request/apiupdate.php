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
    private $primaryValueMap;
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
        // check variable table
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
            $buildingStatusCodes[strval($result['buildingStatusCode'])] = strval($result['buildingStatusID']);
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
            if (!isset($buildingCategories[$buildingCode])) {
                $sql = "INSERT INTO BuildingCategory (buildingCategoryCode, buildingCategoryText) VALUES('$buildingCode', 'unknown')";
                $this->db->query($sql);
                $this->db->execute();
                $buildingCategories[$buildingCode] = $this->db->getLastInsertID();
                $buildingCategoryID = $this->db->getLastInsertID();
            } else {
                $buildingCategoryID = $buildingCategories[$buildingCode];
            }
            if (!isset($buildingStatusCodes[$buildingStatusCode])) {
                $sql = "INSERT INTO BuildingStatus (buildingStatusCode, buildingStatusText) VALUES('$buildingStatusCode', '$buildingStatusCode')";
                $this->db->query($sql);
                $this->db->execute();
                $buildingStatusCodes[$buildingStatusCode] = $this->db->getLastInsertID();
                $buildingStatusID = $this->db->getLastInsertID();
            } else {
                $buildingStatusID = $buildingStatusCodes[$buildingStatusCode];
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
        foreach ($dataSet as $item) {
            $municipalityID = $this->getMunicipalityID($item->Region);
            $naceID = $this->getNaceID($item->NACE2007);
            $genderID = $this->getGenderID($item->Kjonn);
            $ageRangeID = $this->getAgeRangeID($item->Alder);
            $pYear = $item->Tid;
            $uid = strval($municipalityID) . strval($naceID) . strval($genderID) . strval($ageRangeID) . strval($pYear);
            if (!isset($data[$uid])) {
                $data[$uid] = new stdClass();
                if ($item->ContentsCode == 'Sysselsatte') {
                    $data[$uid]->living = $item->value;
                } else {
                    $data[$uid]->working = $item->value;
                }
            } else {
                $valueSet = [];
                array_push($valueSet, $variableID);
                array_push($valueSet, $municipalityID);
                array_push($valueSet, $naceID);
                array_push($valueSet, $genderID);
                array_push($valueSet, $pYear);
                $balance = 0;
                if (isset($data[$uid]->working)) {
                    array_push($valueSet, $data[$uid]->working);
                    array_push($valueSet, $item->value);
                    $balance = $item->value - $data[$uid]->working;
                } else {
                    array_push($valueSet, $item->value);
                    array_push($valueSet, $data[$uid]->living);
                    $balance = $data[$uid]->living - $item->value;
                }
                array_push($valueSet, $balance);
                $sql .= '(' . implode(',', $valueSet) . ')';
                if (end($dataSet) != $item) { $sql .= ','; }
            }
        }
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
            $res[$item->Tid][$this->getMunicipalityID($item->Region)][$item->ContentsCode] = $item->value;
        }
        foreach ($res as $year => $outer1) {
            foreach ($outer1 as $munic => $data) {
                $incomingAll = $data['Innflytting'];
                $outgoingAll = $data['Utflytting'];
                $sumAll = $data['Netto'];
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
                $this->municipalityMap[$result['municipalityCode']] = $result['municipalityID'];
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
            $retSet = array();
            foreach ($dataSet as $item) {
                $staticAge = $item->Alder;
                switch ($staticAge) {
                    case in_array($staticAge, range(0,14)):
                        $item->Alder = '00-14';
                        break;
                    case in_array($staticAge, range(15,19)):
                        $item->Alder = '15-19';
                        break;
                    case in_array($staticAge, range(20,24)):
                        $item->Alder = '20-24';
                        break;
                    case in_array($staticAge, range(25,39)):
                        $item->Alder = '25-39';
                        break;
                    case in_array($staticAge, range(40,54)):
                        $item->Alder = '40-54';
                        break;
                    case in_array($staticAge, range(55,66)):
                        $item->Alder = '55-66';
                        break;
                    case in_array($staticAge, range(67,74)):
                        $item->Alder = '67-74';
                        break;
                    default:
                        $item->Alder = '75-127';
                        break;
                }
            }
            $this->logger->log('Time elapsed executing mapAlderAndReplace was ' . ($this->logger->microTimeFloat() - $startTime));
            return $dataSet;
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
//        $this->REMOVEME_updateVariableUpdateReason();
        $mysqltime = date('Y-m-d H:i:s');
        $sql = "INSERT into VariableUpdateLog VALUES($variableID, '$mysqltime', $reason, '$updateSource')";
        $this->db->query($sql);
        $this->db->execute();

    }

    private function REMOVEME_updateVariableUpdateReason() {
        $sql = 'DELETE FROM VariableUpdateLog';
        $this->db->query($sql);
        $this->db->execute();
        $sql = 'delete from VariableUpdateReason';
        $this->db->query($sql);
        $this->db->execute();
        foreach (VariableUpdateReason::a() as $item) {
            $sql = "insert into VariableUpdateReason VALUES($item->key, '$item->value')";
            $this->db->query($sql);
            echo $this->db->execute();
        }
    }
}
