<?php
class ApiUpdate {
    private $db;
    private $logger;
    private $mysqltime;
    public function __construct($initializeDB = true) {
        $this->mysqltime = date('Y-m-d H:i:s');
        if ($this->logger == null) {
            $this->logger = new Logger();
        }
        if ($initializeDB) {
            $this->db = DatabaseHandlerFactory::getDatabaseHandler();
        }
    }

    public function update($request) {
        switch ($request->providerID) {
            case 1:
                return $this->updateSSB($request);
            default:
                return ['{unhandled update source type}'];
        }
    }

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
                $this->logger->log('DB: Forcing replacement of table ' . $tableName);
                $this->logger->log('DB: Force request given by ' . $_SERVER['REMOTE_ADDR']);
                $sql = 'DELETE FROM ' . $tableName;
                $this->db->query($sql);
                $queryResult = $this->db->execute();
                $this->logger->log('DB: Force delete request of table ' . $tableName . ' was a ' . ($queryResult ? 'success.' : 'failure.'));
                $this->logDb(VariableUpdateReason::a()->forceReplaceFull->key, $variableID, $_SERVER['REMOTE_ADDR']);
            }
        }
        return $this->updateTable($request, $tableName, $variableID);
    }
     private function updateTable($request, $tableName, $variableID) {
        $startTime = $this->logger->microTimeFloat();
        if (isset($request->dataSet[0]->Alder) && $request->dataSet[0]->Alder != null) {
            $request->dataSet = $this->mapAgeAndReplace($request->dataSet);
        }
//        $this->db->beginTransaction();
        try {
            switch ($tableName) {
                case 'PopulationChange':
                    $this->insertPopulationChange($request->dataSet, $tableName, $variableID);
                    break;
                case 'Movement':
                    $this->insertSpecialMovement($request->dataSet, $tableName, $variableID);
                    break;
                case 'Employment':
                    $this->insertSpecialEmployment($request->dataSet, $tableName, $variableID);
                    break;
                default:
                    $testTime = $this->logger->microTimeFloat();
//                    $this->db->beginTransaction();
                    ini_set('max_execution_time',60);
                    $sql = $this->generateInsertSql($request->dataSet, $tableName, $variableID);
//                    echo 'updateTable died after ' . ($this->logger->microTimeFloat() - $startTime) . ' seconds'; die;
                    $this->db->query($sql);
                    $this->db->execute();
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

    private function insertSpecialEmployment($dataSet, $tableName, $variableID) {

    }

    private function insertSpecialMovement($dataSet, $tableName, $variableID) {
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

    private function generateInsertSql($dataSet, $tableName, $variableID) {
        $sql = 'INSERT INTO ' . $tableName . '(';
        $municipalityID = 'municipalityID';
        $ageRangeID = 'ageRangeID';
        $genderID = 'genderID';
        $pYear = 'pYear';
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
            };
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
                $values[$counter] .= ',' . $item->Tid;
            }
            if (isset($item->value)) {
                if (!in_array($primaryValueName, $colNames)) array_push($colNames, $primaryValueName);
                $values[$counter] .= ',' . $item->value;
            }
            $values[$counter] .= ')';
            $counter++;
        }
        $sql .= implode(',', $colNames);
        $sql .= ') VALUES' . implode(',', $values);
        return $sql;
    }

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

    private $municipalityMap;
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
    private $ageRangeMap;
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
    private $genderMap;
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
    private $primaryValueMap;
    private function getPrimaryValueName($tableName) {
        if ($this->primaryValueMap == null) {
            $this->primaryValueMap['PopulationAge'] = 'population';
            $this->primaryValueMap['CommuteBalance'] = 'commuters';
            $this->primaryValueMap['Unemployment'] = 'unemploymentPercent';
            $this->primaryValueMap['EmploymentRatio'] = 'employmentPercent';
        }
        return $this->primaryValueMap[$tableName];
    }

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
