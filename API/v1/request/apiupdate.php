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
                $this->updateSSB($request);
                break;
            default:
                return ['{unhandled update source type}'];
        }
        return ['{result=true}'];
    }

    private function updateSSB($request) {
        // check variable table
        $sql = 'SELECT VariableID, TableName from Variable WHERE ProviderCode =\'' . $request->sourceCode . '\'';
        $this->db->query($sql);
        $res = $this->db->getSingleResult();
        $variableID = 0;
        $tableName = '';
        if (!$res) { // Does not exist
            // TODO create entries if it does not exist. Requires incoming info
        } else {
            $variableID = $res['VariableID'];
            $tableName = $res['TableName'];
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
        if (isset($request->dataSet[0]->Alder) && $request->dataSet[0]->Alder != null) {
            $request->dataSet = $this->mapAgeAndReplace($request->dataSet);
        }
//        $this->db->beginTransaction();
        try {
            switch ($tableName) {
                case 'PopulationChange':
                    $this->insertSpecialPopulationChange($request->dataSet, $tableName, $variableID);
                    break;
                case 'Movement':
                    $this->insertSpecialMovement($request->dataSet, $tableName, $variableID);
                    break;
                case 'Employment':
                    $this->insertSpecialEmployment($request->dataSet, $tableName, $variableID);
                    break;
                default:
                    $this->db->beginTransaction();
                    foreach ($request->dataSet as $item) {
                        $sql = $this->generateInsertSql($item, $tableName, $variableID);
                        $this->db->query($sql);
                        $this->db->execute();
                    }
                    $this->db->endTransaction();
            }
            $sql = "UPDATE Variable SET LastUpdatedDate='$this->mysqltime' WHERE VariableID=$variableID";
            $this->db->query($sql);
            $this->db->execute();
        } catch (PDOException $PDOException) {
            $this->logger->log('PDO error when performing database write: ' . $PDOException->getMessage());
//            $this->db->rollbackTransaction();
        }
//        $this->db->endTransaction();
        return true;
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

    private function insertSpecialPopulationChange($dataSet, $tableName, $variableID) {
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


    private function generateInsertSql($item, $tableName, $variableID) {
        $sql = 'INSERT INTO ' . $tableName;
        $colNames = [];
        $values = [];
        array_push($colNames, 'VariableID');
        array_push($values, $variableID);
        if (isset($item->Region)) {
            array_push($colNames, 'MunicipalityID');
            array_push($values, $this->getMunicipalityID($item->Region));
        };
        if (isset($item->Alder)) {
            array_push($colNames, 'AgeRangeID');
            array_push($values, $this->getAgeRangeID($item->Alder));
        }
        if (isset($item->Kjonn)) {
            array_push($colNames, 'GenderID');
            array_push($values, $this->getGenderID($item->Kjonn));
        }
        if (isset($item->Tid)) {
            array_push($colNames, 'pYear');
            array_push($values, $item->Tid);
        }
        if (isset($item->value)) {
            array_push($colNames, $this->getPrimaryValueName($tableName));
            array_push($values, $item->value);
        }
        $sql .= ' (';
        foreach ($colNames as $column) {
            $sql .= $column;
            if ($column != end($colNames)) {
                $sql .= ', ';
            }
        }
        $sql .= ') VALUES (';
        for ($i = 0; $i < sizeof($values); $i++) {
            $sql .= $values[$i];
            if ($i < sizeof($values) - 1) {
                $sql .= ', ';
            }
        }
        $sql .= ')';
        return $sql;

    }

    private function insertMunicipalities() {
        $arry = [];
        $arry['0605'] = 'Ringerike';
        $arry['0612'] = 'Hole';
        $arry['0532'] = 'Jevnaker';
        $arry['0533'] = 'Lunner';
        $arry['0534'] = 'Gran';
        $arry['0536'] = 'Søndre Land';
        $arry['0540'] = 'Sør-Aurdal';
        $arry['0615'] = 'Flå';
        $arry['0622'] = 'Krødsherad';
        $arry['0623'] = 'Modum';
        $arry['0626'] = 'Lier';
        $arry['0624'] = 'Øvre Eiker';
        $arry['0625'] = 'Nedre Eiker';
        $arry['0602'] = 'Drammen';
        $arry['0219'] = 'Bærum';
        $arry['0220'] = 'Asker';
        $arry['0301'] = 'Oslo';
        foreach($arry as $key => $value) {
            $sql = "SELECT COUNT(*) as c FROM Municipality WHERE municipalityCode='$key'";
            $this->db->query($sql);
            if ($this->db->getSingleResult()['c'] != 1) {
                $sql = "INSERT INTO Municipality (municipalityCode, municipalityName) VALUES('$key', '$value')";
                $this->db->query($sql);
                $this->db->execute();
            }
        }
    }

    private $municipalityMap;
    private function getMunicipalityID($regionCode) {
        if ($this->municipalityMap == null) {
            $sql = 'SELECT municipalityID, municipalityCode FROM Municipality';
            $this->db->query($sql);
            foreach ($this->db->getResultSet() as $result) {
                $this->municipalityMap[$result['municipalityCode']] = $result['municipalityID'];
            }
        }
        if (!isset($this->municipalityMap[$regionCode])) {
            $this->municipalityMap = null;
            $this->insertMunicipalities();
            return $this->getMunicipalityID($regionCode);
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
            $startTime = microtime();
            $retSet = array();
            foreach ($dataSet as $item) {
                $staticAge = $item->Alder;
                if (!isset($retSet[$item->Region])) $retSet[$item->Region] = array();
                if (!isset($retSet[$item->Region][$item->Tid])) $retSet[$item->Region][$item->Tid] = array();
                if (!isset($retSet[$item->Region][$item->Tid][$item->Kjonn])) $retSet[$item->Region][$item->Tid][$item->Kjonn] = array();
                switch ($staticAge) {
                    case in_array($staticAge, range(0,14)):
                        if (!isset($retSet[$item->Region][$item->Tid][$item->Kjonn]['00-14'])) $retSet[$item->Region][$item->Tid][$item->Kjonn]['00-14'] = 0;
                        $retSet[$item->Region][$item->Tid][$item->Kjonn]['00-14'] += $item->value;
                        break;
                    case in_array($staticAge, range(15,19)):
                        if (!isset($retSet[$item->Region][$item->Tid][$item->Kjonn]['15-19'])) $retSet[$item->Region][$item->Tid][$item->Kjonn]['15-19'] = 0;
                        $retSet[$item->Region][$item->Tid][$item->Kjonn]['15-19'] += $item->value;
                        break;
                    case in_array($staticAge, range(20,24)):
                        if (!isset($retSet[$item->Region][$item->Tid][$item->Kjonn]['20-24'])) $retSet[$item->Region][$item->Tid][$item->Kjonn]['20-24'] = 0;
                        $retSet[$item->Region][$item->Tid][$item->Kjonn]['20-24'] += $item->value;
                        break;
                    case in_array($staticAge, range(25,39)):
                        if (!isset($retSet[$item->Region][$item->Tid][$item->Kjonn]['25-39'])) $retSet[$item->Region][$item->Tid][$item->Kjonn]['25-39'] = 0;
                        $retSet[$item->Region][$item->Tid][$item->Kjonn]['25-39'] += $item->value;
                        break;
                    case in_array($staticAge, range(40,54)):
                        if (!isset($retSet[$item->Region][$item->Tid][$item->Kjonn]['40-54'])) $retSet[$item->Region][$item->Tid][$item->Kjonn]['40-54'] = 0;
                        $retSet[$item->Region][$item->Tid][$item->Kjonn]['40-54'] += $item->value;
                        break;
                    case in_array($staticAge, range(55,66)):
                        if (!isset($retSet[$item->Region][$item->Tid][$item->Kjonn]['55-66'])) $retSet[$item->Region][$item->Tid][$item->Kjonn]['55-66'] = 0;
                        $retSet[$item->Region][$item->Tid][$item->Kjonn]['55-66'] += $item->value;
                        break;
                    case in_array($staticAge, range(67,74)):
                        if (!isset($retSet[$item->Region][$item->Tid][$item->Kjonn]['67-74'])) $retSet[$item->Region][$item->Tid][$item->Kjonn]['67-74'] = 0;
                        $retSet[$item->Region][$item->Tid][$item->Kjonn]['67-74'] += $item->value;
                        break;
                    default:
                        if (!isset($retSet[$item->Region][$item->Tid][$item->Kjonn]['75-127'])) $retSet[$item->Region][$item->Tid][$item->Kjonn]['75-127'] = 0;
                        $retSet[$item->Region][$item->Tid][$item->Kjonn]['75-127'] += $item->value;
                        break;
                }
                continue;
            }
            $classSet = array();
            foreach ($retSet as $regionKey => $region) {
                foreach ($region as $yearKey => $year) {
                    foreach ($year as $ageKey => $gender) {
                        foreach ($gender as $genderKey => $val) {
                            $value = $val;
                            $Tid = $yearKey;
                            $ContentsCode = $dataSet[0]->ContentsCode;
                            $Alder = $genderKey;
                            $Kjonn = $ageKey;
                            $Region = $regionKey;
                            $classItem = new stdClass();
                            $classItem->value = $value;
                            $classItem->Tid = $Tid;
                            $classItem->ContentsCode = $ContentsCode;
                            $classItem->Alder = $Alder;
                            $classItem->Kjonn = $Kjonn;
                            $classItem->Region = $Region;
                            array_push($classSet, $classItem);
                        }
                    }
                }
            }
            $this->logger->log('Time elapsed executing mapAlderAndReplace was ' . (microtime() - $startTime));
            return $classSet;
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
