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
        $this->db->beginTransaction();
        try {
            foreach ($request->dataSet as $item) {
                $sql = $this->generateInsertSql($item, $tableName, $variableID);
                $this->db->query($sql);
                $this->db->execute();
            }
            $sql = "UPDATE Variable SET LastUpdatedDate='$this->mysqltime' WHERE VariableID=$variableID";
            $this->db->query($sql);
            $this->db->execute();
        } catch (PDOException $PDOException) {
            $this->logger->log('PDO error when performing database write: ' . $PDOException->getMessage());
            $this->db->rollbackTransaction();
        }
        $this->db->endTransaction();
        return true;
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

    private $regionMap;
    private function getMunicipalityID($regionCode) {
        if ($this->regionMap == null) {
            $sql = 'SELECT MunicipalityID, MunicipalityCode FROM Municipality';
            $this->db->query($sql);
            foreach ($this->db->getResultSet() as $result) {
                $this->regionMap[$result['MunicipalityCode']] = $result['MunicipalityID'];
            }
        }
        return $this->regionMap[$regionCode];
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
            $this->primaryValueMap['PopulationAge'] = 'Population';
            $this->primaryValueMap['PopulationChange'] = 'TotalPopulation';
            $this->primaryValueMap['Movement'] = 'SumAll';
            $this->primaryValueMap['Employment'] = ['WorkplaceValue', 'LivingplaceValue'];
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
