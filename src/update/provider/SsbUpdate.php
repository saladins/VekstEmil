<?php

class SsbUpdate {
    /** @var Logger */
    private $logger;
    /** @var CoreMethods  */
    private $core;
    /** @var DatabaseHandler  */
    private $db;

    /**
     * SsbUpdate constructor.
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
                case 'ClosedEnterprise':
                    $this->insertClosedEnterprise($request->dataSet, $variableID);
                    break;
                case 'CommuteBalance':
                    $this->insertCommuteBalance($request->dataSet, $variableID);
                    break;
                case 'Education':
                    $this->insertEducation($request->dataSet, $variableID);
                    break;
                case 'Employment':
                    $this->insertEmployment($request->dataSet, $variableID);
                    break;
                case 'EmploymentDetailed':
                    $this->insertEmploymentDetailed($request->dataSet, $variableID);
                    break;
                case 'EmploymentRatio':
                    $this->insertEmploymentRatio($request->dataSet, $variableID);
                    break;
                case 'EmploymentSector':
                    $this->insertEmploymentSector($request->dataSet, $variableID);
                    break;
                case 'HomeBuildingArea':
                case 'FunctionalBuildingArea':
                    $this->insertBuildingArea($request->dataSet, $tableName, $variableID);
                    break;
                case 'HouseholdIncome':
                    $this->insertHouseholdIncome($request->dataSet, $variableID);
                    break;
                case 'ImmigrantPopulation':
                    $this->insertImmigrantPopulation($request->dataSet, $variableID);
                    break;
                case 'Immigration':
                    $this->insertImmigration($request->dataSet, $variableID);
                    break;
                case 'MunicipalEconomy':
                    $this->insertMunicipalEconomy($request->dataSet, $variableID);
                    break;
                case 'Movement':
                    $this->insertMovement($request->dataSet, $variableID);
                    break;
                case 'NewEnterprise':
                    $this->insertNewEnterprise($request->dataSet, $variableID);
                    break;
                case 'PopulationAge':
                    $this->insertPopulationAge($request->dataSet, $variableID);
                    break;
                case 'PopulationChange':
                    $this->insertPopulationChange($request->dataSet, $variableID);
                    break;
                case 'Proceeding':
                    $this->insertProceeding($request->dataSet, $variableID);
                    break;
                case 'RegionalCooperation':
                    $this->insertRegionalCooperation($request->dataSet, $variableID);
                    break;
                case 'Unemployment':
                    $this->insertUnemployment($request->dataSet, $variableID);
                    break;
                default:
//                    throw new PDOException('Generic method used');
                    $this->insertGeneric($request->dataSet, $tableName, $variableID);
            }
            $date = new DateTime();
            $this->core->setLastUpdatedTime($tableName, $date->getTimestamp());
            $this->db->disconnect();
            $message = 'Successfully updated: ' . $tableName . '. Elapsed time: ' . date('i:s:u', (integer)($this->logger->microTimeFloat() - $startTime));
            return $message;
        } catch (PDOException $PDOException) {
            $this->db->disconnect();
            $message = 'PDO error when performing database write on ' . $tableName . ': '
                . $PDOException->getMessage() . ' '
                . $PDOException->getTraceAsString();
            $this->logger->log($message);
            return '{'.$message.'}';
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
            return $this->db->commit();
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
            return $this->db->commit();
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
            $region = [];
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $this->db->prepare($sql);
                $this->db->bind(':varID', $variableID);
                $this->db->bind(':munID', $this->core->getMunicipalityID($item->Bokommuen));
                $this->db->bind(':workMunID', $this->core->getMunicipalityID($item->ArbstedKomm));
                $this->db->bind(':pYear', $item->Tid);
                $this->db->bind(':commuters', $item->value);
                $this->db->execute();
                if (in_array($item->Bokommuen, $this->core->getRegionCodes())) {
                    if (!isset($region[$item->Tid])) {$region[$item->Tid] = []; }
                    if (in_array($item->ArbstedKomm, $this->core->getRegionCodes())) {
                        if (!isset($region[$item->Tid]['9999'])) {
                            $region[$item->Tid]['9999'] = 0; }
                        $region[$item->Tid]['9999']+= $item->value;
                    } else {
                        if (!isset($region[$item->Tid][$item->ArbstedKomm])) {
                            $region[$item->Tid][$item->ArbstedKomm] = 0; }
                        $region[$item->Tid][$item->ArbstedKomm] += $item->value;
                    }
                }
            }
            $regID = $this->core->getMunicipalityID('9999');
            foreach ($region as $year => $item2) {
                foreach ($item2 as $workMunID => $item) {
                    $this->db->prepare($sql);
                    $this->db->bind(':varID', $variableID);
                    $this->db->bind(':munID', $regID);
                    $this->db->bind(':workMunID', $this->core->getMunicipalityID($workMunID));
                    $this->db->bind(':pYear', $year);
                    $this->db->bind(':commuters', $item);
                    $this->db->execute();
                }
            }
            return $this->db->commit();
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
            $region = [];
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $municipalityID = $this->core->getMunicipalityID($item->Region);
                $genderID = $this->core->getGenderID($item->Kjonn);
                $gradeID = $grades[strval($item->Nivaa)];
                $pYear = $item->Tid;
                $this->db->prepare($sql);
                $this->db->bind(':varID', $variableID);
                $this->db->bind(':munID', $municipalityID);
                $this->db->bind(':genderID', $genderID);
                $this->db->bind(':gradeID', $gradeID);
                $this->db->bind(':pYear', $pYear);
                $this->db->bind(':percentEducated', $item->value);
                $this->db->execute();
                if (in_array($item->Region, $this->core->getRegionCodes())) {
                    if (!isset($region[$pYear])) {$region[$pYear] = []; }
                    if (!isset($region[$pYear][$genderID])) {$region[$pYear][$genderID] = []; }
                    if (!isset($region[$pYear][$genderID][$gradeID])) {$region[$pYear][$genderID][$gradeID] = 0; }
                    $region[$pYear][$genderID][$gradeID] += $item->value;
                }
            }
            $regionID = $this->core->getMunicipalityID('9999');
            foreach ($region as $pYear => $item3) {
                foreach ($item3 as $genderID => $item2) {
                    foreach ($item2 as $gradeID => $item) {
                        $this->db->prepare($sql);
                        $this->db->bind(':varID', $variableID);
                        $this->db->bind(':munID', $regionID);
                        $this->db->bind(':genderID', $genderID);
                        $this->db->bind(':gradeID', $gradeID);
                        $this->db->bind(':pYear', $pYear);
                        $this->db->bind(':percentEducated', $item / 3);
                        $this->db->execute();
                    }
                }
            }
            return $this->db->commit();
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
        $region = [];
        $regionObj = '{"workVal": 0, "livVal": 0, "balance": 0}';
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
                    if (in_array($item->Region, $this->core->getRegionCodes())) {
                        if (!isset($region[$pYear])) { $region[$pYear] = []; }
                        if (!isset($region[$pYear][$naceID])) { $region[$pYear][$naceID] = []; }
                        if (!isset($region[$pYear][$naceID][$genderID])) { $region[$pYear][$naceID][$genderID] = json_decode($regionObj); }
                        $region[$pYear][$naceID][$genderID]->workVal += $data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear]['working'];;
                        $region[$pYear][$naceID][$genderID]->livVal += $data[$municipalityID][$naceID][$genderID][$ageRangeID][$pYear]['living'];
                        $region[$pYear][$naceID][$genderID]->balance += $balance;
                    }
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
            $regionID = $this->core->getMunicipalityID('9999');
            foreach ($region as $year => $item3) {
                foreach ($item3 as $naceID => $item2) {
                    foreach ($item2 as $genderID => $item) {
                        $this->db->prepare($sql);
                        $this->db->bind(':varID', $variableID);
                        $this->db->bind(':munID', $regionID);
                        $this->db->bind(':naceID', $naceID);
                        $this->db->bind(':genderID', $genderID);
                        $this->db->bind(':pYear', $year);
                        $this->db->bind(':workVal', $item->workVal);
                        $this->db->bind(':livVal', $item->livVal);
                        $this->db->bind(':balance', $item->balance);
                        $this->db->execute();
                    }
                }
            }
            return $this->db->commit();

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
VALUES (:variableID, :municipalityID, :naceID, :genderID, :pYear, :workplaceValue, :livingplaceValue, :employmentBalance)
SQL;
        $data = [];
        $regionObj = '{"workVal": 0, "livVal": 0, "balance": 0}';
        $region = [];
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
                    $genderID = $this->core->getGenderID($item->Kjonn);
                    $naceID = $this->core->getNaceID($item->NACE2007);
                    $pYear = $item->Tid;
                    $this->db->prepare($sql);
                    $this->db->bind(':variableID', $variableID);
                    $this->db->bind(':municipalityID', $this->core->getMunicipalityID($item->Region));
                    $this->db->bind(':naceID', $naceID);
                    $this->db->bind(':genderID', $genderID);
                    $this->db->bind(':pYear', $pYear);
                    $this->db->bind(':workplaceValue', $working);
                    $this->db->bind(':livingplaceValue', $living);
                    $this->db->bind(':employmentBalance', $balance);
                    $this->db->execute();
                    if (in_array($item->Region, $this->core->getRegionCodes())) {
                        if (!isset($region[$pYear])) {$region[$pYear] = [];}
                        if (!isset($region[$pYear][$naceID])) {$region[$pYear][$naceID] = [];}
                        if (!isset($region[$pYear][$naceID][$genderID])) {$region[$pYear][$naceID][$genderID] = json_decode($regionObj);}
                        $region[$pYear][$naceID][$genderID]->workVal = $working;
                        $region[$pYear][$naceID][$genderID]->livVal = $living;
                        $region[$pYear][$naceID][$genderID]->balance = $balance;
                    }
                }
            }
            $regionID = $this->core->getMunicipalityID('9999');
            foreach ($region as $year => $item3) {
                foreach ($item3 as $naceID => $item2) {
                    foreach ($item2 as $genderID => $item) {
                        $this->db->prepare($sql);
                        $this->db->bind(':variableID', $variableID);
                        $this->db->bind(':municipalityID', $regionID);
                        $this->db->bind(':naceID', $naceID);
                        $this->db->bind(':genderID', $genderID);
                        $this->db->bind(':pYear', $year);
                        $this->db->bind(':workplaceValue', $item->workVal);
                        $this->db->bind(':livingplaceValue', $item->livVal);
                        $this->db->bind(':employmentBalance', $item->balance);
                        $this->db->execute();
                    }
                }
            }
            return $this->db->commit();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            throw $ex;
        }
    }

    private function insertEmploymentRatio($dataSet, $variableID) {
        $sql = <<<'SQL'
INSERT INTO EmploymentRatio (variableID, municipalityID, genderID, ageRangeID, pYear, employmentPercent)
VALUES (:variableID, :municipalityID, :genderID, :ageRangeID, :pYear, :employmentPercent);
SQL;
        try {
            $this->db->beginTransaction();
            $region = [];
            foreach ($dataSet as $item) {
                $municipalityID = $this->core->getMunicipalityID($item->Region);
                $genderID = $this->core->getGenderID($item->Kjonn);
                $ageRangeID = $this->core->getAgeRangeID($item->Alder);
                $pYear = $item->Tid;
                $employmentPercent = $item->value;
                if (in_array($item->Region, $this->core->getRegionCodes())) {
                    if (!isset($region[$pYear][$genderID][$ageRangeID])) {$region[$pYear][$genderID][$ageRangeID] = 0; }
                    $region[$pYear][$genderID][$ageRangeID] += $employmentPercent;
                }
                $this->db->prepare($sql);
                $this->db->bind(':variableID', $variableID);
                $this->db->bind(':municipalityID', $municipalityID);
                $this->db->bind(':genderID', $genderID);
                $this->db->bind(':ageRangeID', $ageRangeID);
                $this->db->bind(':pYear', $pYear);
                $this->db->bind(':employmentPercent', $employmentPercent);
                $this->db->execute();

            }
            $regionID = $this->core->getMunicipalityID('9999');
            foreach ($region as $pYear => $item3) {
                foreach ($item3 as $genderID => $item2) {
                    foreach ($item2 as $ageRangeID => $item) {
                        $this->db->prepare($sql);
                        $this->db->bind(':variableID', $variableID);
                        $this->db->bind(':municipalityID', $regionID);
                        $this->db->bind(':genderID', $genderID);
                        $this->db->bind(':ageRangeID', $ageRangeID);
                        $this->db->bind(':pYear', $pYear);
                        $this->db->bind(':employmentPercent', $item / 3);
                        $this->db->execute();
                    }
                }
            }
            return $this->db->commit();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    /**
     * @param $dataSet
     * @param $variableID
     * @return bool
     * @throws PDOException
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
            return $this->db->commit();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            throw $ex;
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
            $region = [];
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $pYear = $item->Tid;
                $hid = $householdTypes[strval($item->HusholdType)];
                $municipalityID = $this->core->getMunicipalityID($item->Region);
                $this->db->prepare($sql);
                $this->db->bind(':varID', $variableID);
                $this->db->bind(':munID', $municipalityID);
                $this->db->bind(':householdTypeID', $hid);
                $this->db->bind(':pYear', $pYear);
                $this->db->bind(':householdIncomeAvg', $item->value);
                $this->db->execute();

                if (in_array($item->Region, $this->core->getRegionCodes())) {
                    if (!isset($region[$pYear])) {$region[$pYear] = []; }
                    if (!isset($region[$pYear][$hid])) {$region[$pYear][$hid] = 0; }
                    $region[$pYear][$hid] += $item->value;
                }
            }
            foreach ($region as $pYear => $item2) {
                foreach ($item2 as $hid => $item) {
                    $this->db->prepare($sql);
                    $this->db->bind(':varID', $variableID);
                    $this->db->bind(':munID', $this->core->getMunicipalityID('9999'));
                    $this->db->bind(':householdTypeID', $hid);
                    $this->db->bind(':pYear', $pYear);
                    $this->db->bind(':householdIncomeAvg', $item / 3);
                    $this->db->execute();
                }
            }
            return $this->db->commit();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    private function insertImmigration($dataSet, $variableID) {
        $sql = <<<'SQL'
INSERT INTO Immigration (variableID, municipalityID, pYear, incomingAll, outgoingAll, sumAll) 
VALUES (:variableID, :municipalityID, :pYear, :incomingAll, :outgoingAll, :sumAll);
SQL;
        try {
            $temp = [];
            $region = [];
            foreach ($dataSet as $item) {
                $temp[$item->Region][$item->Tid][$item->ContentsCode] = $item->value;
            }
            $this->db->beginTransaction();
            foreach ($temp as $muni => $item2) {
                foreach ($item2 as $pYear => $item) {
                    $municipalityID = $this->core->getMunicipalityID($muni);
                    $this->db->prepare($sql);
                    $this->db->bind(':variableID', $variableID);
                    $this->db->bind(':municipalityID', $municipalityID);
                    $this->db->bind(':pYear', $pYear);
                    $this->db->bind(':incomingAll', $item['Innflytting']);
                    $this->db->bind(':outgoingAll', $item['Utflytting']);
                    $this->db->bind(':sumAll', $item['Netto']);
                    $this->db->execute();
                    if (in_array($muni, $this->core->getRegionCodes())) {
                        if (!isset($region[$pYear])) {
                            $region[$pYear]['Innflytting'] = 0;
                            $region[$pYear]['Utflytting'] = 0;
                            $region[$pYear]['Netto'] = 0;
                        }
                        $region[$pYear]['Innflytting'] += $item['Innflytting'];
                        $region[$pYear]['Utflytting'] += $item['Utflytting'];
                        $region[$pYear]['Netto'] += $item['Netto'];
                    }
                }
            }
            $regionID = $this->core->getMunicipalityID('9999');
            foreach ($region as $pYear => $item) {
                $this->db->prepare($sql);
                $this->db->bind(':variableID', $variableID);
                $this->db->bind(':municipalityID', $regionID);
                $this->db->bind(':pYear', $pYear);
                $this->db->bind(':incomingAll', $item['Innflytting']);
                $this->db->bind(':outgoingAll', $item['Utflytting']);
                $this->db->bind(':sumAll', $item['Netto']);
                $this->db->execute();
            }
            return $this->db->commit();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            throw $ex;
        }
    }

    private function insertImmigrantPopulation($dataSet, $variableID) {
        $sql = <<<'SQL'
INSERT INTO ImmigrantPopulation (variableID, municipalityID, genderID, countryBackgroundID, pYear, persons) 
VALUES (:variableID, :municipalityID, :genderID, :countryBackgroundID, :pYear, :persons);
SQL;
        try {
            $countryBackgrounds = [];
            $sqlCountryBackgrounds = 'SELECT countryBackgroundID, countryBackgroundCode FROM CountryBackground';
            $this->db->query($sqlCountryBackgrounds);
            $countries = $this->db->getResultSet();
            foreach ($countries as $country) {
                $countryBackgrounds[strval($country['countryBackgroundCode'])] = $country['countryBackgroundID'];
            }
            $region = [];
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $municipalityID = $this->core->getMunicipalityID($item->Region);
                $genderID = $this->core->getGenderID($item->Kjonn);
                $countryBackgroundID = $countryBackgrounds[$item->Landbakgrunn];
                $pYear = $item->Tid;
                $this->db->prepare($sql);
                $this->db->bind(':variableID', $variableID);
                $this->db->bind(':municipalityID', $municipalityID);
                $this->db->bind(':genderID', $genderID);
                $this->db->bind(':countryBackgroundID', $countryBackgroundID);
                $this->db->bind(':pYear', $pYear);
                $this->db->bind(':persons', $item->value);
                $this->db->execute();
                if (in_array($item->Region, $this->core->getRegionCodes())) {
                    if (!isset($region[$pYear][$genderID][$countryBackgroundID])) {
                        $region[$pYear][$genderID][$countryBackgroundID] = 0;
                    }
                    $region[$pYear][$genderID][$countryBackgroundID] += $item->value;
                }
            }
            $regionID = $this->core->getMunicipalityID('9999');
            foreach ($region as $pYear => $item3) {
                foreach ($item3 as $genderID => $item2) {
                    foreach ($item2 as $countryBackgroundID => $item) {
                        $this->db->prepare($sql);
                        $this->db->bind(':variableID', $variableID);
                        $this->db->bind(':municipalityID', $regionID);
                        $this->db->bind(':genderID', $genderID);
                        $this->db->bind(':countryBackgroundID', $countryBackgroundID);
                        $this->db->bind(':pYear', $pYear);
                        $this->db->bind(':persons', $item);
                        $this->db->execute();
                    }
                }
            }
            return $this->db->commit();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            throw $ex;
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
        $regionObj = '{"incoming": 0, "outgoing": 0, "sum": 0}';
        $region = [];
        foreach ($dataSet as $item) {
            $res[$item->Tid][$item->Region][$item->ContentsCode] = $item->value;
            if (in_array($item->Region, $this->core->getRegionCodes())) {
                if (!isset($region[$item->Tid])) {$region[$item->Tid] = json_decode($regionObj); }
            }
        }
        $sql = 'INSERT INTO Movement (variableID, municipalityID, pYear, incomingAll, outgoingAll, sumAll) VALUES (:varID, :munID, :pYear, :incoming, :outgoing, :sum)';
        try {
            $this->db->beginTransaction();
            foreach ($res as $year => $outer1) {
                foreach ($outer1 as $munic => $data) {
                    $this->db->prepare($sql);
                    $this->db->bind(':varID', $variableID);
                    $this->db->bind(':munID', $this->core->getMunicipalityID(strval($munic)));
                    $this->db->bind(':pYear', $year);
                    $this->db->bind(':incoming', $data['Innflytting']);
                    $this->db->bind(':outgoing', $data['Utflytting']);
                    $this->db->bind(':sum', $data['Netto']);
                    $this->db->execute();
                    if (in_array($munic, $this->core->getRegionCodes())) {
                        $region[$year]->incoming += $data['Innflytting'];
                        $region[$year]->outgoing += $data['Utflytting'];
                        $region[$year]->sum += $data['Netto'];
                    }
                }
            }
            $regionID = $this->core->getMunicipalityID('9999');
            foreach ($region as $year => $item) {
                $this->db->prepare($sql);
                $this->db->bind(':varID', $variableID);
                $this->db->bind(':munID', $regionID);
                $this->db->bind(':pYear', $year);
                $this->db->bind(':incoming', $item->incoming);
                $this->db->bind(':outgoing', $item->outgoing);
                $this->db->bind(':sum', $item->sum);
                $this->db->execute();
            }
            return $this->db->commit();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            return $ex;
        }
    }

    private function insertMunicipalEconomy($dataSet, $variableID) {
        $sql = <<<'SQL'
INSERT INTO MunicipalEconomy (variableID, municipalityID, municipalIncomeCategoryID, pYear, income) 
VALUES (:variableID, :municipalityID, :municipalIncomeCategoryID, :pYear, :income);
SQL;
        try {
            $municipalIncomeCategories = [];
            $incomeCategorySql = 'SELECT municipalIncomeCategoryID, municipalIncomeCategoryCode FROM MunicipalIncomeCategory';
            $this->db->query($incomeCategorySql);
            foreach ($this->db->getResultSet() as $item) {
                $municipalIncomeCategories[strval($item['municipalIncomeCategoryCode'])] = $item['municipalIncomeCategoryID'];
            }
            $this->db->beginTransaction();
            $temp = [];
            foreach ($dataSet as $item) {
                $temp[$item->Region][$item->Tid][$item->ContentsCode] = $item->value;
            }
            $regionSet = [];
            foreach ($temp as $region => $item3) {
                foreach ($item3 as $pYear => $item2) {
                    foreach ($item2 as $municipalIncomeCategoryCode => $value) {
                        $municipalityID = $this->core->getMunicipalityID($region);
                        $municipalIncomeCategoryID = $municipalIncomeCategories[$municipalIncomeCategoryCode];
                        $this->db->prepare($sql);
                        $this->db->bind(':variableID', $variableID);
                        $this->db->bind(':municipalityID', $municipalityID);
                        $this->db->bind(':municipalIncomeCategoryID', $municipalIncomeCategoryID);
                        $this->db->bind(':pYear', $pYear);
                        $this->db->bind(':income', $value);
                        $this->db->execute();
                        if (in_array($region, $this->core->getRegionCodes())) {
                            if (!isset($regionSet[$pYear])) {$regionSet[$pYear] = []; }
                            if (!isset($regionSet[$pYear][$municipalIncomeCategoryID])) {
                                $regionSet[$pYear][$municipalIncomeCategoryID] = 0; }
                            $regionSet[$pYear][$municipalIncomeCategoryID] += $value;
                        }
                    }
                }
            }
            $regionID = $this->core->getMunicipalityID($this->core->getRegionUmbrellaCode());
            foreach ($regionSet as $pYear => $item2) {
                foreach ($item2 as $municipalIncomeCategoryID => $value) {
                    $this->db->prepare($sql);
                    $this->db->bind(':variableID', $variableID);
                    $this->db->bind(':municipalityID', $regionID);
                    $this->db->bind(':municipalIncomeCategoryID', $municipalIncomeCategoryID);
                    $this->db->bind(':pYear', $pYear);
                    $this->db->bind(':income', $value);
                    $this->db->execute();
                }
            }

            return $this->db->commit();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            throw $ex;
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
            return $this->db->commit();
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
            return $this->db->commit();
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
        $sqlMunicipalExpenseCategories = 'SELECT municipalExpenseCategoryID, municipalExpenseCategoryCode FROM MunicipalExpenseCategory';
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
            return $this->db->commit();
        } catch (PDOException $ex) {
            $this->db->rollbackTransaction();
            throw $ex;
        }
    }

    private function insertPopulationAge($dataSet, $variableID) {
        $sql = <<<'SQL'
INSERT INTO PopulationAge (variableID, municipalityID, ageRangeID, genderID, pYear, population)
VALUES (:variableID, :munID, :ageRangeID, :genderID, :pYear, :population);
SQL;
        $region = [];
        $this->db->beginTransaction();
        foreach ($dataSet as $item) {
            $municipalityID = $this->core->getMunicipalityID($item->Region);
            $ageRangeID = $this->core->getAgeRangeID($item->Alder);
            $genderID = $this->core->getGenderID($item->Kjonn);
            $pYear = $item->Tid;
            if (in_array($item->Region, $this->core->getRegionCodes())) {
                if (!isset($region[$pYear][$ageRangeID][$genderID])) {$region[$pYear][$ageRangeID][$genderID] = 0; }
                $region[$pYear][$ageRangeID][$genderID] += $item->value;
            }
            $this->db->prepare($sql);
            $this->db->bind(':variableID', $variableID);
            $this->db->bind(':munID', $municipalityID);
            $this->db->bind(':ageRangeID', $ageRangeID);
            $this->db->bind(':genderID', $genderID);
            $this->db->bind(':pYear', $pYear);
            $this->db->bind(':population', $item->value);
            $this->db->execute();
        }
        $regionID = $this->core->getMunicipalityID('9999');
        foreach ($region as $pYear => $item3) {
            foreach ($item3 as $ageRangeID => $item2) {
                foreach ($item2 as $genderID => $item) {
                    $this->db->prepare($sql);
                    $this->db->bind(':variableID', $variableID);
                    $this->db->bind(':munID', $regionID);
                    $this->db->bind(':ageRangeID', $ageRangeID);
                    $this->db->bind(':genderID', $genderID);
                    $this->db->bind(':pYear', $pYear);
                    $this->db->bind(':population', $item);
                    $this->db->execute();

                }
            }
        }
        return $this->db->commit();
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
        $jsonObject = <<<'JSON'
{"dead": 0, "born": 0, "total": 0}
JSON;

        try {
            $regionSet = array();
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
                    if (in_array($item->Region, $this->core->getRegionCodes())) {
                        if (!isset($regionSet[$year][$quarter])) {$regionSet[$year][$quarter] = new stdClass(); }
                        if (!isset($regionSet[$year][$quarter]->dead)) {$regionSet[$year][$quarter]->dead = 0;}
                        if (!isset($regionSet[$year][$quarter]->born)) {$regionSet[$year][$quarter]->born = 0;}
                        if (!isset($regionSet[$year][$quarter]->total)) {$regionSet[$year][$quarter]->total = 0;}
                        $regionSet[$year][$quarter]->dead += $temp[$municipalityID][$year][$quarter]->dead;
                        $regionSet[$year][$quarter]->born += $temp[$municipalityID][$year][$quarter]->born;
                        $regionSet[$year][$quarter]->total += $temp[$municipalityID][$year][$quarter]->total;
                    }
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
            $regionID = $this->core->getMunicipalityID('9999');
            foreach ($regionSet as $year => $qVal) {
                foreach ($qVal as $quarter => $item) {
                    $this->db->prepare($sql);
                    $this->db->bind(':variableID', $variableID);
                    $this->db->bind(':munID', $regionID);
                    $this->db->bind(':pYear', $year);
                    $this->db->bind(':pQuarter', $quarter);
                    $this->db->bind(':born', $item->born);
                    $this->db->bind(':dead', $item->dead);
                    $this->db->bind(':total', $item->total);
                    $this->db->execute();
                }
            }
            return $this->db->commit();
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
        $region = [];
        try {
            $this->db->beginTransaction();
            foreach ($dataSet as $item) {
                $pYear = substr($item->Tid, 0, 4);
                $pMonth = substr($item->Tid, 5);
                $ageRangeID = $this->core->getAgeRangeID($item->Alder);
                $this->db->prepare($sql);
                $this->db->bind(':varID', $variableID);
                $this->db->bind(':munID', $this->core->getMunicipalityID($item->Region));
                $this->db->bind(':ageRangeID', $ageRangeID);
                $this->db->bind(':pYear', $pYear);
                $this->db->bind(':pMonth', $pMonth);
                $this->db->bind(':unemploymentPercent', $item->value);
                $this->db->execute();
                if (in_array($item->Region, $this->core->getRegionCodes())) {
                    if (!isset($region[$pYear])) {$region[$pYear] = []; }
                    if (!isset($region[$pYear][$pMonth])) {$region[$pYear][$pMonth] = []; }
                    if (!isset($region[$pYear][$pMonth][$ageRangeID])) {$region[$pYear][$pMonth][$ageRangeID] = 0; }
                    $region[$pYear][$pMonth][$ageRangeID] += $item->value;
                }
            }
            foreach ($region as $pYear => $item3) {
                foreach ($item3 as $pMonth => $item2) {
                    foreach ($item2 as $ageRangeID => $item) {
                        $this->db->prepare($sql);
                        $this->db->bind(':varID', $variableID);
                        $this->db->bind(':munID', $this->core->getMunicipalityID('9999'));
                        $this->db->bind(':ageRangeID', $ageRangeID);
                        $this->db->bind(':pYear', $pYear);
                        $this->db->bind(':pMonth', $pMonth);
                        $this->db->bind(':unemploymentPercent', $item / 3);
                        $this->db->execute();
                    }
                }
            }
            return $this->db->commit();

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