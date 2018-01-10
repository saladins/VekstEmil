<?php
class CoreMethods {
    /** @var DatabaseHandler */
    private $db;
    /** @var Logger */
    private $logger;
    /** @var array */
    private $municipalityMap;
    /** @var array */
    private $naceMap;
    /** @var array */
    private $ageRangeMap;
    /** @var array */
    private $genderMap;
    /** @var array */
    private $sectorMap;
    /** @var array */
    private $primaryValueMap;
    /** @var array */
    private $organizationTypeMap;
    /** @var array */
    private $enterprisePostCategoryMap;

    /**
     * CoreMethods constructor.
     * @param DatabaseHandler $db
     * @param Logger $logger
     */
    public function __construct($db, $logger) {
        $this->db = $db;
        $this->logger = $logger;
        $this->municipalityMap =            array();
        $this->naceMap =                    array();
        $this->ageRangeMap =                array();
        $this->genderMap =                  array();
        $this->sectorMap =                  array();
        $this->primaryValueMap =            array();
        $this->organizationTypeMap =        array();
        $this->enterprisePostCategoryMap =  array();
    }

    /**
     * @param integer $variableID
     * @param integer $timestamp
     * @return bool
     */
    public function setLastUpdatedTime($variableID, $timestamp) {
        $sql = 'UPDATE Variable SET lastUpdatedDate = :timeValue WHERE variableID = :variableID';
        $this->db->prepare($sql);
        $this->db->bind(':timeValue', Date(Globals::dateTimeFormat, $timestamp));
        $this->db->bind(':variableID', $variableID);
        return $this->db->execute();
    }

    /**
     * Gets the interal ID for provided municipality code.
     * Generates a local cache of municipality code and internal IDs.
     * @param string $regionCode
     * @return string
     */
    public function getMunicipalityID($regionCode) {
        if ($this->municipalityMap == []) {
            $sql = 'SELECT municipalityID, municipalityCode FROM Municipality';
            $this->db->query($sql);
            foreach ($this->db->getResultSet() as $result) {
                $this->municipalityMap[strval($result['municipalityCode'])] = $result['municipalityID'];
            }
        }
        if (!isset($this->municipalityMap[strval($regionCode)])) {
            $sql = 'INSERT INTO Municipality (municipalityCode, municipalityName) VALUES (:code, :name)';
            $this->db->beginTransaction();
            $this->db->prepare($sql);
            $this->db->bind(':code', strval($regionCode));
            $this->db->bind(':name', strval($regionCode));
            $this->db->execute();
            $municipalityID = $this->db->getLastInsertID();
            $this->db->endTransaction();
            $this->municipalityMap[strval($regionCode)] = $municipalityID;
            return $municipalityID;
        } else {
            return $this->municipalityMap[strval($regionCode)];
        }
    }

    /**
     * @param string $municipalityName
     * @return string
     */
    public function getMunicipalityRegionCode($municipalityName) {
        $sql = "SELECT municipalityCode FROM Municipality WHERE municipalityName LIKE '%$municipalityName%'";
        $this->db->query($sql);
        $res = strval($this->db->getSingleResult()['municipalityCode']);
        if (strlen($res) === 0) {
            return '-1';
        } else {
            return strval($this->db->getSingleResult()['municipalityCode']);
        }
    }

    /**
     * @param $enterprisePostCategoryCode
     * @return mixed
     */
    public function getEnterprisePostCategory($enterprisePostCategoryCode) {
        if ($this->enterprisePostCategoryMap == []) {
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

    /**
     * @param string $organizationTypeCode
     * @return string
     */
    public function getOrganizationTypeID($organizationTypeCode) {
        if ($this->organizationTypeMap == []) {
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
    public function getNaceID($naceCode) {
        if ($this->naceMap == []) {
            $sql = 'SELECT naceID, naceCodeStart, naceCodeEnd FROM Nace';
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
INSERT INTO Nace (naceCodeStart, naceCodeEnd, naceText)
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
     * @param $sectorCode
     * @return mixed
     */
    public function getSectorID($sectorCode) {
        if ($this->sectorMap == []) {
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
    public function getAgeRangeID($ageRange) {
        if ($this->ageRangeMap == []) {
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
    public function getGenderID($gender) {
        if ($this->genderMap == []) {
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
    public function getPrimaryValueName($tableName) {
        if ($this->primaryValueMap == []) {
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
    public function mapAgeAndReplace($dataSet) {
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
     * @param integer $variableID
     * @param integer $timeStamp
     * @param integer $updateReasonID
     * @param string $updateSource
     * @return bool
     */
    public function logDb($variableID, $timeStamp, $updateReasonID, $updateSource) {
        $sql = "INSERT into VariableUpdateLog (variableID, updateDate, updateReasonID, updateSource) 
                VALUES (:varID, :updateTime, :reason, :updateSource);";
        $this->db->prepare($sql);
        $this->db->bind(':varID', $variableID);
        $this->db->bind(':updateTime', date('Y-m-d H:i:s', $timeStamp));
        $this->db->bind(':reason', $updateReasonID);
        $this->db->bind(':updateSource', $updateSource);
        return $this->db->execute();
    }
}