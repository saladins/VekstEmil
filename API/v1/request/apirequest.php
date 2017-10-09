<?php

class APIrequest {
    /** @var DatabaseHandler */
    private $db;
    private $logger;
    private $mysqltime;
    private $groupBy = [];
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
     * @param RequestModel $request
     * @throws Exception
     */
    public function checkRequestOrDie($request) {
        $validate = new Validate($this->db);
        $validate->checkRequestOrDie($request);
    }

    /**
     * @param RequestModel $request
     */
    public function getVariableData($request) {
        $sql = '';
        $selectExtra = '';
        foreach (columnMap::columns() as $column) {
            foreach ($request as $key => $value) {
                if (isset($value) && $value != null && strtolower($column) == strtolower($value)) {
                    $selectExtra .= $request->tableName . '.' . $column .',';
                }
            }
        }
        switch ($request->tableName) {
            case TableMap::getTableMap()[30]: // 'PopulationAge':
                $sql = <<<SQL
SELECT 
PopulationAge.municipalityID,
$selectExtra
pYear,
sum(Population) as value
  from PopulationAge
SQL;
                break;
            case TableMap::getTableMap()[31]: // 'PopulationChange':
                $sql = <<<SQL
SELECT PopulationChangeID,
PopulationChange.municipalityID,
{$this->_SQLmunicipalityName()},
pYear,
pQuarter,
Born,
Dead,
TotalPopulation as value
from PopulationChange, Municipality
WHERE PopulationChange.MunicipalityID = Municipality.MunicipalityID
SQL;
                break;
            case TableMap::getTableMap()[23]: // Movement
                $sql = <<<SQL
SELECT MovementID,
{$this->_SQLmunicipalityName()},
pYear,
IncomingAll, OutgoingAll, SumAll
from Movement, Municipality
WHERE Movement.MunicipalityID = Municipality.MunicipalityID
SQL;
                break;
            case TableMap::getTableMap()[9]: // Employment
                $sql = <<<SQL
SELECT EmploymentID,
{$this->_SQLmunicipalityName()},
{$this->_SQLnace()},
{$this->_SQLgender()},
pYear,
WorkplaceValue,
LivingplaceValue,
EmploymentBalance
from Employment, Municipality, Nace2007, Gender
WHERE Employment.MunicipalityID = Municipality.MunicipalityID
AND Employment.NaceID = Nace2007.NaceID
AND Employment.GenderID = Gender.GenderID
SQL;
                break;
            case TableMap::getTableMap()[6]: // CommuteBalance
                $sql = <<<SQL
SELECT CommuteBalanceID,
Municipality.MunicipalityName as WorkingMunicipality,
Municipality.MunicipalityName as LivingMunicipality,
pYear,
Commuters
FROM CommuteBalance, Municipality
WHERE CommuteBalance.LivingMunicipalityID = Municipality.MunicipalityID
AND CommuteBalance.WorkingMunicipalityID = Municipality.MunicipalityID
SQL;
                break;
            case TableMap::getTableMap()[39]: // Unemployment
                $sql = <<<SQL
SELECT UnemploymentID,
municipalityID,
ageRangeID,
pYear,
pMonth,
unemploymentPercent
FROM Unemployment
SQL;
                break;
            case TableMap::getTableMap()[10]: // EmploymentRatio
                $sql = <<<SQL
SELECT EmploymentRatioID,
{$this->_SQLmunicipalityName()},
{$this->_SQLgender()},
{$this->_SQLageRanges()},
pYear,
EmploymentPercent
from EmploymentRatio
WHERE EmploymentRatio.MunicipalityID = MunicipalityID
AND EmploymentRatio.GenderID = Gender.GenderID
AND EmploymentRatio.AgeRangeID = AgeRange.AgeRangeID
SQL;
                break;
            case TableMap::getTableMap()[20]: // HomeBuildingArea
                $sql = <<<SQL
SELECT HomeBuildingAreaID,
{$this->_SQLmunicipalityName()},
BuildingStatus.BuildingStatusText as BuildingStatusText,
BuildingCategory.BuildingCategoryText as BuildingCategoryText,
pYear,
pQuarter,
HomeBuildingValue
FROM HomeBuildingArea, Municipality, BuildingStatus, BuildingCategory
WHERE HomeBuildingArea.MunicipalityID = Municipality.MunicipalityID
AND HomeBuildingArea.BuildingStatusID = BuildingStatus.BuildingStatusID
AND HomeBuildingArea.BuildingCategoryID = BuildingCategory.BuildingCategoryID
SQL;
                break;
            case TableMap::getTableMap()[17]: // FunctionalBuildingArea
                $sql = <<<SQL
SELECT FunctionalBuildingAreaID,
{$this->_SQLmunicipalityName()},
BuildingStatus.BuildingStatusText as BuildingStatusText,
BuildingCategory.BuildingCategoryText as BuildingCategoryText,
pYear,
pQuarter,
FuncBuildingValue
FROM FunctionalBuildingArea, Municipality, BuildingStatus, BuildingCategory
WHERE FunctionalBuildingArea.MunicipalityID = Municipality.MunicipalityID
AND FunctionalBuildingArea.BuildingStatusID = BuildingStatus.BuildingStatusID
AND FunctionalBuildingArea.BuildingCategoryID = BuildingCategory.BuildingCategoryID
SQL;
                break;
            case TableMap::getTableMap()[35]: // Proceeding
                $sql = <<<SQL
SELECT ProceedingID,
{$this->_SQLmunicipalityName()},
ProceedingCategory.ProceedingText as ProceedingText,
ProceedingValueType.ProceedingValueTypeText as ProceedingText,
pYear,
ProceedingValue
FROM Proceeding, Municipality, ProceedingCategory, ProceedingValueType
WHERE Proceeding.MunicipalityID = Municipality.MunicipalityID
AND Proceeding.ProceedingCategoryID = ProceedingCategory.ProceedingCategoryID
AND Proceeding.ProceedingValueTypeID = ProceedingValueType.ProceedingValueTypeID
SQL;
                break;
            case TableMap::getTableMap()[34]: // PrivateEmployee
                $sql = <<<SQL

SQL;
                break;
        }
        $sql .= $this->getSqlConstraints($request);
        $sql .= $this->getGroupByClause($request);
        $this->db->query($sql);
        $result = $this->db->getResultSet();
        if (isset($result[0]['value'])) {
            for ($i = 0; $i < sizeof($result); $i++) {
                $var = $result[$i]['value'];
                if (is_numeric($var)) {
                    $result[$i]['value'] = intval($var);
                }
            }
        }
        return $result;
    }

    public function getMetaData($request) {
        $ret = new stdClass();
        if (isset($request->variableID) && $request->variableID != null) {
            $ret->variable = $this->getVariableAndProvider($request->variableID);
            $ret->tags = $this->getTagsForVariable($request->variableID);
            $ret->linkedDocuments = $this->getLinkedDocuments($request->variableID);
            $ret->relatedVariables = $this->getVariablesRelated($request->variableID);
            $ret->description = $this->getVariableDescription($request->variableID);
            $ret->params = $request;
        }
        return $ret;
    }

    public function getMinimalMetaData($request) {
        $ret = new stdClass();
        if (isset($request->variableID) && $request->variableID != null) {
            $fieldDescAndConstraints = $this->getFieldDescriptionsAndConstraints($request);
            $ret->constraints = $fieldDescAndConstraints[0];
            $ret->descriptions = $fieldDescAndConstraints[1];
            $ret->variable = $this->getVariableAndProvider($request->variableID);
        }
        return $ret;
    }

    private function getFieldDescriptionsAndConstraints($request) {
        $ret = array();
        $constraints = new stdClass();
        $descriptions = new stdClass();
//        $ret->constraints = $constraints;
//        $ret->descriptions = $descriptions;
        foreach (columnMap::columns() as $column) {
            $sqlCheckIfColumn = 'SHOW COLUMNS FROM ' . $request->tableName . " LIKE '$column' ";
            $this->db->query($sqlCheckIfColumn);
            if ($this->db->getResultSet()) {
                $sqlGetConstraints = 'SELECT DISTINCT ' . $column . ' FROM ' . $request->tableName;
                $sqlGetConstraints .= $this->getSqlConstraints($request);
                $this->db->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
                $this->db->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, false);
                $this->db->query($sqlGetConstraints);
                $constraints->{$column} = $this->db->getResultSet(PDO::FETCH_NUM);
                $descriptions->{$column} = $this->getFieldDescriptions($column);
            }
        }
        $ret[0] = $constraints;
        $ret[1] = $descriptions;;
        return $ret;
    }

    private function getFieldDescriptions($columnName) {
        if (columnMap::columnsTableParent()[$columnName] == null ) {
            return null;
        }
        $sql = 'SELECT DISTINCT * FROM ' . columnMap::columnsTableParent()[$columnName];
        $this->db->query($sql);
        return $this->db->getResultSet(PDO::FETCH_CLASS);
    }

    private function getVariableAndProvider($variableID) {
        $sql = <<<SQL
SELECT a.providerID, b.providerName, b.providerNameShortForm, b.providerNotice, b.providerLink, b.providerAPIAddress, a.statisticName, a.tableName, a.lastUpdatedDate, a.providerCode 
FROM Variable a, VariableProvider b
WHERE a.providerID = b.providerID
AND   a.variableID = $variableID
SQL;
        $this->db->query($sql);
        return $this->db->getResultSet(PDO::FETCH_CLASS)[0];
    }
    private function getTagsForVariable($variableID) {
        $sql = <<<SQL
SELECT a.variableID, a.tagID, b.tagText 
FROM VariableTagList a, VariableTag b
WHERE a.tagID = b.tagID
AND   a.variableID = $variableID
SQL;
        $this->db->query($sql);
        return $this->db->getResultSet(PDO::FETCH_CLASS);
    }
    private function getLinkedDocuments($variableID) {
        $sql = <<<SQL
SELECT a.linkedDocumentID, a.linkedDocumentAddress, a.linkedDocumentTitle, a.linkedDocumentDescription
FROM VariableLinkedDocument a
WHERE a.variableID = $variableID 
SQL;
        $this->db->query($sql);
        return $this->db->getResultSet(PDO::FETCH_CLASS);
    }
    private function getVariablesRelated($variableID) {
        $sql = <<<SQL
SELECT a.relatedVariableID, b.statisticName, c.subCategoryName, c.iconType, c.iconData
FROM VariableRelated a, Variable b, VariableSubCategory c
WHERE a.relatedVariableID = b.variableID
AND b.subCategoryID = c.subCategoryID
AND a.parentVariableID = $variableID;
SQL;
        $this->db->query($sql);
        return $this->db->getResultSet(PDO::FETCH_CLASS);
    }
    private function getVariableDescription($variableID) {
        $sql = <<<SQL
SELECT a.descriptionID, a.variableID, a.descriptionText 
FROM VariableDescription a, Variable b
WHERE a.variableID = b.variableID
AND b.variableID = $variableID
SQL;
        $this->db->query($sql);
        $this->logger->log('getVariableDescription sql: ' . $sql);
        return $this->db->getResultSet(PDO::FETCH_CLASS);

    }

    public function metaIncludeAfter() {
        return $this->groupBy;
    }

    /**
     * @param $request
     * @return string
     */
    private function getSqlConstraints($request) {
        $sql = '';
        foreach ($request as $key => $value) {
            if ($this->in_arrayi($key, columnMap::columns()) && is_array($value)) {
                $sql .= $this->getSqlFromManyArgs($request->tableName, $key, $value);
                $sql .= ' AND ';
            }
        }
        return (strlen($sql) != 0 ? ' WHERE ' . substr($sql, 0, -5) : ''); // TODO hack! change to proper
    }

    /**
     * @param RequestModel $request
     * @return string
     */
    private function getGroupByClause($request) {
        $ret = ' GROUP BY ';
        $sqlGetTableColumns = 'SHOW COLUMNS FROM ' . $request->tableName;
        $this->db->query($sqlGetTableColumns);
        $dbResult = $this->db->getResultSet();
        if (isset($request->groupBy) && is_array($request->groupBy)) {
            foreach ($dbResult['Field'] as $column) {
                if (in_array($column, $request->groupBy)) {
                    $ret .= ' ' . $column;
                    array_push($this->groupBy, $column);
                    if (!end($dbResult)) {
                        $ret .= ', ';
                    }
                }
            }
        } else {
            $municipalityID = false;
            for ($i = 0; $i < sizeof($dbResult); $i++) {
                if (strtolower($dbResult[$i]['Field']) == strtolower(columnMap::columns()[0])) {
                    $ret .= columnMap::columns()[0];
                    array_push($this->groupBy, columnMap::columns()[0]);
                    $municipalityID = true;
                }
            }
            if (!$municipalityID) {
                $ret .= columnMap::columns()[6];
                array_push($this->groupBy, columnMap::columns()[6]);
            }
            $ret .= ', ' . columnMap::columns()[3];
            array_push($this->groupBy, columnMap::columns()[3]);
        }
        return $ret;
    }

    private function getSqlFromManyArgs($tableName, $key, $values) {
        $sql = '';
        if (!is_array($values)) return $sql;
        $last = end($values);
        foreach ($values as $value) {
            $sql .= "$tableName.$key=$value";
            if ($value != $last) {
                $sql .= ' OR';
            }
        }
        return $sql;
    }



    private function MunicipalityName() { return ['Municipality.MunicipalityName', 'MunicipalityName']; }
    private function _SQLmunicipalityName() { return $this->MunicipalityName()[0] . ' AS ' . $this->MunicipalityName()[1]; }
    private function AgeRanges() { return ['concat_ws("-", AgeRange.AgeRangeStart, AgeRange.AgeRangeEnd)', 'Ages']; }
    private function _SQLageRanges() { return $this->AgeRanges()[0] . ' AS ' . $this->AgeRanges()[1]; }
    private function Gender() { return ['Gender.GenderTextSingular', 'Gender'];}
    private function _SQLgender() { return $this->Gender()[0]. ' AS ' . $this->Gender()[1]; }
    private function Nace2007() { return ['Nace2007.NaceText', 'Nace']; }
    private function _SQLnace() { return [$this->Nace2007()[0] . ' AS ' . $this->Nace2007()[1]]; }

    /**
     * @param RequestModel $request
     * @return stdClass
     * @throws Exception
     */
    public function getAuxiliary($request) {
        if (isset($request->tableNumber) && (isset($request->variableID)) && $request->tableNumber != null && $request->variableID != null) {
//            $ret = new stdClass();
            $this->logger->log('Serving getAuxiliary with tableNumber ' .
                print_r($request->tableNumber, true) . ' and variableID ' .
                print_r($request->variableID, true));
            switch ($request->tableNumber) {
                case 42: // Description
                    return $this->getVariableDescription($request->variableID);
                case 44: // VariableLinkedDocument
                    return $this->getLinkedDocuments($request->variableID);
                case 47: // VariableRelated
                    return $this->getVariablesRelated($request->variableID);
            }
        } else {
            throw new Exception('Missing or invalid table name and variable ID');
        }
    }









//    public function parseTable($request) { // todo deleteme
//        $tableNumber = -1;
//        if (isset($request->tableNumber) && is_numeric($request->tableNumber)) {
//            $tableNumber = $request->tableNumber;
//        } else {
//            foreach (TableMap::getTableMap() as $key => $value) {
//                if ($value == $request->tableName) {
//                    $tableNumber = $key;
//                    break;
//                }
//            }
//        }
//        if ($tableNumber == -1) {
//            echo json_encode('unable to determine table number'); die;
//        }
//        switch ($tableNumber) {
//            case 45:
//            case 48:
//                return $this->getGenericTable($request);
//                break;
//            case 30:
//                return $this->PopulationAge($request);
//                break;
//            case 1:
//            case 3:
//            case 4:
//            case 8:
//            case 11:
//            case 18:
//            case 19:
//            case 22:
//            case 28:
//            case 33:
//            case 36:
//            case 37:
//            case 38:
//            case 42:
//            case 43:
//            case 44:
//            case 45:
//            case 46:
//            case 47:
//            case 48:
//            case 49:
//            case 50:
//            case 51:
//            case 52:
//                // TODO: Non-queryable tables
//                // TODO: Return error on query
//                return null;
//                break;
//
//        }
//
//    }
//
//    private function PopulationAge($request, $metaData = true) {
//        try {
//            $sql  = 'SELECT MunicipalityID as ID, AgeRangeID, pYear as Year, Population as Value from PopulationAge';
//            $sql .= ' WHERE ' . $this->parseArgsPreProcessor($request);
//            $this->db->query($sql);
//            return $this->db->getResultSet();
//
//        } catch (PDOException $ex) {
//            $this->db->DbhError($ex);
//        }
//    }

    public function getMenu($request) {
        try {
            $sql  = <<<SQL
SELECT VariableID as ID, VariableMasterCategory.MasterCategoryID as MasterCatID, 
VariableSubCategory.SubCategoryID as SubCatID, StatisticName, 
VariableSubCategory.Position as SubPosition, 
SubCategoryName, VariableMasterCategory.Position as MsPosition, 
MasterCategoryName, VariableSubCategory.IconType as SubCatIconType, 
VariableSubCategory.IconData as SubCatIconData 
FROM Variable, VariableSubCategory, VariableMasterCategory
WHERE Variable.SubCategoryID = VariableSubCategory.SubCategoryID 
AND VariableSubCategory.MasterCategoryID = VariableMasterCategory.MasterCategoryID
ORDER BY StatisticName;

SQL;
            $this->db->query($sql);
            return $this->db->getResultSet();
        } catch (PDOException $ex) {
            $this->db->DbhError($ex);
        }
    }
    public function getVariableMainData($request) {
        try {
            $ret = new stdClass();
            $sql = <<<SQL
SELECT a.variableID, a.subCategoryID, a.providerID, b.providerNameShortForm, a.descriptionID
a.statisticName, a.tableName, a.updateInterval, a.lastUpdatedDate, 
a.providerCode, a.isImplemented 
FROM Variable a, VariableProvider b, VariableDescription c
WHERE a.providerID = b.providerID
SQL;
            $this->db->query($sql);
            $ret->variable = $this->db->getResultSet();
            $sql = <<<SQL
SELECT 
SQL;

        } catch (PDOException $ex) {
            $this->db->DbhError($ex);
        }
    }
//
//    private function parseArgsPreProcessor($request) {
//        return ''; // TODO method stub, should parse arguments and return a SQL where clause sentence
//    }

//    private function parseNumArgs($args) {
//        $sqlWhere = '';
//        $sql = '';
//        $count = 0;
//        $total = 0;
//        foreach ($args as $key => $param) { // Counts to weed out invalid or no value entries
//            if (!$this->in_arr($key, requestInterface::getReserved())) {
//                if ($param != null) {
//                    $total++;
//                }
//            }
//        }
//        if (!isset($args->columnType)) {
//            $args->columnType = 20;
//        }
//        foreach ($args as $key => $param) {
//            if ($this->in_arr($key, requestInterface::getReserved()) || $param == null) continue;
//            var_dump($param);
//            $sql .= $this->parseColumnRequest($param->columnType, TableMap::getTableMap()[$args->tableNumber], $key, $param->values);
//            $count++;
//            if ($count < $total) $sql .= ' AND ';
//        }
//        return $sqlWhere . $sql;
//
//    }
//
//    private function in_arr($value, $array) {
//        foreach ($array as $item) {
//            if ($item == $value) return true;
//        }
//        return false;
//    }
//
//    private function parseColumnRequest($columnType, $tableName, $key, $values) {
//        switch($columnType) {
//            case 10: // Or
//                $ret = '';
//                foreach ($values as $value) {
//                    $ret .= " $tableName.$key = $value ";
//                    if ($value != end($values)) $ret .= ' OR ';
//                }
//                return $ret;
//                break;
//            case 20: // Exclusive
//                return " $tableName.$key = $values[0]";
//                break;
//            default:
//                throw new Exception('Invalid argument supplied for ColumnRequest parameter');
//        }
//    }



    /**
     * @return string[]
     */
    function getVariableTableNames() {
        try {
            // TODO Implement cache and return if recent
            $this->db->query('select TableName from Variable');
            return $this->db->getResultSet(PDO::FETCH_ASSOC);
        } catch (PDOException $ex) {
            $this->db->DbhError($ex);
        }
    }

    function in_arrayi($needle, $haystack) {
        return in_array(strtolower($needle), array_map('strtolower', $haystack));
    }

    /**
     * @param string[] $content
     */
    function output($content) {
        $options = null;
        if ($GLOBALS['debug']) {
            $options = JSON_UNESCAPED_UNICODE + JSON_UNESCAPED_SLASHES + JSON_NUMERIC_CHECK + JSON_PRETTY_PRINT;
        }
//        header('Content-Type: application/json', false);
        if ($content == null || !is_array($content) ||  count($content) == 0) {
        } else {
            $this->logger->log('Sending data to ' . $_SERVER['REMOTE_ADDR'] . ' with content ' . print_r($content, true));
            echo json_encode($content, null);
        }
    }

//$res = $db->query("select * from statistic");
//$tbl = $res->fetch(PDO::FETCH_ASSOC);
//echo implode(', ', $tbl);
//echo '<br>';
//echo json_encode($res->fetchAll(PDO::FETCH_ASSOC));
}