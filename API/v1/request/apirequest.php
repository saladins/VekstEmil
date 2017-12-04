<?php
/** Class for API (GET) requests */
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
     * Validates request or throws exception
     * @param RequestModel $request
     * @throws Exception
     */
    public function checkRequestOrDie($request) {
        $validate = new Validate($this->db);
        $validate->checkRequestOrDie($request);
    }

    public function getDetailedData($request) {
        $sql = '';
        switch ($request->variableID) {
            case 8:
                $sql = <<<SQL
SELECT municipalityID, pYear, naceID, SUM(valueInNOK) AS value 
FROM EnterpriseEntry, Enterprise
WHERE Enterprise.enterpriseID = EnterpriseEntry.enterpriseID AND EnterpriseEntry.enterprisePostCategoryID = 7
GROUP BY municipalityID, pYear, naceID;
SQL;
                break;
            case 42:
                $sql = <<<SQL
SELECT municipalityID, naceID, MAX(pYear), sum(livingPlaceValue) AS value 
FROM EmploymentDetailed 
GROUP BY municipalityID, naceID;
SQL;

        }
        $this->logger->log($sql);
        $this->db->query($sql);
        $result = $this->db->getResultSet();
        if (isset($result[0]['value'])) {
            for ($i = 0; $i < sizeof($result); $i++) {
                $var = $result[$i]['value'];
                if (is_numeric($var)) {
                    if (is_double($var)) {
                        $result[$i]['value'] = floatval($var);
                    } else {
                        $result[$i]['value'] = intval($var);
                    }
                }
            }
        }
        return $result;
    }

    /**
     * Gets and returns data about at specific variable (data table)
     * @param RequestModel $request
     * @return array String array containing variable data
     */
    public function getVariableData($request) {
        $sql = '';
//        $selectExtra = '';
//        foreach (columnMap::columns() as $column) {
//            foreach ($request as $key => $value) {
//                if (isset($value) && !is_array($value) && $value != null && strtolower($column) == strtolower($value)) {
//                    $selectExtra .= $request->tableName . '.' . $column .',';
//                }
//            }
//        }
//        $this->logger->log('Extra requested: ' . print_r($selectExtra, true));
        switch ($request->tableName) {
            case TableMap::getTableMap()[2]: // Bankruptcy
                $sql = <<<SQL
SELECT 
municipalityID,
naceID,
pYear,
pQuarter,
bankruptcies as value
FROM Bankruptcy
SQL;

                break;
            case TableMap::getTableMap()[5]: // ClosedEnterprise
                $sql = <<<SQL
SELECT
municipalityID,
naceID,
pYear,
closedEnterprises as value
FROM ClosedEnterprise
SQL;
                break;

            case TableMap::getTableMap()[30]: // 'PopulationAge':
                $sql = <<<SQL
SELECT 
municipalityID,
ageRangeID,
genderID,
pYear,
population as value
from PopulationAge
SQL;
                break;
            case TableMap::getTableMap()[31]: // 'PopulationChange':
                $sql = <<<SQL
SELECT
municipalityID,
pYear,
pQuarter,
born,
dead,
totalPopulation as value
from PopulationChange
SQL;
                break;
            case TableMap::getTableMap()[23]: // Movement
                $sql = <<<SQL
SELECT
municipalityID,
pYear,
incomingAll as incoming, outgoingAll as outgoing, sumAll as value
from Movement 
SQL;
                break;
            case TableMap::getTableMap()[9]: // Employment
                $sql = <<<SQL
SELECT 
municipalityID,
naceID,
genderID,
pYear,
workplaceValue,
livingplaceValue,
employmentBalance
from Employment
SQL;
                break;
            case TableMap::getTableMap()[6]: // CommuteBalance
                $sql = <<<SQL
SELECT 
municipalityID,
workingMunicipalityID,
pYear,
commuters as value
FROM CommuteBalance
SQL;
                break;
            case TableMap::getTableMap()[39]: // Unemployment
                $sql = <<<SQL
SELECT unemploymentID,
municipalityID,
ageRangeID,
pYear,
pMonth,
unemploymentPercent as value
FROM Unemployment
SQL;
                break;
            case TableMap::getTableMap()[10]: // EmploymentRatio
                $sql = <<<SQL
SELECT employmentRatioID,
municipalityID,
genderID,
ageRangeID,
pYear,
EmploymentPercent as value
FROM EmploymentRatio
SQL;
                break;
            case TableMap::getTableMap()[20]: // HomeBuildingArea
                $sql = <<<SQL
SELECT 
municipalityID,
buildingStatusID,
buildingCategoryID,
pYear,
pQuarter,
buildingValue as value
FROM HomeBuildingArea
SQL;
                break;
            case TableMap::getTableMap()[17]: // FunctionalBuildingArea
                $sql = <<<SQL
SELECT 
municipalityID,
buildingStatusID,
buildingCategoryID,
pYear,
pQuarter,
buildingValue as value
FROM FunctionalBuildingArea
SQL;
                break;
            case TableMap::getTableMap()[35]: // Proceeding
                $sql = <<<SQL
SELECT
municipalityID,
proceedingCategoryID,
applicationTypeID,
pYear,
proceedingValue as value
FROM Proceeding
SQL;
                break;
            case TableMap::getTableMap()[34]: // PrivateEmployee
                $sql = <<<SQL

SQL;
                break;
            case TableMap::getTableMap()[29]: //NewEnterprise
                $sql = <<<SQL
SELECT 
municipalityID,
enterpriseCategoryID,
employeeCountRangeID,
pYear,
newEnterprises as value
FROM NewEnterprise
SQL;
                break;
            case TableMap::getTableMap()[21]: //HouseholdIncome
                $sql = <<<SQL
SELECT
municipalityID,
householdTypeID,
pYear,
householdIncomeAvg as value
FROM HouseholdIncome
SQL;
                break;
            case TableMap::getTableMap()[7]: //Education
                $sql = <<<SQL
SELECT 
municipalityID,
genderID,
gradeID,
pYear,
percentEducated as value
FROM Education
SQL;
                break;
            case TableMap::getTableMap()[25]: //RegionalCooperation
                $sql = <<<SQL
SELECT
municipalityID,
kostraCategoryID,
municipalExpenseCategoryID,
pYear,
expense as value
FROM RegionalCooperation
SQL;
                break;
            case TableMap::getTableMap()[56]: //EmploymentSector
                $sql = <<<SQL
SELECT
municipalityID,
naceID,
sectorID,
pYear,
workplaceValue,
livingplaceValue
FROM EmploymentSector
SQL;
                break;
            case TableMap::getTableMap()[57]:
                $sql = <<<SQL
SELECT 
municipalityID,
naceID,
employees,
enterpriseName,
organizationNumber,
organizationTypeID
FROM Enterprise
SQL;
                break;
            default:
                $tableName = $request->tableName;
                $sql = <<<SQL
SELECT * FROM $tableName
SQL;

        }
        $sql .= $this->getSqlConstraints($request);
        $sql .= $this->getGroupByClause($request);
        $this->logger->log($sql);
        $this->db->query($sql);
        $result = $this->db->getResultSet();
        if (isset($result[0]['value'])) {
            for ($i = 0; $i < sizeof($result); $i++) {
                $var = $result[$i]['value'];
                if (is_numeric($var)) {
                    if (is_double($var)) {
                        $result[$i]['value'] = floatval($var);
                    } else {
                        $result[$i]['value'] = intval($var);
                    }
                }
            }
        }
        return $result;
    }

    /**
     * DEPRECATED
     * Gets full meta data for the variable
     * @param $request
     * @return stdClass
     */
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

    /**
     * Gets constraints, descriptions and variable meta data for the variable (data table)
     * @param $request
     * @return stdClass
     */
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

    /**
     * Gets and returns constraints and descriptions for each column in the data table
     * @param $request
     * @return array
     */
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

    /**
     * Determines if there are returnable descriptions in child table. If there are, returns entire contents of said table.
     * @param $columnName
     * @return array|null
     */
    private function getFieldDescriptions($columnName) {
        if (columnMap::columnsTableParent()[$columnName] == null ) {
            return null;
        }
        $sql = 'SELECT DISTINCT * FROM ' . columnMap::columnsTableParent()[$columnName];
        $this->db->query($sql);
        return $this->db->getResultSet(PDO::FETCH_CLASS);
    }

    /**
     * Gets variable and provider metadata for the variable (data table)
     * @param $variableID
     * @return mixed
     */
    private function getVariableAndProvider($variableID) {
        $sql = <<<SQL
SELECT a.variableID, a.providerID, a.statisticName, a.tableName, 
a.lastUpdatedDate, a.providerCode, a.isImplemented, 
b.providerName, b.providerNameShortForm, b.providerNotice, 
b.providerLink, b.providerAPIAddress, c.subCategoryName
FROM Variable a, VariableProvider b, VariableSubCategory c
WHERE a.providerID = b.providerID
AND a.subCategoryID = c.subCategoryID
AND   a.variableID = $variableID
SQL;
        $this->db->query($sql);
        return $this->db->getResultSet(PDO::FETCH_CLASS)[0];
    }


    /**
     * Gets and returns list of tags for the variable (data table)
     * @param $variableID
     * @return array
     */
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


    /**
     * Gets links and document link descriptions for the variable (data table)
     * @param $variableID
     * @return array
     */
    private function getLinkedDocuments($variableID) {
        $sql = <<<SQL
SELECT a.linkedDocumentID, a.linkedDocumentAddress, a.linkedDocumentTitle, a.linkedDocumentDescription
FROM VariableLinkedDocument a
WHERE a.variableID = $variableID 
SQL;
        $this->db->query($sql);
        return $this->db->getResultSet(PDO::FETCH_CLASS);
    }

    /**
     * Gets list of internal variable IDs for related variables (data tables)
     * @param $variableID
     * @return array
     */
    private function getVariablesRelated($variableID) {
        $sql = <<<SQL
SELECT a.relatedVariableID, b.statisticName, c.subCategoryName, c.subCategoryID
FROM VariableRelated a, Variable b, VariableSubCategory c
WHERE a.relatedVariableID = b.variableID
AND b.subCategoryID = c.subCategoryID
AND a.parentVariableID = $variableID;
SQL;
        $this->db->query($sql);
        return $this->db->getResultSet(PDO::FETCH_CLASS);
    }

    /**
     * Gets and returns variable description for the variable (data table)
     * @param $variableID
     * @return array
     */
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


    /**
     * Returns any post data retrieval messages or meta data.
     * @return array
     */
    public function metaIncludeAfter() {
        return $this->groupBy;
    }

    /**
     * Determines and sets any SQL constraints (where clause) on the final SQL query string.
     * First checks whether or not the requested constraint is valid (determined by columnMap::columns() class).
     * Then adds the constraint to the SQL query.
     * @param $request
     * @return string
     */
    private function getSqlConstraints($request) {
        $sql = '';
        foreach ($request as $key => $value) {
            if ($this->in_arrayi($key, columnMap::columns())) {
                if (is_array($value)) {
                    $sql .= $this->getSqlFromManyArgs($request->tableName, $key, $value);
                    $sql .= ' AND ';
                } else {
                    if ($value != '') { // $key == columnMap::columns()[3] &&
                        $sql .= $key . '=' . $value . ' AND ';
                    }

                }
            }
        }
        return (strlen($sql) != 0 ? ' WHERE ' . substr($sql, 0, -5) : ''); // TODO hack! change to proper
    }

    /**
     * Generates one SQL constraint (WHERE clause) based on the table name, column name and value.
     * @param $tableName
     * @param $key
     * @param $values
     * @return string
     */
    private function getSqlFromManyArgs($tableName, $key, $values) {
        $sql = '(';
        if (!is_array($values)) return $sql;
        $last = end($values);
        foreach ($values as $value) {
            $sql .= "$tableName.$key=$value";
            if ($value != $last) {
                $sql .= ' OR ';
            }
        }
        return $sql . ')';
    }

    /**
     * Determines and sets the SQL group by on the final SQL string.
     * If no group by is set tries to determine based on some simple rules.
     * @param RequestModel $request
     * @return string
     */
    private function getGroupByClause($request) {
        $ret = ' GROUP BY ';
        $sqlGetTableColumns = 'SHOW COLUMNS FROM ' . $request->tableName;
        $this->db->query($sqlGetTableColumns);
        $dbResult = $this->db->getResultSet();
        if (isset($request->groupBy) && is_array($request->groupBy)) {
            foreach ($dbResult as $item) {
                if ($this->in_arrayi($item['Field'], $request->groupBy)) {
                    $ret .= ' ' . $item['Field'];
                    array_push($this->groupBy, $item['Field']);
                    if (strtolower($item['Field']) != strtolower(end($request->groupBy))) {
                        $ret .= ',';
                    }
                }
            }
        } else {
            $text = ' ORDER BY municipalityID';
            foreach ($dbResult as $item) {
                if ($item['Field'] == 'pYear') {
                    $text .= ', pYear';
                }
            }
            return $text;
            return ' ORDER BY municipalityID';
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
            foreach ($dbResult as $column) {
                if ($column['Field'] == columnMap::columns()[4]) { // pQuarter
                    $ret .= ', ' . columnMap::columns()[4];
                    array_push($this->groupBy, columnMap::columns()[4]);
                } elseif ($column['Field'] == columnMap::columns()[7]) { // pMonth
                    $ret .= ', ' . columnMap::columns()[7];
                    array_push($this->groupBy, columnMap::columns()[7]);
                }
            }
        }
        return $ret;
    }


    /** DEPRECIATED METHODS */ // TODO remove old functions
    private function MunicipalityName() { return ['Municipality.MunicipalityName', 'MunicipalityName']; }
    private function _SQLmunicipalityName() { return $this->MunicipalityName()[0] . ' AS ' . $this->MunicipalityName()[1]; }
    private function AgeRanges() { return ['concat_ws("-", AgeRange.AgeRangeStart, AgeRange.AgeRangeEnd)', 'Ages']; }
    private function _SQLageRanges() { return $this->AgeRanges()[0] . ' AS ' . $this->AgeRanges()[1]; }
    private function Gender() { return ['Gender.GenderTextSingular', 'Gender'];}
    private function _SQLgender() { return $this->Gender()[0]. ' AS ' . $this->Gender()[1]; }
    private function Nace2007() { return ['Nace2007.NaceText', 'Nace']; }
    private function _SQLnace() { return [$this->Nace2007()[0] . ' AS ' . $this->Nace2007()[1]]; }

    /**
     * Currently not in use. TODO remove me
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
        return null;
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
//            $sql  = <<<SQL
//SELECT VariableID as ID, VariableMasterCategory.MasterCategoryID as MasterCatID,
//VariableSubCategory.SubCategoryID as SubCatID, StatisticName,
//VariableSubCategory.Position as SubPosition,
//SubCategoryName, VariableMasterCategory.Position as MsPosition,
//MasterCategoryName, VariableSubCategory.IconType as SubCatIconType,
//VariableSubCategory.IconData as SubCatIconData
//FROM Variable, VariableSubCategory, VariableMasterCategory
//WHERE Variable.SubCategoryID = VariableSubCategory.SubCategoryID
//AND VariableSubCategory.MasterCategoryID = VariableMasterCategory.MasterCategoryID
//ORDER BY StatisticName;
//SQL;
            $sql2 = <<<SQL
SELECT variableID, VariableMasterCategory.masterCategoryID as masterCategoryID,
VariableMasterCategory.Position as masterPosition,
VariableSubCategory.subCategoryID as subCategoryID,
VariableSubCategory.position as subPosition,
masterCategoryName,
subCategoryName,
statisticName 
FROM Variable, VariableSubCategory, VariableMasterCategory
WHERE Variable.subCategoryID = VariableSubCategory.subCategoryID 
AND VariableSubCategory.masterCategoryID = VariableMasterCategory.masterCategoryID
ORDER BY StatisticName;
SQL;

            $this->db->query($sql2);
            return $this->db->getResultSet();
        } catch (PDOException $ex) {
            $this->db->DbhError($ex);
            return null;
        }
    }

    /**
     * @param $request
     */
    public function getVariableMainData($request) {
        // TODO method stub
        try {
            $ret = new stdClass();
            $sql = <<<SQL
SELECT a.variableID, a.subCategoryID, a.providerID, b.providerNameShortForm,
a.statisticName, a.tableName, a.updateInterval, a.lastUpdatedDate, 
a.providerCode, a.isImplemented 
FROM Variable a, VariableProvider b
WHERE a.providerID = b.providerID
SQL;
            $this->db->query($sql);
            $ret->variable = $this->db->getResultSet();
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
    function getVariableList() {
        try {
            // TODO Implement cache and return if recent
            $this->db->query('SELECT variableID, tableName, statisticName, isImplemented FROM Variable');
            return $this->db->getResultSet(PDO::FETCH_CLASS);
        } catch (PDOException $ex) {
            $this->db->DbhError($ex);
            return null;
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