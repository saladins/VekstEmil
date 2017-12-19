<?php
/** Class for API (GET) requests */
class APIrequest {
    /** @var DatabaseHandler */
    private $db;
    /** @var Logger */
    private $logger;
//    private $mysqltime;
    /** @var array */
    private $binds = [];
    /** @var array */
    private $groupBy = [];
    public function __construct() {
//        $this->mysqltime = date('Y-m-d H:i:s');
        $this->logger = new Logger();
        $this->db = DatabaseHandlerFactory::getDatabaseHandler();
    }

    /**
     * Validates request or throws exception
     * @param RequestModel $request
     * @throws Exception
     * @return void
     */
    public function checkRequestOrDie($request) {
        $validate = new Validate($this->db);
        $validate->checkRequestOrDie($request);
    }

    /**
     * @param RequestModel $request
     * @return mixed
     */
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
SELECT municipalityID, naceID, pYear, livingPlaceValue AS value 
FROM EmploymentDetailed WHERE pYear =(
	SELECT MAX(pYear) FROM EmploymentDetailed)
GROUP BY municipalityID, naceID, pYear;
SQL;
                break;
            case 43:
                $sql = <<<SQL
SELECT municipalityID, pYear, kostraCategoryID, SUM(expense) FROM RegionalCooperation
WHERE municipalExpenseCategoryID = 2
GROUP BY municipalityID, pYear;
SQL;
                break;
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
     * TODO refactor to use variableID and get tableName later
     * @param RequestModel $request
     * @return array String array containing variable data
     */
    public function getVariableData($request) {
        $this->logger->log(serialize($request));
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
        if (sizeof($this->binds) > 0) {
            $result = $this->db->getResultSetWithBinding($this->binds);
        } else {
            $result = $this->db->getResultSet();
        }
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
     * Gets constraints, descriptions and variable meta data for the variable (data table)
     * @param RequestModel $request
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
     * @param RequestModel $request
     * @return array
     */
    private function getFieldDescriptionsAndConstraints($request) {
        $ret = array();
        $constraints = new stdClass();
        $descriptions = new stdClass();
        foreach (columnMap::columns() as $column) {
            $sqlCheckIfColumn = 'SHOW COLUMNS FROM ' . $request->tableName . " LIKE '$column' ";
            $this->db->query($sqlCheckIfColumn);
            if ($this->db->getResultSet()) {
                $sqlGetConstraints = 'SELECT DISTINCT ' . $column . ' FROM ' . $request->tableName;
                $sqlGetConstraints .= $this->getSqlConstraints($request);
                $this->db->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
                $this->db->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, false);
                $this->db->prepare($sqlGetConstraints);
                if (count($this->binds) > 0) {
                    $constraints->{$column} = $this->db->getResultSetWithBinding($this->binds);
                } else {
                    $constraints->{$column} = $this->db->getResultSet(PDO::FETCH_NUM);
                }
                $descriptions->{$column} = $this->getFieldDescriptions($column);
            }
        }
        $ret[0] = $constraints;
        $ret[1] = $descriptions;
        return $ret;
    }

    /**
     * Determines if there are returnable descriptions in child table. If there are, returns entire contents of said table.
     * @param string $columnName
     * @return stdClass|null
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
     * @param integer $variableID
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
     * @param integer $variableID
     * @return array
     */
    private function getTagsForVariable($variableID) {
        $sql = <<<SQL
SELECT a.variableID, a.tagID, b.tagText 
FROM VariableTagList a, VariableTag b
WHERE a.tagID = b.tagID
AND   a.variableID = :variableID
SQL;
        $this->db->prepare($sql);
        $this->db->bind(':variableID', $variableID);
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
     * @param RequestModel $request
     * @return string
     * @throws Exception
     */
    private function getSqlConstraints($request) {
        if ($request->constraints) {
            $this->binds = [];
            $a = [];
            foreach ($request->constraints as $constraintName => $valueArray) {
                if ($this->in_arrayi($constraintName, columnMap::columns())) {
                    $in = str_repeat('?,', count($valueArray) - 1) . '?';
                    array_push($a, $constraintName . " IN ($in)");
                    $this->bindLater($valueArray);
                } else {
//                    throw new Exception('Invalid column constraint check');
                }
            }
            return ' WHERE ' . implode(' AND ', $a);
        } else {
            return '';
        }
    }


    /**
     * 'Hack'. Is called after sql string has been completed
     * @param mixed $value
     * @return void
     */
    private function bindLater($value) {
        $this->binds = array_merge($this->binds, $value);
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
            return $ret;
        } else {
            $text = ' ORDER BY municipalityID';
            foreach ($dbResult as $item) {
                if ($item['Field'] == 'pYear') {
                    $text .= ', pYear';
                }
            }
            return $text;
        }
    }

    /**
     * @return array
     */
    public function getMenu() {
        $menu = new Menu($this->db);
        return $menu->getMenu();
    }

    /**
     * @param integer $variableID
     * @return stdClass
     */
    public function getDescription($variableID) {
        $description = new Description($this->db);
        return $description->getVariableDescription($variableID);
    }

    /**
     * @param integer $variableID
     * @return stdClass
     */
    public function getRelated($variableID) {
        $related = new Related($this->db);
        return $related->getRelated($variableID);
    }

    /**
     * @param integer $variableID
     * @return stdClass
     */
    public function getLinks($variableID) {
        $links = new Links($this->db);
        return $links->getLinks($variableID);
    }

    /**
     * TODO Implement cache and return if recent
     * @return string[]
     * @throws PDOException
     */
    function getVariableList() {
        $this->db->query('SELECT variableID, tableName, statisticName, isImplemented FROM Variable');
        return $this->db->getResultSet(PDO::FETCH_CLASS);
    }

    /**
     * @param string $needle
     * @param string[] $haystack
     * @return bool
     */
    function in_arrayi($needle, $haystack) {
        return in_array(strtolower($needle), array_map('strtolower', $haystack));
    }
}