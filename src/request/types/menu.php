<?php
class Menu {
    /** @var DatabaseHandler */
    private $db;

    /**
     * Menu constructor.
     * @param DatabaseHandler $db
     */
    public function __construct($db) {
        $this->db = $db;
    }

    /**
     * @return array
     * @throws PDOException
     */
    public function getMenu() {
        $sql = <<<SQL
SELECT variableID, VariableMasterCategory.masterCategoryID as masterCategoryID,
VariableMasterCategory.Position as masterPosition,
VariableSubCategory.subCategoryID as subCategoryID,
VariableSubCategory.position as subPosition,
Variable.position as listPosition,
masterCategoryName,
masterCategoryText,
subCategoryName,
statisticName 
FROM Variable, VariableSubCategory, VariableMasterCategory
WHERE Variable.subCategoryID = VariableSubCategory.subCategoryID 
AND VariableSubCategory.masterCategoryID = VariableMasterCategory.masterCategoryID
ORDER BY VariableMasterCategory.position, VariableSubCategory.position, listPosition;
SQL;
        $this->db->query($sql);
        return $this->db->getResultSet();
    }
}