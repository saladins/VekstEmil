<?php
class Related {
    /** @var DatabaseHandler */
    private $db;

    /**
     * Related constructor.
     * @param DatabaseHandler $db
     */
    public function __construct($db) {
        $this->db = $db;
    }

    /**
     * Gets list of internal variable IDs for related variables (data tables)
     * @param integer $variableID
     * @return stdClass
     */
    public function getRelated($variableID) {
        $sql = <<<SQL
SELECT a.relatedVariableID, b.statisticName, c.subCategoryName, c.subCategoryID
FROM VariableRelated a, Variable b, VariableSubCategory c
WHERE a.relatedVariableID = b.variableID
AND b.subCategoryID = c.subCategoryID
AND a.parentVariableID = :variableID;
SQL;
        $this->db->prepare($sql);
        $this->db->bind(':variableID', $variableID);
        return $this->db->getResultSet(PDO::FETCH_CLASS);
    }
}