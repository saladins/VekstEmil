<?php
class Tags {
    /** @var DatabaseHandler */
    private $db;

    /**
     * Tags constructor.
     * @param DatabaseHandler $db
     */
    public function __construct($db) {
        $this->db = $db;
    }

    /**
     * Gets and returns list of tags for the variable (data table)
     * @param integer $variableID
     * @return array
     */
    public function getTagsForVariable($variableID) {
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

}