<?php
class Description {
    /** @var DatabaseHandler */
    private $db;

    /**
     * Description constructor.
     * @param DatabaseHandler $db
     */
    public function __construct($db) {
        $this->db = $db;
    }

    /**
     * Gets and returns variable description for the variable (data table)
     * @param integer $variableID
     * @return stdClass
     */
    public function getVariableDescription($variableID) {
        $sql = <<<SQL
SELECT a.descriptionID, a.variableID, a.descriptionText 
FROM VariableDescription a, Variable b
WHERE a.variableID = b.variableID
AND b.variableID = :variableID
SQL;
        $this->db->prepare($sql);
        $this->db->bind(':variableID', $variableID);
        return $this->db->getResultSet(PDO::FETCH_CLASS);
    }

}