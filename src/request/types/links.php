<?php
class Links {
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
     * Gets links and document link descriptions for the variable (data table)
     * @param integer $variableID
     * @return stdClass
     */
    public function getLinks($variableID) {
        $sql = <<<SQL
SELECT a.linkedDocumentID, a.linkedDocumentAddress, a.linkedDocumentTitle, a.linkedDocumentDescription
FROM VariableLinkedDocument a
WHERE a.variableID = :variableID
SQL;
        $this->db->prepare($sql);
        $this->db->bind(':variableID', $variableID);
        return $this->db->getResultSet(PDO::FETCH_CLASS);
    }

}