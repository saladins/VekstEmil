<?php

class Search {
    /** @var DatabaseHandler */
    private $db;
    /** @var  Logger */
    private $logger;

    /**
     * Search constructor.
     * @param DatabaseHandler $db
     * @param Logger $logger;
     */
    public function __construct($db, $logger) {
        $this->db = $db;
        $this->logger = $logger;
    }

    public function searchTerm($searchTerm) {
        $this->logger->log($searchTerm);
        $sql = "SELECT variableID, statisticName FROM Variable WHERE statisticName LIKE concat('%', :searchTerm, '%') ORDER BY statisticName";
        $this->db->prepare($sql);
        $this->db->bind(':searchTerm', $searchTerm);
        $res1 = $this->db->getResultSet();
        return $res1;
//        $sql = <<<SQL
//SELECT Variable.variableID, statisticName FROM Variable, VariableTagList, VariableTag
//WHERE Variable.variableID = VariableTagList.variableID
//AND VariableTagList.tagID = VariableTag.tagID
//AND VariableTag.tagText LIKE CONCAT('%', :searchTerm, '%');
//SQL;
//        $this->db->prepare($sql);
//        $this->db->bind(':searchTerm', $searchTerm);
//        $res2 = $this->db->getResultSet();
//        return array_merge($res1, $res2);
    }
}