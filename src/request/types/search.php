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
        $sql = "SELECT variableID, statisticName FROM Variable ORDER BY statisticName";
        $this->db->prepare($sql);
        $this->db->bind(':searchTerm', $searchTerm);
        $res1 = $this->db->getResultSet();
        return $res1;
    }
}