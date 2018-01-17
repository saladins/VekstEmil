<?php
class DataTables {
    /** @var DatabaseHandler */
    private $db;

    /**
     * Description constructor.
     * @param DatabaseHandler $db
     */
    public function __construct($db) {
        $this->db = $db;
    }

    public function getDataTables() {
        $sql = "select distinct tableName, providerCode, lastUpdatedDate, updateInterval from Variable where providerCode regexp '^[0-9]+$' group by tableName;";
        $this->db->prepare($sql);
        return $this->db->getResultSet();
    }


}