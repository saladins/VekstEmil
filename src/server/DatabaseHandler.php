<?php
class DatabaseHandler extends DatabaseConnector {
    /** @var PDOStatement $stmt */
    private $stmt;

    /**
     * DatabaseHandler constructor.
     * @param string $configFilePath
     */
    public function __construct($configFilePath) {
        parent::__construct($configFilePath);
        $this->stmt = new PDOStatement;
    }

    /**
     * @param string $query
     * @return void
     */
    function query($query) {
        $this->stmt = $this->dbh->prepare($query);
    }

    /**
     * @return bool Returns true or false depending on query success
     */
    function execute() {
        return $this->stmt->execute();
    }

    /**
     * Returns resultSet. Defaults to associative array.
     * @param int $fetchStyle
     * @return mixed
     */
    function getResultSet($fetchStyle = PDO::FETCH_ASSOC) {
        $this->execute();
        return $this->stmt->fetchAll($fetchStyle);
    }

    /**
     * @param array $bindings
     * @return array
     */
    function getResultSetWithBinding($bindings) {
        $this->stmt->execute($bindings);
        return $this->stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Returns a single row. Defaults to associative array.
     * @param integer $fetchStyle
     * @return mixed
     */
    function getSingleResult($fetchStyle = PDO::FETCH_ASSOC) {
        $this->execute();
        return $this->stmt->fetch($fetchStyle);
    }

    /**
     * Returns row count for query.
     * @return integer
     */
    function getRowCount() {
        return $this->stmt->rowCount();
    }

    /**
     * Gets the ID for the last inserted row.
     * @return string
     */
    function getLastInsertID() {
        return $this->dbh->lastInsertId();
    }

    function isTransaction() {
        return $this->dbh->inTransaction();
    }

    /**
     * @return bool
     */
    function beginTransaction() {
        $this->dbh->setAttribute(PDO::ATTR_AUTOCOMMIT, 0);
//        if (!$this->dbh->inTransaction()) {
            return $this->dbh->beginTransaction();
//        }
    }

    /**
     * @return bool
     */
    function commit() {
        return $this->dbh->commit();
    }

    /**
     * @return bool
     */
    function rollbackTransaction() {
        return $this->dbh->rollBack();
    }

    /**
     * @param integer $attribute
     * @param boolean $value
     * @return void
     */
    function setAttribute($attribute, $value) {
        $this->dbh->setAttribute($attribute, $value);
    }

    /**
     * @param string $sql
     * @return void
     */
    function prepare($sql) {
        $this->stmt = $this->dbh->prepare($sql);
    }

    /**
     * @param string $param
     * @param mixed $value
     * @param integer $type
     * @return void
     */
    function bind($param, $value, $type = null) {
        if (is_null($type)) {
            switch (true) {
                case is_int($value):
                    $type = PDO::PARAM_INT;
                    break;
                case is_bool($value):
                    $type = PDO::PARAM_BOOL;
                    break;
                case is_null($value):
                    $type = PDO::PARAM_NULL;
                    break;
                default:
                    $type = PDO::PARAM_STR;
                    break;
            }
        }
        $this->stmt->bindValue($param, $value, $type);
    }

    /**
     * Quotes string
     * @param string $string
     * @return string
     */
    public function quote($string) {
        return $this->dbh->quote($string);
    }

    function disconnect() {
        parent::disconnect();
        $this->stmt = null;
    }

}

class DatabaseHandlerFactory {
    /**
     * @return DatabaseHandler
     */
    public static function getDatabaseHandler() {
        $db = new DatabaseHandler(Globals::configFilePath);
        return $db;
    }
}