<?php
include 'DatabaseConnector.php';
class DatabaseHandler extends DatabaseConnector {
    /** @var  PDOStatement $stmt */
    private $stmt;

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
     * @return array
     */
    function getResultSet($fetchStyle = PDO::FETCH_ASSOC) {
        $this->execute();
        return $this->stmt->fetchAll($fetchStyle);
    }

    /**
     * Returns a single row. Defaults to associative array.
     * @param int $fetchStyle
     * @return mixed
     */
    function getSingleResult($fetchStyle = PDO::FETCH_ASSOC) {
        $this->execute();
        return $this->stmt->fetch($fetchStyle);
    }

    /**
     * Returns row count for query.
     * @return int
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

    function beginTransaction() {
        return $this->dbh->beginTransaction();
    }

    function endTransaction() {
        return $this->dbh->commit();
    }

    function rollbackTransaction() {
        return $this->dbh->rollBack();
    }

    function setAttribute($attribute, $value) {
        $this->dbh->setAttribute($attribute, $value);
    }

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

    public function DbhError($exception) {
        $this->errorMessage = $exception;
        parent::handleError();
    }
}

class DatabaseHandlerFactory {
    public static function getDatabaseHandler() {
        $db = new DatabaseHandler($GLOBALS['configFile']);
        $db->connect();
        return $db;
    }
}