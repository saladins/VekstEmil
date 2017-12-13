<?php
/**
 * @param RequestModel $request
 * @throws Exception
 */
class Validate {
    /** @var DatabaseHandler  */
    private $db;

    /**
     * validate constructor.
     * @param DatabaseHandler $db
     */
    public function __construct($db) {
        $this->db = $db;
    }

    /**
     * TODO rewrite to first check correct request type, then check for contents of said request
     * @param RequestModel $request
     * @return void
     * @throws Exception
     */
    public function checkRequestOrDie($request) {
        if (!isset($request) || $request == null) {
            throw new Exception('Invalid or missing request');
        }
        if (!isset($request->requestType) || $request->requestType == null) {
            throw new Exception('Invalid or missing request type');
        } else {
            // TODO named constants
            switch ($request->requestType) {
                case 10:
                case 20:
                    if (isset($request->variableID) && $request->variableID != null) {
                        $this->checkVariableOrDie($request);
                    } elseif (isset($request->tableNumber) && $request->tableNumber != null) {
                        $this->checkTableNumberOrDie($request);
                    } elseif (isset($request->tableName) && $request->tableName != null) {
                        $this->checkTableNameOrDie($request);
                    } else {
                        // todo figure out how to handle valid requests that are missing id/numbers like menu requests
                    }
                    break;
                case 30:
                case 40:
                case 50:
                    if (isset($request->variableID) && $request->variableID != null) {
                        $this->checkVariableOrDie($request);
                    }
                    break;
                case 70: // Menu

                    break;
                default:
                    throw new Exception('Invalid or missing request type');
            }
        }
    }


    /**
     * Checks whether or not it's a valid variable ID
     * @param RequestModel $request
     * @return void
     * @throws Exception
     */
    private function checkVariableOrDie($request) {
        if (!isset($request->variableID)) {
            throw new Exception('Missing variable ID in variable request');
        }
        if ($request->variableID < 0) {
            throw new Exception('Variable ID is invalid');
        }
        $sql = 'SELECT TableName FROM Variable WHERE VariableID =' . $request->variableID;
        $this->db->query($sql);
        $result = $this->db->getSingleResult();
        if (!$result['TableName']) {
            throw new Exception('Variable ID is invalid');
        } else {
            if ($request->tableName == null) {
                $request->tableName = $result['TableName'];
            }
            if ($request->tableNumber == null) {
                $request->tableNumber = $this->mapTableNameToTableNumber($result['TableName']);
            }
        }
    }

    /**
     * @param RequestModel $request
     * @return void
     */
    private function checkTableNumberOrDie($request) {
        $request->tableName = $this->mapTableNumberToTableName($request->tableNumber);
    }

    /**
     * @param RequestModel $request
     * @return void
     */
    private function checkTableNameOrDie($request) {
        $request->tableNumber = $this->mapTableNameToTableNumber($request->tableName);
    }


    /**
     * Checks the internal table number => table name mapping and return the array ID
     * @param string $tableName
     * @return int              Array ID for the table table name
     * @throws Exception        Throws exception if table is not found
     */
    private function mapTableNameToTableNumber($tableName) {
        foreach (TableMap::getTableMap() as $key => $value) {
            if (strtolower($value) == strtolower($tableName)) {
                return $key;
            }
        }
        throw new Exception('Table number and name mismatch. No table found with that number.');
    }

    /**
     * Checks the internal table mapping array for the corresponding table
     * @param int $tableNumber Table number
     * @return String Table name
     * @throws Exception
     */
    private function mapTableNumberToTableName($tableNumber) {
        if ($tableNumber < 0 || $tableNumber > count(TableMap::getTableMap())) {
            throw new Exception('Table name and number mismatch. No table found with that name.');
        }
        return TableMap::getTableMap()[$tableNumber];
    }
}
