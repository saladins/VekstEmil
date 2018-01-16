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
        if (!isset($request) || $request === null) {
            throw new Exception('Invalid or missing request');
        }
        if (!isset($request->requestType) || $request->requestType === RequestType::Unknown) {
            throw new Exception('Invalid or missing request type');
        } else {
            switch ($request->requestType) {
                case 10:
                case 20:
                    if ($request->variableID > 0) {
                        $this->checkVariableOrDie($request);
                    }
                    if ($request->tableName !== '') {
                        $this->checkTableNameOrDie($request);
                    }
                    if ($request->variableID < 0 && $request->tableName === '') {
                        throw new Exception('Invalid or missing ID or table name');
                    }
                    break;
                case 30:
                case 40:
                case 50:
                    if (isset($request->variableID) && $request->variableID !== null) {
                        $this->checkVariableOrDie($request);
                    }
                    break;
                case 70: // Menu
                    break;
                case 80: // Search
                    break;
                case 100: // Update
                    break;
                case 110: // DataTables
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
        $sql = 'SELECT TableName FROM Variable WHERE variableID = :variableID';
        $this->db->prepare($sql);
        $this->db->bind(':variableID', $request->variableID);
        $result = $this->db->getSingleResult();
        if (!$result['TableName']) {
            throw new Exception('Variable ID is invalid');
        } else {
            if ($request->tableName === '') {
                $request->tableName = $result['TableName'];
            }
        }
    }

    /**
     * @param RequestModel $request
     * @return void
     * @throws Exception
     */
    private function checkTableNameOrDie($request) {
        if ($request->tableName !== null && strlen($request->tableName) < 1) {
            throw new Exception('Table name is invalid');
        }
        $sql = 'SELECT variableID FROM Variable WHERE tableName = :tableName';
        $this->db->prepare($sql);
        $this->db->bind(':tableName', $request->tableName);
        $result = $this->db->getSingleResult();
        if (!$result['variableID']) {
            throw new Exception('Table name is invalid');
        } else {
            if ($request->variableID === null) {
                $request->variableID = $result['variableID'];
            }
        }
    }
}
