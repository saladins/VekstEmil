<?php
class DatabaseConnector {
    private $isConnected;
    private $configFile;
    /** @var  PDO $dbh */
    protected $dbh;
    /** @var  Exception $errorMessage */
    protected $errorMessage;

    /**
     * DatabaseConnector constructor.
     * @param $configFile
     */
    public function __construct($configFile) {
        $this->configFile = $configFile;
    }

    /**
     * Connects to the local database
     * @return PDO
     * @throws Exception
     */
    function connect() {
        if (!$this->isConnected || $this->dbh == null) {
            if (!$settings = parse_ini_file($this->configFile, TRUE)) {
                $this->errorMessage = 'Unable to open configuration file. The file does not exist or has been moved.';
                // TODO Logging and/or e-mail alert
            }
            $dns = $settings['db']['db_driver'] .
                ':host=' . $settings['db']['db_host'] .
                ((!empty($settings['db']['db_port'])) ? (';port=' . $settings['db']['db_port']) : '') .
                ';dbname=' . $settings['db']['db_schema'] . ';charset=utf8mb4';
            $options = array(
                PDO::ATTR_PERSISTENT => false,
                PDO::ATTR_EMULATE_PREPARES => true,
                PDO::ATTR_STRINGIFY_FETCHES => false,
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
            );
            $GLOBALS['schema_name'] = $settings['db']['db_schema'];
            try {
                $this->dbh = new PDO($dns, $settings['db']['db_username'], $settings['db']['db_password'], $options);
                $this->isConnected = true;
            } catch (PDOException $e) {
                $this->isConnected = false;
                $this->handleError();
            } catch (Exception $ex) {
                $this->isConnected = false;
                $this->errorMessage = $ex->getMessage();
                $this->handleError();
            }
        }
        if ($this->isConnected) {
            return $this->dbh;
        } else {
            $this->handleError();
            return null;
        }

    }

    /**
     * Handles error processing and messages
     * @return string JSON-encoded error message
     */
    function handleError() {
        if (!$GLOBALS['debug']) {
//            header('Content-Type: application/json', false);
            http_response_code(404);
            switch ($this->errorMessage->getCode()) {
                case '42S02':   // Table not found
                    $this->errorMessage = 'Misconfigured PDO statement.';
                    break;
                case 1045:      // Username or pw error
                    $this->errorMessage = 'User access error. ';
                    break;
                default:
                    $this->errorMessage = $this->errorMessage->getMessage();
                    break;
            }

            echo json_encode($this->errorMessage);
            exit(1);
        } else {
            echo '<b>An error occurred: </b><br>';
            echo $this->errorMessage . '<br>';
            echo '<b>It\'s message: </b><br>' . $this->errorMessage->getMessage();
            exit(1);
        }
    }
}