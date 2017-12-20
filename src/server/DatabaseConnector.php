<?php
class DatabaseConnector {
    /** @var boolean */
    private $isConnected;
    /** @var PDO $dbh */
    protected $dbh;

    /**
     * DatabaseConnector constructor.
     * @param string $configFilePath
     */
    public function __construct($configFilePath) {
        $this->isConnected = false;
        $this->dbh = $this->connect($configFilePath);
    }

    /**
     * Connects to the local database
     * @param string $configFilePath
     * @return PDO
     */
    private function connect($configFilePath) {
        if (!$this->isConnected || $this->dbh == null) {
            $raw = file_get_contents($configFilePath);
            $settings = parse_ini_string($raw, TRUE);
            if (!$settings) {
                $this->handleError(new Exception('Unable to open configuration file. Contact the system administrator'));
            } else {
                $dns = $settings['db']['db_driver'] .
                    ':host=' . $settings['db']['db_host'] .
                    ((!empty($settings['db']['db_port'])) ? (';port=' . $settings['db']['db_port']) : '') .
                    ';dbname=' . $settings['db']['db_schema'] . ';charset=utf8mb4';
                $options = array(
                    PDO::ATTR_PERSISTENT => false,
                    PDO::ATTR_EMULATE_PREPARES => false,
                    PDO::ATTR_STRINGIFY_FETCHES => false,
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
                );
                Globals::setSchemaName($settings['db']['db_schema']);
                try {
                    $this->dbh = new PDO($dns, $settings['db']['db_username'], $settings['db']['db_password'], $options);
                    $this->isConnected = true;
                } catch (PDOException $e) {
                    $this->isConnected = false;
                    $this->handleError($e);
                } catch (Exception $ex) {
                    $this->isConnected = false;
                    $this->handleError($ex);
                }
            }
        }
        if (!$this->isConnected) {
            $this->handleError(new Exception('Server went away'));
        }
        return $this->dbh;
    }

    /**
     * Handles error processing and messages
     * @param Exception $exception
     * @return void
     */
    function handleError($exception) {
        $logger = new Logger();
        if (!Globals::debugging) {
            http_response_code(404);
            switch ($exception->getCode()) {
                case '42S02':   // Table not found
                    $message = 'Misconfigured PDO statement.';
                    break;
                case 1045:      // Username or pw error
                    $message = 'User access error. ';
                    break;
                default:
                    $message = $exception->getMessage();
                    break;
            }
            $logger->log($message);
            echo json_encode($message) or die;
            die;
        } else {
            $logger->log(print_r($exception, true));
            echo json_encode($exception) or die;
            die;
        }
    }
}