<?php
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

header('Access-Control-Allow-Origin: *');
header('Content-Type: application/json; charset=utf-8', false);
header('Access-Control-Allow-Headers: Authorization, Content-Type, Content-Range, Content-Disposition, Content-Description', false);
require '../vendor/autoload.php';
include 'model/index.php';
include 'update/index.php';
include 'request/index.php';
include 'server/index.php';

$headers = getallheaders();
if (!isset($headers['Access-Control-Request-Method'])) {
    $parser = new ApiParser();
}

/** Main class and entry point for any and all operations on the db */
class ApiParser {
    /** @var RequestModel */
    private $requestModel;
    /** @var ApiRequest */
    private $apiRequest;
    /** @var Logger  */
    private $logger;

    public function __construct() {
        $this->logger = new Logger();
        if (!GLOBALS::debugging) {$this->logger->clearLog(); }
        $this->apiRequest = new ApiRequest();
        $this->requestModel = new RequestModel();
        if (file_get_contents('php://input') || isset($_GET)) {
            if (Globals::debugging) {
                $this->logger->log('Received request from ' . $_SERVER['REMOTE_ADDR']);
            }
            $this->handleRequest();
        } else {
            $this->logger->log('Malformed request from ' . $_SERVER['REMOTE_ADDR']);
            $this->logger->log('Content was : ' . json_encode($this->requestModel));
            $this->output(array('error' => 'Malformed request'));
            http_response_code(400);
            die;
        }
    }

    /**
     * Common method that handles request checking and determines the type of request.
     * If the request passes all tests the proper method is invoked
     * @return void
     * @throws Exception
     */
    public function handleRequest() {
        $startTime = $this->logger->microTimeFloat();
        $response = [];

        try {
            $this->requestModel = $this->parseRequest();

            $this->apiRequest->checkRequestOrDie($this->requestModel);
            /** @var RequestType $requestType */
            $requestType = $this->requestModel->requestType;

            switch ($requestType) {
                case RequestType::Detailed:
//                    $this->validateAccessOrDie();
                    $response[Globals::meta] = $this->apiRequest->getMetaData($this->requestModel);
                    $response[Globals::resultSet] = $this->apiRequest->getDetailedData($this->requestModel);
                    break;
                case RequestType::Variable:
//                    $this->validateAccessOrDie();
                    $response[Globals::meta] = $this->apiRequest->getMetaData($this->requestModel);
                    $response[Globals::resultSet] = $this->apiRequest->getVariableData($this->requestModel);
                    break;
                case RequestType::Description:
                    $response[Globals::resultSet] = $this->apiRequest->getDescription($this->requestModel->variableID);
                    break;
                case RequestType::Related:
                    $response[Globals::resultSet] = $this->apiRequest->getRelated($this->requestModel->variableID);
                    break;
                case RequestType::Links:
                    $response[Globals::resultSet] = $this->apiRequest->getLinks($this->requestModel->variableID);
                    break;
                case RequestType::Tags:
                    $response[Globals::resultSet] = $this->apiRequest->getTags($this->requestModel->variableID);
                    break;
                case RequestType::Menu:
                    $response[Globals::resultSet] = $this->apiRequest->getMenu();
                    break;
                case RequestType::Search:
                    $response[GLOBALS::resultSet] = $this->apiRequest->getSearchResult($this->requestModel->searchTerm);
                    break;
                case RequestType::Update:
                    $this->validateAccessOrDie();
                    $apiUpdate = new ApiUpdate();
                    $response[Globals::resultSet] = $apiUpdate->update($this->requestModel);
                    break;
                case RequestType::VariableSettings:
                    //$this->validateAccessOrDie();
                    $response[GLOBALS::resultSet]  =  $this->apiRequest->getVariableSettings($this->requestModel->variableID);
                    break;
                case RequestType::VariableUpdate:
                    //$this->validateAccessOrDie();
                    $response[GLOBALS::resultSet]  =  $this->apiRequest->updateVariableSettings($this->requestModel);
                    break;
                case RequestType::LinkInsert:
                    //$this->validateAccessOrDie();
                    $response[GLOBALS::resultSet]  =  $this->apiRequest->insertRelatedDocument($this->requestModel);
                    break;
                case RequestType::LinkDelete:
                    //$this->validateAccessOrDie();
                    $response[GLOBALS::resultSet]  =  $this->apiRequest->deleteRelatedDocument($this->requestModel);
                    break;

                case RequestType::DataTables:
                    $response[GLOBALS::resultSet] = $this->apiRequest->getDataTables();
                    break;
                default:
                    http_response_code(404);
                    exit(0);
                    break;
            }
        } catch (Exception $ex) {
            $this->logger->log('Fatal error! ');
            $this->logger->log($ex->getMessage());
            $errorPackage = [];
            $errorPackage['error'] = $ex->getMessage();
            $errorPackage['meta'] = $ex->getCode();
            $this->output($errorPackage);
            http_response_code(400);
            die;
        }
        $endTime = $this->logger->microTimeFloat();
        if (Globals::debugging) {
            $this->logger->log('Handling request ' . $this->requestModel->requestType . ' took ' . ($endTime - $startTime) . ' seconds');
        }
        $this->output($response);
    }

    /**
     * @return void
     * @throws Exception
     */
    private function validateAccessOrDie() {
        $auth = new Authenticate(Globals::configFilePath);
        if (!$auth->validate()) die;
    }

    /**
     * @return RequestModel
     */
    private function parseRequest() {
        if (file_get_contents('php://input')) {
            return $this->parsePost();
        } else {
            return $this->parseGet();
        }
    }

    /**
     * Parses the GET request, and maps arguments to a holding class RequestModel
     * @return RequestModel Returns a model of the request
     * @throws Exception
     */
    private function parseGet() {
        /** @var RequestModel postContent */
        $postContent = new RequestModel;
        if (!isset($_GET[ValidParam::requestType])) {
            throw new Exception('No request type specified');
        }
        $postContent->requestType = $_GET[ValidParam::requestType];
        $postContent->variableID = (isset($_GET[ValidParam::variableID]) ? $_GET[ValidParam::variableID] : null);
        $postContent->searchTerm = (isset($_GET[ValidParam::searchTerm]) ? $_GET[ValidParam::searchTerm] : null);
        if (isset($_GET[ValidParam::constraints])) {
            $groups = explode(';', $_GET[ValidParam::constraints]);
            $postContent->constraints = [];
            foreach ($groups as $group) {
                if (strlen($group) > 1) {
                    $item = explode('=', $group);
                    $args = explode(',', $item[1]);
                    $postContent->constraints[$item[0]] = $args;
                }
            }
        }
        return $postContent;
    }

    /**
     * POST message mapping to class.
     * Memory limit increased due to json_decode overhead makes large sets of data produce a stack overflow
     * @return RequestModel
     * @throws Exception
     */
    private function parsePost() {
        ini_set('memory_limit', '-1');
        $postContent = file_get_contents('php://input');

        if (!$postContent) {
            throw new Exception('Unable to parse POST data. Contact the administrator');
        } else {
            return json_decode($postContent);
        }
    }

    /**
     * Parses and returns the requested data packet back to sender
     * @param mixed $content
     * @return void
     */
    function output($content) {
        $options = 0;
        if (Globals::debugging && Globals::debuggingOutput) {
            $options = JSON_UNESCAPED_UNICODE + JSON_UNESCAPED_SLASHES + JSON_PRETTY_PRINT;
        }
        if ($content == null || !is_array($content) ||  count($content) == 0) {
        } else {
            if (Globals::debugging) {
                $this->logger->log('Sending data to ' . $_SERVER['REMOTE_ADDR']);
            }
            print_r(json_encode($content, $options), false);
//            echo json_encode($content, $options);
        }
    }
}

/** Logging class */
class Logger {
    /**
     * @param string $content
     * @param bool $clearlog
     * @return void
     */
    public function log($content, $clearlog = false) {
        if ($clearlog) $this->clearLog();
        $timestamp = date('Y-m-d H:i:s : ');
        file_put_contents('log/log.log', $timestamp . $content . "\n", FILE_APPEND);
    }

    /**
     * @return void
     */
    public function clearLog() {
        file_put_contents('log.log', '');
    }

    /**
     * @return float
     */
    function microTimeFloat() {
        list($usec, $sec) = explode(' ', microtime());
        return ((float)$usec + (float)$sec);
    }
}