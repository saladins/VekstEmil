<?php
header('Access-Control-Allow-Origin: *');
//header('Access-Control-Allow-Methods: OPTION, OPTIONS, GET, POST, PUT, DELETE', false); // OPTIONS, GET, POST, PUT, DELETE
//header('Access-Control-Allow-Headers: Origin, Accept, Content-Type, X-Requested-With', false); // , X-Requested-With, origin, authorization, accept
//header("Access-Control-Allow-Headers: Origin, X-Requested-With, Content-Type, Accept, Access-Control-Allow-Headers", false);
header('Content-Type: application/json; charset=utf-8', false);
require '../vendor/autoload.php';
include './server/index.php';
include './model/tablemap.php';
include './model/requestmap.php';
include './model/columnmap.php';
include './model/validarguments.php';
include './model/requestmodel.php';
include './helpers/variableupdatereasonmodel.php';
include 'update/index.php';
include 'request/index.php';

//session_start();
//$_SESSION = array();
//session_destroy();

$parser = new APIparser();

/** Main class and entry point for any and all operations on the db */
class APIparser {
    /** @var RequestModel */
    private $postContent;
    /** @var APIrequest */
    private $ApiRequest;
    /** @var Logger  */
    private $logger;
    /** @var string  */
    private $genericTableName = 'table';
    /** @var string  */
    private $metaTableName = 'meta';

    public function __construct() {
//        echo hash('sha256', 'ringerike3511'); die;
        $this->logger = new Logger();
        $this->ApiRequest = new APIrequest();
//        $auth = new Authenticate();
//        if ($auth->authenticate('default', '$2y$10$cLZqmlSn1DZEg7kYWMvwfuJfQXQ/A4lhFvLQw4tbQq40eiMXSvUi.')) {
            if (file_get_contents('php://input') != null) {
                $this->logger->log('Received POST request from ' . $_SERVER['REMOTE_ADDR']);
                $this->parsePost();
            } else {
                $this->postContent = $this->parseGet();
                if (Globals::isDebugging()) {
                    $this->logger->log('Received GET request');
                    $this->logger->log('GET request content was ' . serialize($_GET));
                }
            }
            if ($this->postContent == null || !is_object($this->postContent)) {
                $this->logger->log('Malformed request from ' . $_SERVER['REMOTE_ADDR']);
                $this->logger->log('Content was : ' . json_encode($this->postContent));
                echo json_encode('{malformed request}') or die;
                http_response_code(400);
                die;
            } else {
                if (Globals::isDebugging()) {
                    $this->logger->log('Handling request from '
                        . $_SERVER['REMOTE_ADDR']
                        . '. content: '
                        . ' (omitted) ');
                }
                $this->handleRequest();
            }
//        }
    }


    /**
     * Parses the GET request, and maps arguments to a holding class RequestModel
     * @return RequestModel Returns a model of the request
     */
    private function parseGet() {
        /** @var RequestModel postContent */
        $postContent = new RequestModel;
        if (!isset($_GET[ValidArguments::requestType])) {
            http_response_code(400);
            $this->output(array('error' => 'No request type specified')); die;
        }
        $postContent->requestType = $_GET[ValidArguments::requestType];
        $postContent->variableID = (isset($_GET[ValidArguments::variableID]) ? $_GET[ValidArguments::variableID] : null);
        if (isset($_GET[ValidArguments::constraints])) {
            $groups = explode(';', $_GET[ValidArguments::constraints]);
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
     * @return void
     */
    private function parsePost() {
        ini_set('memory_limit', '-1');
        $PostContent = file_get_contents('php://input');
        if (!$PostContent) {
            die;
        } else {
            $this->postContent = json_decode($PostContent);
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
            $this->ApiRequest->checkRequestOrDie($this->postContent);
            $response[$this->metaTableName] = $this->parseRequestMetaData($this->postContent);
            /** @var RequestMap $requestType */
            $requestType = $this->postContent->requestType;
            switch ($requestType) {
                case RequestMap::Detailed:
                    $response[$this->getRetArrID($this->postContent)] = $this->ApiRequest->getDetailedData($this->postContent);
//                        $response[$this->getRetArrID($this->postContent)] = $this->API->parseTable($this->postContent);
                    break;
                case RequestMap::Variable:
                    $response[$this->getRetArrID($this->postContent)] = $this->ApiRequest->getVariableData($this->postContent);
                    $response[$this->metaTableName]->groupBy = $this->metaIncludeAfter();
                    break;
                case RequestMap::Description:
                    $response[$this->getRetArrID($this->postContent)] = $this->ApiRequest->getDescription($this->postContent->variableID);
                    break;
                case RequestMap::Related:
                    $response[$this->getRetArrID($this->postContent)] = $this->ApiRequest->getRelated($this->postContent->variableID);
                    break;
                case RequestMap::Links:
                    $response[$this->getRetArrID($this->postContent)] = $this->ApiRequest->getLinks($this->postContent->variableID);
                    break;
                case RequestMap::Tags:
                    $response[$this->getRetArrID($this->postContent)] = $this->ApiRequest->getTags($this->postContent->variableID);
                    break;
                case RequestMap::Menu:
                    $response[$this->getRetArrID($this->postContent)] = $this->ApiRequest->getMenu();
                    break;
                case RequestMap::Generic:
                    throw new Exception('Not yet implemented');
                    break;
                case RequestMap::Update:
                    $ApiUpdate = new ApiUpdate();
                    $response[$this->getRetArrID($this->postContent)] = $ApiUpdate->update($this->postContent);
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
        if (Globals::isDebugging()) {
            $this->logger->log('Handling request ' . $this->postContent->requestType . ' took ' . ($endTime - $startTime) . ' seconds');
        }
        $this->output($response);
    }

    /**
     * Sets correct flag for the return data packet
     * @param RequestModel $request
     * @return mixed
     */
    private function getRetArrID($request) {
        if (isset($request->variableID) && $request->variableID != null) {
            return 'resultSet';
        } elseif ($request->requestType == RequestMap::Menu) {
            return 'Menu';
        } else {
            return $this->genericTableName;
        }
    }

    /**
     * Generates descriptive meta data for the request. Includes constraints, descriptions and information about the variable.
     * @param RequestModel $request
     * @return stdClass|string
     */
    private function parseRequestMetaData($request) {
        switch ($request->requestType) {
            case RequestMap::Variable:
            case RequestMap::Detailed:
                return $this->ApiRequest->getMinimalMetaData($request);
                break;
            default:
                return '{}';
                break;
        }
    }

    /**
     * Any included meta data that needs to be handled after main data has been fetched is handled here.
     * @return array
     */
    private function metaIncludeAfter() {
        return $this->ApiRequest->metaIncludeAfter();
    }

    /**
     * Parses and returns the requested data packet back to sender
     * @param mixed $content
     * @return void
     */
    function output($content) {
        $options = 0;
        if (Globals::isDebugging() && Globals::isDebuggingOutput()) {
            $options = JSON_UNESCAPED_UNICODE + JSON_UNESCAPED_SLASHES + JSON_PRETTY_PRINT;
        }
        if ($content == null || !is_array($content) ||  count($content) == 0) {
        } else {
            if (Globals::isDebugging()) {
                $this->logger->log('Sending data to ' . $_SERVER['REMOTE_ADDR']);
            }
            echo json_encode($content, $options);
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
        file_put_contents('log.log', $timestamp . $content . "\n", FILE_APPEND);
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