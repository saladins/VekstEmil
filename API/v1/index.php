<?php
header('Access-Control-Allow-Origin: *');
//header('Access-Control-Allow-Methods: OPTION, OPTIONS, GET, POST, PUT, DELETE', false); // OPTIONS, GET, POST, PUT, DELETE
//header('Access-Control-Allow-Headers: Origin, Accept, Content-Type, X-Requested-With', false); // , X-Requested-With, origin, authorization, accept
//header("Access-Control-Allow-Headers: Origin, X-Requested-With, Content-Type, Accept, Access-Control-Allow-Headers", false);
header('Content-Type: application/json; charset=utf-8', false);
include 'globals.php';
include './model/tablemap.php';
include './model/requestmap.php';
include './model/columnmap.php';
include './model/ValidArgs.php';
include './model/requestmodel.php';
include './helpers/variableupdatereasonmodel.php';
include './request/DatabaseHandler.php';
include './request/apirequest.php';
include './request/apiupdate.php';
include './request/validate.php';

//session_start();
//$_SESSION = array();
//session_destroy();

$parser = new APIparser();
$parser->handleRequest();

/** Main class and entry point for any and all operations on the db */
class APIparser {
    /** @var null|RequestModel|RequestModel[] */
    private $postContent;
    /** @var APIrequest */
    private $ApiRequest;
    private $logger;
    private $genericTableName = 'table';
    private $metaTableName = 'meta';

    public function __construct() {
        $this->logger = new Logger();
//        if ($GLOBALS['debug']) $this->logger->clearLog();
        if (file_get_contents('php://input') != null) {
            $this->logger->log('Received POST request');
            $this->parsePost();
        } else {
            $this->logger->log('Received GET request');
            $this->postContent = $this->parseGet();
            $this->logger->log('GET request content was ' .
                serialize($_GET));
        }
        if ($this->postContent == null || !is_object($this->postContent)) {
            $this->logger->log('Malformed request from ' . $_SERVER['REMOTE_ADDR']);
            $this->logger->log('Content was : ' . json_encode($this->postContent));
            echo json_encode('{malformed request}');
            http_response_code(400);
            die;
        } else {
            if ($GLOBALS['debug']) {
                $this->logger->log('Handling request from '
                    . $_SERVER['REMOTE_ADDR']
                    . '. content: '
                    . ' (omitted) ');
//                    . print_r($this->postContent, true));
            }
            if ($this->ApiRequest == null) {
                $this->ApiRequest = new APIrequest();
            }

        }
    }


    /**
     * Parses the GET request, and maps arguments to a holding class RequestModel
     * @return null|RequestModel Returns a model of the request
     */
    private function parseGet() {
        /** @var RequestModel postContent */
        $postContent = new RequestModel();
        if (isset($_GET)) {
            if (isset($_GET[ValidArgs::a()->requestType])) {
                $postContent->requestType = $_GET[ValidArgs::a()->requestType];
                $postContent->variableID = (isset($_GET[ValidArgs::a()->variableID]) ? $_GET[ValidArgs::a()->variableID] : null);
                if (isset($_GET[ValidArgs::a()->municipalityID])) {
                    if (strpos($_GET[ValidArgs::a()->municipalityID], ',') != null) {
                        $postContent->municipalityID = explode(',', $_GET[ValidArgs::a()->municipalityID]);
                    } else {
                        $postContent->municipalityID = [$_GET[ValidArgs::a()->municipalityID]];
                    }
                }
                if (isset($_GET[ValidArgs::a()->naceID])) {
                    if (strpos($_GET[ValidArgs::a()->naceID], ',') != null) {
                        $postContent->naceID = explode(',', $_GET[ValidArgs::a()->naceID]);
                    } else {
                        $postContent->naceID = [$_GET[ValidArgs::a()->naceID]];
                    }
                }
                if (isset($_GET[ValidArgs::a()->genderID])) {
                    if (strpos($_GET[ValidArgs::a()->genderID], ',') != null) {
                        $postContent->genderID = explode(',', $_GET[ValidArgs::a()->genderID]);
                    } else {
                        $postContent->genderID = [$_GET[ValidArgs::a()->genderID]];
                    }
                }
                if (isset($_GET[ValidArgs::a()->gradeID])) {
                    if (strpos($_GET[ValidArgs::a()->gradeID], ',') != null) {
                        $postContent->gradeID = explode(',', $_GET[ValidArgs::a()->gradeID]);
                    } else {
                        $postContent->gradeID = [$_GET[ValidArgs::a()->gradeID]];
                    }
                }
                if (isset($_GET[ValidArgs::a()->ageRangeID])) {
                    if (strpos($_GET[ValidArgs::a()->ageRangeID], ',') != null) {
                        $postContent->ageRangeID = explode(',', $_GET[ValidArgs::a()->ageRangeID]);
                    } else {
                        $postContent->ageRangeID = [$_GET[ValidArgs::a()->ageRangeID]];
                    }
                }
                if (isset($_GET[ValidArgs::a()->tableNumber])) {
                    if (strpos($_GET[ValidArgs::a()->tableNumber], ',') != null) {
                        $postContent->tableNumber = explode(',', $_GET[ValidArgs::a()->tableNumber]);
                    } else {
                        $postContent->tableNumber = $_GET[ValidArgs::a()->tableNumber];
                    }
                }
                if (isset($_GET[ValidArgs::a()->tableName])) {
                    if (strpos($_GET[ValidArgs::a()->tableName], ',') != null) {
                        $postContent->tableName = explode(',', $_GET[ValidArgs::a()->tableName]);
                    } else {
                        $postContent->tableName = $_GET[ValidArgs::a()->tableName];
                    }
                }
                if (isset($_GET[ValidArgs::a()->groupBy])) {
                    if (strpos($_GET[ValidArgs::a()->groupBy], ',') != null) {
                        $postContent->groupBy = explode(',', $_GET[ValidArgs::a()->groupBy]);
                    } else {
                        $postContent->groupBy = $_GET[ValidArgs::a()->groupBy];
                    }
                }
                if (isset($_GET[ValidArgs::a()->years])) {
                    if (strpos($_GET[ValidArgs::a()->years], ',') != null) {
                        $postContent->pYear = explode(',', $_GET[ValidArgs::a()->years]);
                    } else {
                        $postContent->pYear = $_GET[ValidArgs::a()->years];
                    }
                }
                if (isset($_GET[ValidArgs::a()->buildingStatusID])) {
                    if (strpos($_GET[ValidArgs::a()->buildingStatusID], ',') != null) {
                        $postContent->buildingStatusID = explode(',', $_GET[ValidArgs::a()->buildingStatusID]);
                    } else {
                        $postContent->buildingStatusID = $_GET[ValidArgs::a()->buildingStatusID];
                    }
                }
                // TODO the rest
            } else {
                // TODO return capability
            }
        } else {
            return null;
        }
        return $postContent;
    }

    /**
     * POST message mapping to class.
     * Memory limit increased due to json_decode overhead makes large sets of data produce a stack overflow
     */
    private function parsePost() {
        ini_set('memory_limit', '-1');
        $this->postContent = json_decode($this->getRawPostData(), false);
    }

    /**
     * Reads the raw POST data from the internal variable
     * @return bool|string
     */
    private function getRawPostData() {
        return file_get_contents('php://input');
    }

    /**
     * Common method that handles request checking and determines the type of request.
     * If the request passes all tests the proper method is invoked
     */
    public function handleRequest() {
        $startTime = $this->logger->microTimeFloat();
        $response = [];
        try {
                $this->ApiRequest->checkRequestOrDie($this->postContent);
                $response[$this->metaTableName] = $this->parseRequestMetaData($this->postContent);
                switch ($this->postContent->requestType) {
                    case RequestMap::a()->TableSurvey:
                        $response[$this->getRetArrID($this->postContent)] = $this->ApiRequest->getVariableList();
                        break;
                    case RequestMap::a()->TableAggregate:
                        throw new Exception('Requesting TableAggregate is not yet implemented');
                        break;
                    case RequestMap::a()->SingleTable:
//                        $response[$this->getRetArrID($this->postContent)] = $this->API->parseTable($this->postContent);
                        break;
                    case RequestMap::a()->Variable:
                        $response[$this->getRetArrID($this->postContent)] = $this->ApiRequest->getVariableData($this->postContent);
                        $response[$this->metaTableName]->groupBy = $this->metaIncludeAfter();
                        break;
                    case RequestMap::a()->View:
                        throw new Exception('Requesting View is not yet implemented');
                        break;
                    case RequestMap::a()->Auxiliary:
                        $response[$this->getRetArrID($this->postContent)] = $this->ApiRequest->getAuxiliary($this->postContent);
                        break;
                    case RequestMap::a()->Menu:
                        $response[$this->getRetArrID($this->postContent)] = $this->ApiRequest->getMenu($this->postContent);
                        break;
                    case RequestMap::a()->Generic:
                        $response[$this->getRetArrID($this->postContent)] = $this->ApiRequest->getVariableMainData($this->postContent);
                        break;
                    case RequestMap::a()->Update:
                        $ApiUpdate = new ApiUpdate();
                        $response[$this->getRetArrID($this->postContent)] = $ApiUpdate->update($this->postContent);
                        break;
                    default:
                        http_response_code(404);
                        exit(0);
                        break;
                }
        } catch (Exception $ex) {
            http_response_code(400);
            $this->ApiRequest->output([$ex->getMessage()]);
            die;
        }
        $endTime = $this->logger->microTimeFloat();
        $this->logger->log('Handling request ' . $this->postContent->requestType . ' took ' . ($endTime - $startTime) . ' seconds');
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
        } elseif ($request->requestType == RequestMap::a()->Menu) {
            return 'Menu';
        } else {
            return $this->genericTableName;
        }
    }

    /**
     * Generates descriptive meta data for the request. Includes constraints, descriptions and information about the variable.
     * @param $request
     * @return stdClass|string
     */
    private function parseRequestMetaData($request) {
        switch ($request->requestType) {
            case RequestMap::a()->Variable:
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
     * @param string[] $content
     */
    function output($content) {

        $options = null;
        if ($GLOBALS['debug']) {
            $options = JSON_UNESCAPED_UNICODE + JSON_UNESCAPED_SLASHES + JSON_NUMERIC_CHECK + JSON_PRETTY_PRINT;
        }
//        header('Content-Type: application/json', false);
        if ($content == null || !is_array($content) ||  count($content) == 0) {
        } else {
            $this->logger->log('Sending data to ' . $_SERVER['REMOTE_ADDR']);
            echo json_encode($content, null);
        }
    }
}

/** Logging class */
class Logger {
    public function log($content, $clearlog = false) {
        if ($clearlog) $this->clearLog();
        $timestamp = date('Y-m-d H:i:s : ');
        file_put_contents('log.log', $timestamp . $content . "\n", FILE_APPEND);
    }

    public function clearLog() {
        file_put_contents('log.log', '');
    }

    function microTimeFloat() {
        list($usec, $sec) = explode(' ', microtime());
        return ((float)$usec + (float)$sec);
    }
}


