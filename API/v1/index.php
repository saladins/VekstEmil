<?php
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: *', false); // OPTIONS, GET, POST, PUT, DELETE
header('Access-Control-Allow-Headers: Accept, Content-Type', false); // , X-Requested-With, origin, authorization, accept
header('Content-Type: application/json; charset=utf-8', false);
include 'globals.php';
include './model/requestInterface.php';
include './model/tablemap.php';
include './model/requestmap.php';
include './model/columnmap.php';
include './model/ValidArgs.php';
include './model/requestmodel.php';
include './model/variableupdatereasonmodel.php';
include './request/DatabaseHandler.php';
include './request/apirequest.php';
include './request/validate.php';

//session_start();
//$_SESSION = array();
//session_destroy();

$parser = new APIparser();
$parser->handleRequest();

class APIparser {
    /** @var null|RequestModel|RequestModel[] */
    private $postContent;
    /** @var APIrequest */
    private $API;
    private $logger;
    private $genericTableName = 'table';
    private $metaTableName = 'meta';

    public function __construct() {
        $this->logger = new Logger();
        if ($GLOBALS['debug']) $this->logger->clearLog();
        if (file_get_contents('php://input') != null) {
            // TODO migrate POST to inserts
            $this->logger->log('Received POST request');
            $this->parsePost();
        } else { // assume it's GET
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
                    . print_r($this->postContent, true));
            }
            if ($this->API == null) {
                $this->API = new APIrequest();
            }

        }
    }

    private function parseGet() {
        /** @var RequestModel postContent */
        $postContent = new RequestModel();
        if (isset($_GET)) {
            if (isset($_GET[ValidArgs::a()->RequestType])) {
                $postContent->requestType = $_GET[ValidArgs::a()->RequestType];
                $postContent->variableID = (isset($_GET[ValidArgs::a()->VariableID]) ? $_GET[ValidArgs::a()->VariableID] : null);
                if (isset($_GET[ValidArgs::a()->MunicipalityID])) {
                    if (strpos($_GET[ValidArgs::a()->MunicipalityID], ',') != null) {
                        $postContent->MunicipalityID = explode(',', $_GET[ValidArgs::a()->MunicipalityID]);
                    } else {
                        $postContent->MunicipalityID = [$_GET[ValidArgs::a()->MunicipalityID]];
                    }
                }
                if (isset($_GET[ValidArgs::a()->NaceID])) {
                    if (strpos($_GET[ValidArgs::a()->NaceID], ',') != null) {
                        $postContent->naceID = explode(',', $_GET[ValidArgs::a()->NaceID]);
                    } else {
                        $postContent->naceID = [$_GET[ValidArgs::a()->NaceID]];
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
                // TODO the rest
            } else {
                // TODO return capability
            }
        } else {
            return null;
        }
        return $postContent;
    }

    private function parsePost() {
        $this->postContent = json_decode(file_get_contents('php://input'), false);
    }

    public function handleRequest() {
        $startTime = $this->logger->microTimeFloat();
        $response = [];
        $returnArrayIdentifier = $this->genericTableName;
        try {
            if (is_array($this->postContent)) {
                /** @var RequestModel $request */
                foreach ($this->postContent as $request) { // TODO migrate away from tableNumber, use variableID instead
                    $this->API->checkRequestOrDie($request);
                    $response[$returnArrayIdentifier] = $this->parseRequest($request);
                }
            } else {
//                var_dump($this->postContent);
                $this->API->checkRequestOrDie($this->postContent);
//                var_dump($this->postContent);
                $response[$this->metaTableName] = $this->parseRequestMetaData($this->postContent);
                switch ($this->postContent->requestType) {
                    case RequestMap::a()->TableSurvey:
                        $response[$this->getRetArrID($this->postContent)] = $this->API->getVariableTableNames();
                        break;
                    case RequestMap::a()->TableAggregate:
                        throw new Exception('Requesting TableAggregate is not yet implemented');
                        break;
                    case RequestMap::a()->SingleTable:
                        $response[$this->getRetArrID($this->postContent)] = $this->API->parseTable($this->postContent);
                        break;
                    case RequestMap::a()->Variable:
                        $response[$this->getRetArrID($this->postContent)] = $this->API->getVariableData($this->postContent);
                        $response[$this->metaTableName]->groupBy = $this->metaIncludeAfter();
                        break;
                    case RequestMap::a()->View:
                        throw new Exception('Requesting View is not yet implemented');
                        break;
                    case RequestMap::a()->Auxiliary:
                        $response[$this->getRetArrID($this->postContent)] = $this->API->getAuxiliary($this->postContent);
                        break;
                    case RequestMap::a()->Menu:
                        $response[$this->getRetArrID($this->postContent)] = $this->API->getMenu($this->postContent);
                        break;
                    case RequestMap::a()->Generic:
                        throw new Exception('Requesting generic table is not yet implemented');
                        break;
                    case RequestMap::a()->Update:
                        $response[$this->getRetArrID($this->postContent)] = $this->API->update($this->postContent);
                        break;
                    default:
                        http_response_code(404);
                        exit(0);
                        break;
                }


//                $this->API->checkRequestOrDie($this->postContent);
//                $response[$this->metaTableName] = $this->parseRequestMetaData($this->postContent);
//                $response[$this->getRetArrID($this->postContent)] = $this->parseRequest($this->postContent);
//                $response[$this->metaTableName]->groupBy = $this->metaIncludeAfter();
            }
        } catch (Exception $ex) {
            http_response_code(400);
            $this->API->output([$ex->getMessage()]);
            die;
        }
        $endTime = $this->logger->microTimeFloat();
        $this->logger->log('Handling request ' . $this->postContent->requestType . ' took ' . ($endTime - $startTime) . ' seconds');
        $this->output($response);
    }

    /**
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
     * @param   RequestModel $request
     * @return array|string[]
     * @throws Exception
     */
    private function parseRequest($request) {
            switch ($request->requestType) {
                case RequestMap::a()->TableSurvey:
                    return $this->API->getVariableTableNames();
                    break;
                case RequestMap::a()->TableAggregate:
                    throw new Exception('Requesting TableAggregate is not yet implemented');
                    break;
                case RequestMap::a()->SingleTable:
                    return $this->API->parseTable($request);
                    break;
                case RequestMap::a()->Variable:
                    return $this->API->getVariableData($request);
                    break;
                case RequestMap::a()->View:
                    throw new Exception('Requesting View is not yet implemented');
                    break;
                case RequestMap::a()->Auxiliuary:
                    return $this->API->getAuxiliary($request);
                    break;
                case RequestMap::a()->Menu:
                    return $this->API->getMenu($request);
                    break;
                case RequestMap::a()->Generic:
                    throw new Exception('Requesting generic table is not yet implemented');
                    break;
                case RequestMap::a()->Update:
                    return $this->API->update($request);
                    break;
                default:
                    http_response_code(404);
                    exit(0);
                    break;
            }

    }

    private function parseRequestMetaData($request) {
        switch ($request->requestType) {
            case RequestMap::a()->Variable:
                return $this->API->getMinimalMetaData($request);
                break;
            default:
                return '{}';
                break;
        }
    }
    private function metaIncludeAfter() {
        return $this->API->metaIncludeAfter();
    }

    /**
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
            $this->logger->log('Sending data to ' . $_SERVER['REMOTE_ADDR'] . ' with content ' . print_r($content, true));
            echo json_encode($content, null);
        }
    }

}

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


