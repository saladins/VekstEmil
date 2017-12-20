<?php
header('Access-Control-Allow-Origin: *');
header('Content-Type: application/json; charset=utf-8', false);
use Firebase\JWT\JWT;
class Authenticate {
    /** @var DatabaseHandler */
    private $db;
    /** @var int */
    private $validFor = 3600;
    /** @var string */
    private $private_key = '';

    /**
     * @param string $configFilePath
     * @throws Exception
     */
    public function __construct($configFilePath) {
        $settings = parse_ini_file($configFilePath, true);
        if (!$settings || !isset($settings['auth']) || !isset($settings['auth']['private_key'])) {
            throw new Exception('Unable to open configuration file. Contact the system administrator');
        }
        $this->private_key = base64_decode($settings['auth']['private_key']);
        $this->db = DatabaseHandlerFactory::getDatabaseHandler();
    }

    function validate() {
        $headers = getallheaders();
        if (array_key_exists('Authorization', $headers)) {
            $jwt = str_replace('Bearer ', '', $headers['Authorization']);
            try {
                $token = JWT::decode($jwt, $this->private_key, array('HS256'));
                if ($token->exp >= time()) {
                    return true;
                } else {
                    http_response_code(401);
                    return false;
                }
            } catch (Exception $exception) {
                http_response_code(401);
                return false;
            }
        } else {
            http_response_code(401);
            return false;
        }

    }

    /**
     * @param string|null $userName
     * @param string|null $password
     * @return boolean
     */
    function authenticate($userName, $password) {
        if ($userName && $password) {
            $sql = 'SELECT userID, userName, userPw FROM Authentication WHERE userName = :userName';
            $this->db->prepare($sql);
            $this->db->bind(':userName', $userName);
            $res = $this->db->getSingleResult();
            if (!$res) {
                http_response_code(401);
                return false;
            }
            $dbID = $res['userID'];
            $dbPw = $res['userPw'];
            if (!password_verify($password, $dbPw)) {
                http_response_code(401);
                return false;
            }
            $token = array();
            $token['id'] = $dbID;
            $token['exp'] = time() + $this->validFor;
            $encoded = JWT::encode($token, $this->private_key);
            if ($encoded) {
                print_r(json_encode(array('token' => $encoded)));
                return true;
            } else {
                return false;
            }

        } else {
            http_response_code(401);
            return false;
        }
    }
}