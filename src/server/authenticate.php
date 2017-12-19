<?php
header('Access-Control-Allow-Origin: *');
use Firebase\JWT\JWT;
class Authenticate {
    /** @var DatabaseHandler */
    private $db;
    /** @var int */
    private $validFor = 3600;
    /** @var string */
    private $private_key = 'some_private_key';

    /**
     * Authenticate constructor.
     */
    public function __construct() {
        $this->db = DatabaseHandlerFactory::getDatabaseHandler();
    }

    /**
     * @param string $userName
     * @param string $password
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
            $dbUsr = $res['userName'];
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
                echo json_encode(array('token' => $encoded));
                return true;
            } else {
                return false;
            }

        } else {
            $headers = getallheaders();
            if (array_key_exists('Authorization', $headers)) {
                $jwt = $headers['Authorization'];
                $token = JWT::decode($jwt, $this->private_key);
                if ($token->exp >= time()) {
                    //logged in
                    return true;
                } else {
                    http_response_code(401);
                    return false;
                }
            } else {
                http_response_code(401);
                return false;
            }
        }
    }
}