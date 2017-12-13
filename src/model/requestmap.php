<?php
class RequestMap {
    /**
     * @return stdClass
     */
    static function a() {
        $ret = new stdClass();
        $ret->Detailed = 10;
        $ret->Variable = 20;
        $ret->Description = 30;
        $ret->Related = 40;
        $ret->Links = 50;
        $ret->Auxiliary = 60;
        $ret->Menu = 70;
        $ret->Generic = 80;
        $ret->Update = 100;
        return $ret;
    }
}