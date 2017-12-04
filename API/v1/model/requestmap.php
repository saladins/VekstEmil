<?php
class RequestMap {
    static function a() {
        $ret = new stdClass();
        $ret->TableSurvey = 10;
        $ret->TableAggregate = 20;
        $ret->Detailed = 30;
        $ret->Variable = 40;
        $ret->View = 50;
        $ret->Auxiliary = 60;
        $ret->Menu = 70;
        $ret->Generic = 80;
        $ret->Update = 100;
        return $ret;
    }
}