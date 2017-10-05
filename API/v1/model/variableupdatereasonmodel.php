<?php
class VariableUpdateReason {
    static function a() {
        $reason = new stdClass();
        $initialCommit = new stdClass();
        $initialCommit->key = 1;
        $initialCommit->value = 'Initial commit';
        $reason->initialCommit = $initialCommit;
        $forceReplaceFull = new stdClass();
        $forceReplaceFull->key = 2;
        $forceReplaceFull->value = 'Table contents replaced in full';
        $reason->forceReplaceFull = $forceReplaceFull;
        $autoIncrementKeyReset = new stdClass();
        $autoIncrementKeyReset-> key = 3;
        $autoIncrementKeyReset->value = 'AUTO_INCREMENT was set to 1';
        $reason->autoIncrementKeyReset = $autoIncrementKeyReset;
        return $reason;
    }


}