<?php
date_default_timezone_set('UTC');
class Globals {
    /** @var string */
    private static $schemaName = '';
    /**
     * @return string
     */
    public static function dateTimeFormat() {
        return 'Y-m-d H:i:s';
    }

    /**
     * @return string
     */
    public static function getConfigFilePath() {
        return 'config/config.ini';
    }

    /**
     * @return string
     */
    public static function getAppDir() {
        return $_SERVER['DOCUMENT_ROOT'];
    }

    /**
     * @return bool
     */
    public static function isDebugging() {
        return true;
    }

    /**
     * @param string $schemaName
     * @return void
     */
    public static function setSchemaName($schemaName) {
        Globals::$schemaName = $schemaName;
    }

    /**
     * @return string
     */
    public static function getSchemaName() {
        return Globals::$schemaName;
    }

}