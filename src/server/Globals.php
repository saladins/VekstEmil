<?php
date_default_timezone_set('UTC');
abstract class Globals {
    /** @var string */
    private static $schemaName = '';
    const dateTimeFormat = 'Y-m-d H:i:s';
    const configFilePath = 'config/config.ini';
    const debugging = false;
    const debuggingOutput = false;
    const resultSet = 'resultSet';
    const meta = 'meta';

    /**
     * @return string
     */
    public static function getAppDir() {
        return $_SERVER['DOCUMENT_ROOT'];
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