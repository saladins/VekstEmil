<?php
    class columnMap {
        static function columns() {
            $columnMap = array();
            $columnMap[0] = 'municipalityID';
            $columnMap[6] = 'regionID';
            $columnMap[1] = 'ageRangeID';
            $columnMap[2] = 'genderID';
            $columnMap[3] = 'pYear';
            $columnMap[4] = 'pQuarter';
            $columnMap[5] = 'naceID';
            return $columnMap;
        }
        static function columnsTableParent() {
            $colTableMap = array();
            $colTableMap[self::columns()[0]] = TableMap::getTableMap()[26];
            $colTableMap[self::columns()[1]] = TableMap::getTableMap()[1];
            $colTableMap[self::columns()[2]] = TableMap::getTableMap()[18];
            $colTableMap[self::columns()[3]] = null;
            $colTableMap[self::columns()[4]] = null;
            $colTableMap[self::columns()[5]] = TableMap::getTableMap()[28];

            return $colTableMap;
        }
    }