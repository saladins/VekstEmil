<?php
    class columnMap {

        /**
         * Lists common column names
         * @return array
         */
        static function columns() {
            $columnMap = array();
            $columnMap[0] = 'municipalityID';
            $columnMap[6] = 'regionID';
            $columnMap[1] = 'ageRangeID';
            $columnMap[2] = 'genderID';
            $columnMap[3] = 'pYear';
            $columnMap[4] = 'pQuarter';
            $columnMap[7] = 'pMonth';
            $columnMap[5] = 'naceID';
            return $columnMap;
        }

        /**
         * Maps columns to the table holding their descriptions
         * @return array
         */
        static function columnsTableParent() {
            $colTableMap = array();
            $colTableMap[self::columns()[0]] = TableMap::getTableMap()[26];
            $colTableMap[self::columns()[1]] = TableMap::getTableMap()[1];
            $colTableMap[self::columns()[2]] = TableMap::getTableMap()[18];
            $colTableMap[self::columns()[3]] = null;
            $colTableMap[self::columns()[4]] = null;
            $colTableMap[self::columns()[5]] = TableMap::getTableMap()[28];
            $colTableMap[self::columns()[6]] = null;
            $colTableMap[self::columns()[7]] = null;

            return $colTableMap;
        }
    }