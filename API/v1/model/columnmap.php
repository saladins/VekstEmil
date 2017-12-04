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
            $columnMap[8] = 'buildingStatusID';
            $columnMap[9] = 'buildingCategoryID';
            $columnMap[10] = 'enterpriseCategoryID';
            $columnMap[11] = 'employeeCountRangeID';
            $columnMap[12] = 'gradeID';
            $columnMap[13] = 'kostraCategoryID';
            $columnMap[14] = 'municipalExpenseCategoryID';
            $columnMap[15] = 'householdTypeID';
            $columnMap[16] = 'proceedingCategoryID';
            $columnMap[17] = 'applicationTypeID';
            $columnMap[18] = 'sectorID';
            $columnMap[19] = 'organizationTypeID';
            $columnMap[20] = 'enterprisePostCategoryID';
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
            $colTableMap[self::columns()[8]] = TableMap::getTableMap()[4];
            $colTableMap[self::columns()[9]] = TableMap::getTableMap()[3];
            $colTableMap[self::columns()[10]] = TableMap::getTableMap()[11];
            $colTableMap[self::columns()[11]] = TableMap::getTableMap()[8];
            $colTableMap[self::columns()[12]] = TableMap::getTableMap()[19];
            $colTableMap[self::columns()[13]] = TableMap::getTableMap()[22];
            $colTableMap[self::columns()[14]] = TableMap::getTableMap()[27];
            $colTableMap[self::columns()[15]] = TableMap::getTableMap()[54];
            $colTableMap[self::columns()[16]] = TableMap::getTableMap()[36];
            $colTableMap[self::columns()[17]] = TableMap::getTableMap()[37];
            $colTableMap[self::columns()[18]] = TableMap::getTableMap()[55];
            $colTableMap[self::columns()[19]] = TableMap::getTableMap()[60];
            $colTableMap[self::columns()[20]] = TableMap::getTableMap()[59];
            return $colTableMap;
        }
    }