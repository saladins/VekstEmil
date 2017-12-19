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
    }