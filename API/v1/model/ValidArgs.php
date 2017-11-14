<?php
class ValidArgs {

    /**
     * Sets supported GET arguments
     * @return stdClass
     */
    static function a() {
        $ret = new stdClass();
        $ret->requestType = 'requesttype';
        $ret->variableID = 'variableid';
        $ret->municipalityID = 'municipalityid';
        $ret->naceID = 'naceid';
        $ret->genderID = 'genderid';
        $ret->kostraCategoryID = 'kostracatid';
        $ret->municipalExpenseCategoryID = 'municipalexpensecatid';
        $ret->buildingCategoryID = 'buildingcategoryid';
        $ret->enterpriseCategoryID = 'enterprisecatid';
        $ret->employeeCountID = 'employeecountid';
        $ret->workingMunicipalityID = 'workingmunicipalityid';
        $ret->livingMunicipalityID = 'livingmunicipalityid';
        $ret->buildingStatusID = 'buildingstatusid';
        $ret->ageRangeID = 'agerangeid';
        $ret->gradeID = 'gradeid';
        $ret->tableName = 'tablename';
        $ret->tableNumber = 'tablenumber';
        $ret->groupBy = 'groupby';
        $ret->years = 'years';
        return $ret;
    }
}