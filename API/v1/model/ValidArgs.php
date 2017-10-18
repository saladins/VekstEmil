<?php
class ValidArgs {

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
        $ret->ageRangeID = 'agerangeid';
        $ret->gradeID = 'gradeid';
        $ret->tableName = 'tablename';
        $ret->tableNumber = 'tablenumber';
        $ret->groupBy = 'groupby';
        $ret->years = 'years';
        return $ret;

//        return array(
//       'RequestType' => 'requesttype',
//       'VariableID' => 'variableid',
//       'MunicipalityID' => 'municipalityid',
//       'NaceID' =>'naceid',
//       'GenderID' => 'genderid',
//       'KostraCategoryID' => 'kostracatid',
//       'MunicipalExpenseCategoryID' => 'municipalexpensecatid',
//       'BuildingCategoryID' => 'buildingcategoryid',
//       'EnterpriseCategoryID' => 'enterprisecatid',
//       'EmployeeCountID' => 'employeecountid',
//       'WorkingMunicipalityID' => 'workingmunicipalityid',
//       'LivingMunicipalityID' => 'livingmunicipalityid',
//       'AgeRangeID' => 'agerangeid',
//    );
    }
}