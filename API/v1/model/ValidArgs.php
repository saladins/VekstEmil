<?php
class ValidArgs {

    static function a() {
        $ret = new stdClass();
        $ret->RequestType = 'requesttype';
        $ret->VariableID = 'variableid';
        $ret->MunicipalityID = 'municipalityid';
        $ret->NaceID = 'naceid';
        $ret->genderID = 'genderid';
        $ret->KostraCategoryID = 'kostracatid';
        $ret->MunicipalExpenseCategoryID = 'municipalexpensecatid';
        $ret->BuildingCategoryID = 'buildingcategoryid';
        $ret->EnterpriseCategoryID = 'enterprisecatid';
        $ret->EmployeeCountID = 'employeecountid';
        $ret->WorkingMunicipalityID = 'workingmunicipalityid';
        $ret->LivingMunicipalityID = 'livingmunicipalityid';
        $ret->ageRangeID = 'agerangeid';
        $ret->gradeID = 'gradeid';
        $ret->tableName = 'tablename';
        $ret->tableNumber = 'tablenumber';
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