<?php
/** Class for API (GET) requests */
class ApiRequest {
    /** @var DatabaseHandler */
    private $db;
    /** @var Logger */
    private $logger;
    /** @var array */
    private $binds = [];

    public function __construct() {
        $this->logger = new Logger();
        $this->db = DatabaseHandlerFactory::getDatabaseHandler();
    }

    /**
     * Validates request or throws exception
     * @param RequestModel $request
     * @throws Exception
     * @return void
     */
    public function checkRequestOrDie($request) {
        $validate = new Validate($this->db);
        $validate->checkRequestOrDie($request);
    }


    public function insertRelatedDocument($request) {
        $error = null;

        $sql = <<<SQL
INSERT INTO 
VariableLinkedDocument(variableID, linkedDocumentAddress, linkedDocumentTitle, linkedDocumentDescription) 
VALUES(:id, :url,:title,:description);
SQL;
        $this->db->prepare($sql);
        $this->db->bind(':id', $request->variableID);
        $this->db->bind(':url', $request->url);
        $this->db->bind(':title', $request->title);
        $this->db->bind(':description', $request->description);
        $this->db->execute();

        return $error;
    }

    public function deleteRelatedDocument($request) {
        $error = null;

        $sql = <<<SQL
DELETE FROM VariableLinkedDocument 
WHERE linkedDocumentID = :documentID
SQL;
        $this->db->prepare($sql);
        $this->db->bind(':documentID', $request->documentID);
        $this->db->execute();

        return $error;
    }


    public function updateVariableSettings($request) {
        $error = null;

        /*
         * Insert related variables first
         */

        //Start transaction
        $this->db->beginTransaction();

        //remove all related variables (easiest, as we need to commit)
        $delete_sql = "DELETE FROM VariableRelated WHERE parentVariableID = :id";
        $this->db->prepare($delete_sql);
        $this->db->bind(':id', $request->variableID);
        $this->db->execute();

        //Sanitize
        $relatedVariables = preg_replace('/[^0-9,]/', '', $request->relatedVariables);
        //Check if any related variables is set
        if($relatedVariables != '') {
            //Create an array containing related variables
            $relatedVariables = explode(',', $relatedVariables);


            //Loop trough all related variables and add
            foreach ($relatedVariables as $related) {
                $sql = "INSERT INTO VariableRelated VALUES(:id, :related)";
                $this->db->prepare($sql);
                $this->db->bind(':id', $request->variableID);
                $this->db->bind(':related', $related);
                $this->db->execute();
            }
        }

        $updateSql = <<<SQL
        UPDATE Variable 
        SET statisticName = :title, description = :description
        WHERE variableID = :id;
SQL;
        $this->db->prepare($updateSql);

        $this->db->bind(':title', $request->title);
        $this->db->bind(':description', $request->description);
        $this->db->bind(':id', $request->variableID);
        $this->db->execute();

        $this->db->commit();

        return $error;
    }

    public function getVariableSettings($variableID) {
        $sql = <<<SQL
        SELECT variableID, statisticName, description, R.relatedVariables 
        FROM Variable AS V LEFT JOIN (
            SELECT parentVariableID, GROUP_CONCAT(relatedVariableID SEPARATOR ',') AS relatedVariables
            FROM VariableRelated
            GROUP BY parentVariableID
        ) AS R
        ON V.variableID = R.parentVariableID
        WHERE variableID = :id;
SQL;

        $this->db->prepare($sql);
        $this->db->bind(':id', $variableID);
        return $this->db->getResultSet(PDO::FETCH_CLASS);
    }

    /**
     * @param RequestModel $request
     * @return mixed
     */
    public function getDetailedData($request) {
        ini_set('max_execution_time', 300);
        switch ($request->variableID) {
            case 1:
                $sql = <<<SQL
SELECT 
municipalityID,
ageRangeID,
genderID,
pYear,
population as value
from PopulationAge
order by municipalityID, municipalityid, value;
SQL;
                break;
            case 8:
                $sql = <<<SQL
SELECT EE.municipalityID, EE.pYear, naceID, PC.livingplaceValue, valueInNOK AS value   
FROM EnterpriseEntryEBIDTA AS EE,(
	SELECT municipalityID, pYear,  SUM(livingplaceValue) AS livingplaceValue
    FROM Employment 
    GROUP BY municipalityID, pYear
) AS PC
WHERE PC.municipalityID = EE.municipalityID
	AND PC.pYear = EE.pYear
ORDER BY municipalityID, EE.pYear;
SQL;
                break;
            case 11:
                $sql = <<<SQL
SELECT municipalityID, buildingStatusID, buildingCategoryID, pYear, pQuarter, buildingValue AS value
FROM HomeBuildingArea
WHERE pYear BETWEEN (SELECT MAX(pYear)-4 FROM HomeBuildingArea) AND (SELECT MAX(pYear) FROM HomeBuildingArea)
	AND municipalityID IN (3,4,5)
	AND buildingStatusID BETWEEN 4 AND 6
ORDER BY municipalityID
SQL;
                break;
            case 12:
                $sql = <<<SQL
SELECT municipalityID, buildingStatusID, buildingCategoryID, pYear, pQuarter, buildingValue AS value
FROM FunctionalBuildingArea
WHERE pYear BETWEEN (SELECT MAX(pYear)-4 FROM FunctionalBuildingArea) AND (SELECT MAX(pYear) FROM FunctionalBuildingArea)
	AND municipalityID IN (1,2,3)
	AND buildingStatusID BETWEEN 1 AND 3
ORDER BY municipalityID
SQL;
                break;

                break;

                break;
            case 14:
/*
                $sql = <<<SQL
SELECT Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID, enterpriseID, answerText AS value 
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = Survey_Answer.questionID
AND SurveyQuestionAnswer.surveyID = Survey_Answer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = Survey_Answer.givenAnswerID
AND Survey_Question.questionID = 211;
SQL;
*/

            $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 211
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value
SQL;

break;
            case 34:
                $sql = <<<SQL
SELECT municipalityID, buildingStatusID, buildingCategoryID, pYear, pQuarter, buildingValue AS value
FROM HomeBuildingArea
WHERE municipalityID IN (1,2,3)
	AND buildingStatusID = 6
ORDER BY municipalityID
SQL;
                break;
            case 35:
                $sql = <<<SQL
SELECT municipalityID, buildingStatusID, buildingCategoryID, pYear, pQuarter, buildingValue AS value
FROM HomeBuildingArea
WHERE municipalityID IN (1,2,3)
	AND buildingStatusID = 11
ORDER BY municipalityID
SQL;
                break;
            case 36:
                $sql = <<<SQL
SELECT municipalityID, buildingStatusID, buildingCategoryID, pYear, pQuarter, buildingValue AS value
FROM HomeBuildingArea
WHERE municipalityID IN (1,2,3)
	AND buildingStatusID = 13
ORDER BY municipalityID
SQL;
                break;
            case 39:
                $sql = <<<SQL
SELECT municipalityID, buildingStatusID, buildingCategoryID, pYear, pQuarter, buildingValue AS value
FROM HomeBuildingArea
WHERE municipalityID IN (1,2,3)
	AND buildingStatusID = 13
ORDER BY municipalityID
SQL;
                break;
            case 42:
                $sql = <<<SQL
SELECT municipalityID, naceID, pYear, livingPlaceValue AS value 
FROM EmploymentDetailed WHERE pYear =(
	SELECT MAX(pYear) FROM EmploymentDetailed)
GROUP BY municipalityID, naceID, pYear, livingPlaceValue;
SQL;
                break;
            case 43:
                $sql = <<<SQL
SELECT municipalityID, pYear, kostraCategoryID, SUM(expense) FROM RegionalCooperation
WHERE municipalExpenseCategoryID = 2
GROUP BY municipalityID, pYear;
SQL;
                break;
            case 48:
                $sql = <<<SQL
SELECT municipalityID, pYear, naceID, SUM(valueInNOK) AS value 
FROM EnterpriseEntry, Enterprise
WHERE Enterprise.enterpriseID = EnterpriseEntry.enterpriseID AND EnterpriseEntry.enterprisePostCategoryID = 7
AND Enterprise.municipalityID <= 3
GROUP BY municipalityID, pYear, naceID;
SQL;
                break;
            case 59:
                $sql = <<<SQL
SELECT 
	M.municipalityID, 
    M.pYear, 
	M.sumAll AS sumMovement, 
    PC.sumBorn, 
    I.sumAll AS sumMigration
FROM Movement AS M, Immigration AS I, (
	SELECT municipalityID, pYear, SUM(born - dead) AS sumBorn
	FROM PopulationChange 
	GROUP BY municipalityID, pYear
) AS PC
WHERE M.municipalityID = PC.municipalityID
	AND M.municipalityID = I.municipalityID
    AND M.pYear = PC.pYear
    AND M.pYear = I.pYear
    AND M.municipalityID IN (3,4,5) -- only fetch results from hadelandsregionen
GROUP BY M.municipalityID, M.pYear, sumMovement, sumBorn, sumMigration
SQL;

                break;


            case 62:


                $sql =<<<'SQL'
SELECT  M.municipalityID, M.pYear, municipalIncomeCategoryCode, municipalIncomeCategoryText, income as value, ROUND(income * 1000 / totalPopulation, 2) AS totalValue
FROM MunicipalEconomy AS M, MunicipalIncomeCategory AS C, 
(SELECT municipalityID, pYear, totalPopulation FROM PopulationChange
	WHERE pQuarter = 4) AS PC
WHERE M.municipalityID = PC.municipalityID
AND M.pYear = PC.pYear
AND M.municipalIncomeCategoryID = C.municipalIncomeCategoryID 
AND M.pYear < 2015 -- limit rows to 2013-2014
SQL;
                break;
            case 63:
                $sql =<<<'SQL'
SELECT municipalityID, pYear, SUM(born) as born, SUM(dead) as dead, AVG(totalPopulation) AS value 
FROM PopulationChange
GROUP BY municipalityID, pYear;
SQL;
                break;
            case 64:
                $sql = <<<'SQL'
SELECT 
municipalityID, 
pYear, 
Boligtype, 
antallBoliger
FROM
	(SELECT municipalityID, pYear,
	 CASE
		WHEN buildingCategoryCode >= 151 THEN 'Andre boliger'
		WHEN buildingCategoryCode >= 141 THEN 'Boligblokker'
		WHEN buildingCategoryCode >= 131 THEN 'Rekkehus' 
		WHEN buildingCategoryCode >= 121 THEN 'Tomannsbolig'
		ELSE 'Enebolig'
		END AS Boligtype,  
	SUM(buildingValue) AS antallBoliger
	FROM HomeBuildingArea AS BA, BuildingCategory AS BC 
	WHERE BA.buildingCategoryID =BC.buildingCategoryID
	AND buildingStatusID = 13
	GROUP BY municipalityID, pYear, Boligtype) AS U
GROUP BY municipalityID, pYear, Boligtype, antallBoliger
SQL;

                break;
                break;
            case 73:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 212
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 74:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 213
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 75:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 214
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 76:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 215
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 77:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 225
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 78:
                $sql = <<<SQL
SELECT Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID, EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value 
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 227
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016;
SQL;
                break;
            case 79:
                $sql = <<<SQL
SELECT Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID, enterpriseID, answerText AS value 
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = Survey_Answer.questionID
AND SurveyQuestionAnswer.surveyID = Survey_Answer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = Survey_Answer.givenAnswerID
AND Survey_Question.questionID BETWEEN 230 AND 253 
AND answerText NOT LIKE '' 
AND answerText != 0;
SQL;
                break;
            case 80:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 216
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 81:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 217
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 82:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 218
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 83:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 219
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 84:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 220
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 85:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 221
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 86:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 222
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 87:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID = 223
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 88:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID  IN(255,259) 
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 89:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID  IN(256,260) 
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 90:
                $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID  IN(257,261) 
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
AND E.municipalityID IN (1,2,3)
ORDER BY value 
SQL;
                break;
            case 91:
                /*
                $sql = <<<SQL
SELECT Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID, EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value 
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID BETWEEN 211 AND 215
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016;
SQL;
                */

               $sql = <<<SQL
SELECT S.surveyID, YEAR(S.startDate) as pYear, Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID,
 EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, E.municipalityID, EE.valueInNOK, answerText AS value,
 CASE 
	WHEN EE.valueInNOK > 10000  THEN 'Over 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 1000  THEN 'Mellom 1 og 10 mill'
    WHEN EE.valueInNOK > 0  THEN 'Mindre enn 1 mill'
    WHEN EE.valueInNOK < 0  THEN 'Negativ verdiskapning'
    ELSE 'Ukjent verdiskapning'
 END AS ebidta_breakpoint,
  CASE 
	WHEN E.employees > 50  THEN '50+'
    WHEN E.employees > 10  THEN '10-49'
    WHEN E.employees > 0  THEN '1-9'
    ELSE  'Ingen ansatte'

 END AS employee_breakpoint
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, Survey AS S, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND S.surveyID = SurveyQuestionAnswer.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID BETWEEN 211 AND 215
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016;
SQL;

                break;
            case 9:
                $sql = <<<SQL
SELECT a.enterpriseID, a.municipalityID, a.naceID, c.vareideCategoryDescriptionID, a.employees, b.valueInNOK as value
FROM Enterprise a, EnterpriseEntry b, VareideCategory c
WHERE a.enterpriseID = b.enterpriseID
AND a.naceID = c.naceID
AND b.enterprisePostCategoryID = 7
AND pYear = 2016;
SQL;

             /*   $sql = <<<SQL
SELECT Survey_Question.questionID, Survey_GivenAnswer.givenAnswerID, EE.enterpriseID, VC.vareideCategoryDescriptionID, E.employees, EE.valueInNOK, answerText AS value 
FROM Survey_GivenAnswer, SurveyQuestionAnswer, Survey_SurveyQuestion, Survey_Question, Survey_Answer AS SA, EnterpriseEntry AS EE, Enterprise AS E, VareideCategory AS VC
WHERE Survey_GivenAnswer.givenAnswerID = SurveyQuestionAnswer.givenAnswerID
AND SurveyQuestionAnswer.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.questionID = Survey_Question.questionID
AND SurveyQuestionAnswer.questionID = SA.questionID
AND SurveyQuestionAnswer.surveyID = SA.surveyID
AND SurveyQuestionAnswer.givenAnswerID = SA.givenAnswerID
AND SA.questionID BETWEEN 211 AND 215
AND SA.EnterpriseID = EE.EnterpriseID
AND E.EnterpriseID = EE.EnterpriseID
AND E.naceID = VC.naceID
AND enterprisePostCategoryID = 7
AND pYear = 2016
SQL;*/
                break;
            case 94:
                $sql = <<<SQL
SELECT M.municipalityID, M.pYear, ROUND((sumAll /P.totalPopulation) * 100,2) AS changePercent
FROM Movement AS M, PopulationChange AS P, Municipality AS MP
WHERE M.municipalityID = P.municipalityID
    AND P.pYear = M.pYear
    AND P.pQuarter = 4
    AND M.municipalityID = MP.municipalityID
    ORDER BY P.pYear, MP.municipalityOrder;
SQL;
                break;
            case 95:
                /*
                $sql = <<<SQL
SELECT E.municipalityID,  E.pYear, totalPopulation AS population, livingplaceValue AS employment, workplaceValue AS jobs, EBIDTA
FROM 
	( 
	SELECT municipalityID, pYear, SUM(workplaceValue) AS workplaceValue, SUM(livingplaceValue) AS livingplaceValue 
	FROM Employment AS E
	GROUP BY municipalityID, pYear) AS E,
	(SELECT municipalityID, pYear, totalPopulation FROM PopulationChange
	WHERE pQuarter = 4) AS PC,
	(SELECT EE.municipalityID, EE.pYear, SUM(valueInNOK) AS EBIDTA
	FROM EnterpriseEntryEBIDTA AS EE
	GROUP BY EE.municipalityID, EE.pYear) AS EBIDTA
WHERE E.municipalityID = PC.municipalityID
	AND E.municipalityID = EBIDTA.municipalityID
    AND E.pYear = PC.pYear
    AND E.pYear = EBIDTA.pYear; 
SQL;
*/
                $sql = <<<SQL
SELECT E.municipalityID,  E.pYear, totalPopulation AS population, livingplaceValue AS employment, workplaceValue AS jobs, PC.value AS EBIDTA
FROM 
	( 
	SELECT municipalityID, pYear, SUM(workplaceValue) AS workplaceValue, SUM(livingplaceValue) AS livingplaceValue 
	FROM Employment AS E
	GROUP BY municipalityID, pYear) AS E,
    
	(SELECT P.municipalityID, P.pYear, totalPopulation, EB.value
		FROM PopulationChange AS P
    	LEFT JOIN (
			SELECT EE.municipalityID, EE.pYear, SUM(valueInNOK) AS value
			FROM EnterpriseEntryEBIDTA AS EE
			GROUP BY EE.municipalityID, EE.pYear) EB
		ON P.municipalityID = EB.municipalityID
			AND P.pYear = EB.pYear
	WHERE pQuarter = 4) AS PC
WHERE E.municipalityID = PC.municipalityID
    AND E.pYear = PC.pYear
    AND E.municipalityID != 33
SQL;

                break;


            case 97:
                $sql = <<<SQL
SELECT MD.municipalityID, MD.pYear, municipalDetailedCategoryCode, municipalDetailedCategoryText, value, ROUND(value * 1000 / totalPopulation, 2) AS totalValue 
FROM municipaldeailted AS MD,
(SELECT municipalityID, pYear, totalPopulation FROM PopulationChange
	WHERE pQuarter = 4) AS PC,
    municipaldeailtedcategory AS C
WHERE MD.municipalityID = PC.municipalityID
AND MD.pYear = PC.pYear
AND MD.municipaldeailtedcategory = C.id
SQL;
                break;
            case 98:
                $sql = <<<SQL
SELECT municipalityID, pYear, householdIncomeAvg AS value 
FROM HouseholdIncome
WHERE householdTypeID = 1
SQL;

                break;
            default:
                throw new PDOException('There is no SQL for this variable ID');

        }
        $this->db->query($sql);
        $result = $this->db->getResultSet();
        if (isset($result[0]['value'])) {
            for ($i = 0; $i < sizeof($result); $i++) {
                $var = $result[$i]['value'];
                if (is_numeric($var)) {
                    if (is_double($var)) {
                        $result[$i]['value'] = floatval($var);
                    } else {
                        $result[$i]['value'] = intval($var);
                    }
                }
            }
        }
        return $result;
    }

    /**
     * Gets and returns data about at specific variable (data table)
     * @param RequestModel $request
     * @return array String array containing variable data
     * @throws Exception Throws exception when no valid table name is supplied
     */
    public function getVariableData($request) {
        switch ($request->tableName) {
            case 'Bankruptcy': // Bankruptcy
                $sql = <<<SQL
SELECT 
Bankruptcy.municipalityID,
naceID,
pYear,
pQuarter,
bankruptcies as value
FROM Bankruptcy
SQL;

                break;
            case 'ClosedEnterprise': // ClosedEnterprise
                $sql = <<<SQL
SELECT
ClosedEnterprise.municipalityID,
naceID,
pYear,
closedEnterprises as value
FROM ClosedEnterprise
SQL;
                break;

            case 'CommuteBalance': // CommuteBalance
                $sql = <<<SQL
SELECT 
CommuteBalance.municipalityID,
workingMunicipalityID,
pYear,
commuters as value
FROM CommuteBalance
SQL;
                break;
            case 'Education': //Education
                $sql = <<<SQL
SELECT 
Education.municipalityID,
genderID,
gradeID,
pYear,
percentEducated as value
FROM Education
SQL;
                break;
            case 'Employment': // Employment
                $sql = <<<SQL
SELECT 
Employment.municipalityID,
naceID,
genderID,
pYear,
workplaceValue,
livingplaceValue,
employmentBalance
from Employment
SQL;
                break;
            case 'EmploymentDetailed':
                $sql = <<<SQL
SELECT
EmploymentDetailed.municipalityID,
naceID,
genderID, 
pYear,
workplaceValue,
livingPlaceValue,
employmentBalance
FROM EmploymentDetailed
SQL;
                break;
            case 'EmploymentRatio': // EmploymentRatio
                $sql = <<<SQL
SELECT employmentRatioID,
EmploymentRatio.municipalityID,
genderID,
ageRangeID,
pYear,
EmploymentPercent as value
FROM EmploymentRatio
SQL;
                break;
            case 'EmploymentSector': //EmploymentSector
                $sql = <<<SQL
SELECT
EmploymentSector.municipalityID,
naceID,
sectorID,
pYear,
workplaceValue,
livingplaceValue
FROM EmploymentSector
SQL;
                break;
            case 'Enterprise':
                $sql = <<<SQL
SELECT 
Enterprise.municipalityID,
naceID,
employees,
enterpriseName,
organizationNumber,
organizationTypeID
FROM Enterprise
SQL;
                break;
            case 'FunctionalBuildingArea': // FunctionalBuildingArea
                $sql = <<<SQL
SELECT 
FunctionalBuildingArea.municipalityID,
buildingStatusID,
buildingCategoryID,
pYear,
pQuarter,
buildingValue as value
FROM FunctionalBuildingArea
SQL;
                break;
            case 'HomeBuildingArea': // HomeBuildingArea
                $sql = <<<SQL
SELECT 
HomeBuildingArea.municipalityID,
buildingStatusID,
buildingCategoryID,
pYear,
pQuarter,
buildingValue as value
FROM HomeBuildingArea
SQL;
                break;
            case 'HouseholdIncome': //HouseholdIncome
                $sql = <<<SQL
SELECT
HouseholdIncome.municipalityID,
householdTypeID,
pYear,
householdIncomeAvg as value
FROM HouseholdIncome
SQL;
                break;
            case 'ImmigrantPopulation':
                $sql = <<<'SQL'
SELECT 
ImmigrantPopulation.municipalityID, 
genderID, 
countryBackgroundID, 
pYear, 
persons as value
FROM ImmigrantPopulation
SQL;
                break;
            case 'Immigration':
                $sql = <<<'SQL'
SELECT 
Immigration.municipalityID,
pYear, 
incomingAll as incoming, 
outgoingAll as outgoing, 
sumAll as value
FROM Immigration
SQL;
                break;
            case 'Movement': // Movement
                $sql = <<<SQL
SELECT
Movement.municipalityID,
pYear,
incomingAll as incoming, outgoingAll as outgoing, sumAll as value
from Movement 
SQL;
                break;
            case 'MunicipalEconomy':
                $sql = <<<'SQL'
SELECT 
MunicipalEconomy.municipalityID, 
municipalIncomeCategoryID, 
pYear, 
income AS value 
FROM MunicipalEconomy
SQL;
                break;
            case 'NewEnterprise': //NewEnterprise
                $sql = <<<SQL
SELECT 
NewEnterprise.municipalityID,
enterpriseCategoryID,
employeeCountRangeID,
pYear,
newEnterprises as value
FROM NewEnterprise
SQL;
                break;
            case 'PopulationAge': // 'PopulationAge':
                $sql = <<<SQL
SELECT 
PopulationAge.municipalityID,
ageRangeID,
genderID,
pYear,
population as value
from PopulationAge
SQL;
                break;
            case 'PopulationChange': // 'PopulationChange':
                $sql = <<<SQL
SELECT
PopulationChange.municipalityID,
pYear,
pQuarter,
born,
dead,
totalPopulation as value
from PopulationChange
SQL;
                break;
            case 'PopulationEstimation':
                $sql = <<<'SQL'
SELECT
PopulationEstimation.municipalityID,
populationEstimationTypeID,
genderID,
ageRangeID,
pYear,
population as value
FROM PopulationEstimation
SQL;
                break;
            case 'PrivateEmployee': // PrivateEmployee
                $sql = <<<SQL

SQL;
                break;
            case 'Proceeding': // Proceeding
                $sql = <<<SQL
SELECT
Proceeding.municipalityID,
proceedingCategoryID,
applicationTypeID,
pYear,
proceedingValue as value
FROM Proceeding
SQL;
                break;
            case 'RegionalCooperation': //RegionalCooperation
                $sql = <<<SQL
SELECT
RegionalCooperation.municipalityID,
kostraCategoryID,
municipalExpenseCategoryID,
pYear,
expense as value
FROM RegionalCooperation
SQL;
                break;
            case 'Unemployment': // Unemployment
                $sql = <<<SQL
SELECT 
unemploymentID,
Unemployment.municipalityID,
ageRangeID,
pYear,
pMonth,
unemploymentPercent as value
FROM Unemployment
SQL;
                break;
            case 'municipaldeailted': // Unemployment
                $sql = <<<SQL
SELECT id, municipalityID, pYear, municipaldeailtedcategory, value 
FROM municipaldeailted
SQL;
                break;
            default:
               throw new Exception('No table name or variable id specified');
        }
        $sql .= $this->getSqlConstraints($request);
        $sql .= $this->getGroupByClause($request);
        $this->logger->log($sql);
        $this->db->query($sql);
        if (sizeof($this->binds) > 0) {
            $result = $this->db->getResultSetWithBinding($this->binds);
        } else {
            $result = $this->db->getResultSet();
        }
        if (isset($result[0]['value'])) {
            for ($i = 0; $i < sizeof($result); $i++) {
                $var = $result[$i]['value'];
                if (is_numeric($var)) {
                    if (is_double($var)) {
                        $result[$i]['value'] = floatval($var);
                    } else {
                        $result[$i]['value'] = intval($var);
                    }
                }
            }
        }
        return $result;
    }

    /**
     * Gets constraints, descriptions and variable meta data for the variable (data table)
     * @param RequestModel $request
     * @return stdClass
     */
    public function getMetaData($request) {
        $ret = new stdClass();
        $ret->constraints = $this->getConstraints($request->tableName);
        $ret->descriptions = $this->getDescriptions($request->tableName);
        $ret->variable = $this->getVariableAndProvider($request->variableID);
        return $ret;
    }

    /**
     * @param string $tableName
     * @return stdClass
     */
    private function getConstraints($tableName) {
        $constraints = new stdClass();
//        if ($tableName === 'Survey') {
//            $itemSql = <<<SQL
//SELECT DISTINCT Survey_Question.questionID FROM Survey_Question, Survey_SurveyQuestion, Survey
//WHERE Survey_Question.questionID = Survey_SurveyQuestion.questionID
//AND Survey_SurveyQuestion.surveyID = Survey.surveyID
//SQL;
//            $this->db->query($itemSql);
//            $constraints->SurveyQuestion = [];
//            foreach ($this->db->getResultSet(PDO::FETCH_NUM) as $item) {
//                array_push($constraints->SurveyQuestion, $item[0]);
//            }
//        } else {
            foreach ($this->getBearingColumns($tableName) as $column) {
                $itemSql = "SELECT DISTINCT {$column['Field']} FROM $tableName;";
                $this->db->query($itemSql);
                $constraints->{$column['Field']} = [];
                foreach ($this->db->getResultSet(PDO::FETCH_NUM) as $item) {
                    array_push($constraints->{$column['Field']}, $item[0]);
                }
            }
//        }
        return $constraints;
    }

    /**
     * @param string $tableName
     * @return stdClass
     * @throws PDOException
     */
    private function getDescriptions($tableName) {
        $descriptions = new stdClass();
        if ($tableName === 'Survey') {
            $itemSql = <<<SQL
SELECT DISTINCT Survey_Question.questionID, Survey_Question.questionText, Survey_Question.questionTextVariant1 
FROM Survey_Question, Survey_SurveyQuestion, Survey
WHERE Survey_Question.questionID = Survey_SurveyQuestion.questionID
AND Survey_SurveyQuestion.surveyID = Survey.surveyID
SQL;
            $this->db->query($itemSql);
            $descriptions->questionID = [];
            foreach ($this->db->getResultSet(PDO::FETCH_CLASS) as $item) {
                array_push($descriptions->questionID, $item);
            }
            $itemSql = <<<SQL
SELECT vareideCategoryDescriptionID, vareideCategoryDescriptionText FROM VareideCategoryDescription
SQL;
            $this->db->query($itemSql);
            $descriptions->vareideCategoryDescriptionID = [];
            foreach ($this->db->getResultSet(PDO::FETCH_CLASS) as $item) {
                array_push($descriptions->vareideCategoryDescriptionID, $item);
            }
            $itemSql = <<<SQL
SELECT municipalityID, municipalityCode, municipalityName FROM Municipality
SQL;
            $this->db->query($itemSql);
            $descriptions->municipalityID = [];
            foreach ($this->db->getResultSet(PDO::FETCH_CLASS) as $item) {
                array_push($descriptions->municipalityID, $item);
            }
        } else {
            foreach ($this->getBearingColumns($tableName) as $column) {
                $tableName = ucfirst(substr($column['Field'], 0, -2));
                if ($tableName === 'Variable') {
                    continue;
                }
                $itemSql = "SELECT DISTINCT * FROM $tableName";
                try {
                    $this->db->query($itemSql);
                    $descriptions->{$column['Field']} = [];
                    foreach ($this->db->getResultSet(PDO::FETCH_CLASS) as $item) {
                        array_push($descriptions->{$column['Field']}, $item);
                    }
                } catch (PDOException $exception) {
//                 If table is NOT one of variableID or date field, throw the error
                    if ($exception->getCode() !== '42S02') {
                        throw $exception;
                    }
                }
            }
        }
        return $descriptions;
    }

    /**
     * @param string $tableName
     * @return mixed
     */
    private function getBearingColumns($tableName) {
        $sql = "SHOW COLUMNS FROM $tableName WHERE `Null` LIKE 'no' AND Extra NOT LIKE '%increment%'";
        $this->db->query($sql);
        return $this->db->getResultSet();
    }

    /**
     * Gets variable and provider metadata for the variable (data table)
     * @param integer $variableID
     * @return mixed
     */
    private function getVariableAndProvider($variableID) {
        $sql = <<<'SQL'
SELECT a.variableID, a.providerID, a.statisticName, a.description, a.tableName, 
a.lastUpdatedDate, a.providerCode, 
b.providerName, b.providerNameShortForm, b.providerNotice, 
b.providerLink, b.providerAPIAddress, c.subCategoryName
FROM Variable a, VariableProvider b, VariableSubCategory c
WHERE a.providerID = b.providerID
AND a.subCategoryID = c.subCategoryID
AND   a.variableID = :variableID
SQL;
        $this->db->prepare($sql);
        $this->db->bind(':variableID', $variableID);
        return $this->db->getResultSet(PDO::FETCH_CLASS)[0];
    }

    /**
     * Determines and sets any SQL constraints (where clause) on the final SQL query string.
     * First checks whether or not the requested constraint is valid (determined by columnMap::columns() class).
     * Then adds the constraint to the SQL query.
     * @param RequestModel $request
     * @return string
     * @throws Exception
     */
    private function getSqlConstraints($request) {
        if ($request->constraints) {
            $pre = ', Municipality WHERE ' .$request->tableName . '.municipalityID = Municipality.municipalityID';
            $this->binds = [];
            $a = [];
            foreach ($request->constraints as $constraintName => $valueArray) {
                $in = str_repeat('?,', count($valueArray) - 1) . '?';
                array_push($a, $request->tableName . '.' . $constraintName . " IN ($in)");
                $this->bindLater($valueArray);
            }
            return $pre . ' AND ' . implode(' AND ', $a);
        } else {
            return '';
        }
    }

    /**
     * 'Hack'. Is called after sql string has been completed
     * @param mixed $value
     * @return void
     */
    private function bindLater($value) {
        $this->binds = array_merge($this->binds, $value);
    }

    /**
     * Determines and sets the SQL group by on the final SQL string.
     * If no group by is set tries to determine based on some simple rules.
     * @param RequestModel $request
     * @return string
     */
    private function getGroupByClause($request) {
        $pre = '';
        $sqlGetTableColumns = 'SHOW COLUMNS FROM ' . $request->tableName;
        $this->db->query($sqlGetTableColumns);
        $dbResult = $this->db->getResultSet();
        $order = [];
        foreach ($dbResult as $item) {
            switch ($item['Field']) {
                case 'municipalityID':
                    if (!$request->constraints) {
                        $pre = ', Municipality WHERE ' .$request->tableName . '.municipalityID = Municipality.municipalityID';
                    }
                    array_push($order, 'municipalityOrder');
                    break;
                case 'pYear':
                    array_push($order, 'pYear');
                    break;
                case 'pQuarter':
                    array_push($order, 'pQuarter');
                    break;
                case 'pMonth':
                    array_push($order, 'pMonth');
                    break;
            }
        }
        return $pre . (sizeof($order) > 0 ? ' ORDER BY ' . implode(',', $order) : '');
    }

    /**
     * @return array
     */
    public function getMenu() {
        $menu = new Menu($this->db);
        return $menu->getMenu();
    }

    /**
     * @param integer $variableID
     * @return stdClass
     */
    public function getDescription($variableID) {
        $description = new Description($this->db);
        return $description->getVariableDescription($variableID);
    }

    /**
     * @param integer $variableID
     * @return stdClass
     */
    public function getRelated($variableID) {
        $related = new Related($this->db);
        return $related->getRelated($variableID);
    }

    /**
     * @param integer $variableID
     * @return stdClass
     */
    public function getLinks($variableID) {
        $links = new Links($this->db);
        return $links->getLinks($variableID);
    }

    /**
     * Gets and returns list of tags for the variable (data table)
     * @param integer $variableID
     * @return array
     */
    public function getTags($variableID) {
        $tags = new Tags($this->db);
        return $tags->getTagsForVariable($variableID);
    }

    public function getSearchResult($searchTerm) {
        $search = new Search($this->db, $this->logger);
        return $search->searchTerm($searchTerm);
    }

    public function getDataTables() {
        $dataTables = new DataTables($this->db);
        return $dataTables->getDataTables();
    }

    /**
     * @param string $needle
     * @param string[] $haystack
     * @return bool
     */
    function in_arrayi($needle, $haystack) {
        return in_array(strtolower($needle), array_map('strtolower', $haystack));
    }
}