-- Active: 1712538144975@@localhost@3306@bakeryadv

-- Creating the stored procedure.
DELIMITER $$
DROP PROCEDURE IF EXISTS copy_and_clean_data;
CREATE PROCEDURE copy_and_clean_data()
BEGIN
    -- Creating a copy table
    CREATE TABLE IF NOT EXISTS `us_household_income_cleaned` (
    `row_id` int DEFAULT NULL,
    `id` int DEFAULT NULL,
    `State_Code` int DEFAULT NULL,
    `State_Name` text,
    `State_ab` text,
    `County` text,
    `City` text,
    `Place` text,
    `Type` text,
    `Primary` text,
    `Zip_Code` int DEFAULT NULL,
    `Area_Code` int DEFAULT NULL,
    `ALand` int DEFAULT NULL,
    `AWater` int DEFAULT NULL,
    `Lat` double DEFAULT NULL,
    `Lon` double DEFAULT NULL,
    `TimeStamp` TIMESTAMP DEFAULT NULL
    ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

    -- Copy data into the new table to work with
    INSERT INTO us_household_income_cleaned
    SELECT *, CURRENT_TIMESTAMP
    FROM us_household_income;

-- DATA CLEANING STEPS

    -- Getting counts for both tables.
    SELECT COUNT(id) 
    FROM us_household_income_cleaned
    ;

    SELECT COUNT(id)
    FROM us_household_income_cleaned
    ;


    -- Identifying duplicates based on ID for US Household Income table.
    SELECT id, COUNT(id)
    FROM us_household_income_cleaned
    GROUP BY id
    HAVING COUNT(id) > 1
    ;


    -- Finding the Row ID's for all of the duplicates
    SELECT *
    FROM (
        SELECT row_id, id,
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) row_num
        FROM us_household_income_cleaned
        ) duplicates
    WHERE row_num > 1
    ;


    -- Removing the duplicates using a subquery, added timestamp to keep data unique.
    DELETE FROM us_household_income_cleaned
    WHERE row_id IN 
    (
        SELECT row_id
        FROM (
            SELECT row_id, id,
            ROW_NUMBER() OVER(PARTITION BY id, `TimeStamp` ORDER BY id, `TimeStamp`) row_num
            FROM us_household_income_cleaned
            ) duplicates
    WHERE row_num > 1
    )
    ;

    -- Checking for duplicates in the Household Statistics table. There were none
    SELECT id, COUNT(id)
    FROM us_household_income_cleaned
    GROUP BY id
    HAVING COUNT(id) > 1
    ;


    -- Checking for inaccurate state names.
    SELECT DISTINCT `State_Name`, COUNT(`State_Name`)
    FROM us_household_income_cleaned
    GROUP BY `State_Name`
    ;


    -- Changing wrong spelling of Georgia on one row, found in query above.
    UPDATE us_household_income_cleaned
    SET `State_Name` = 'Georgia'
    WHERE `State_Name` = 'georia'
    ;


    -- Found many 'alabama' rows in the data, changing to capitalized.
    UPDATE us_household_income_cleaned
    SET `State_Name` = 'Alabama'
    WHERE `State_Name` = 'alabama'
    ;


    -- Checking for inaccurate State Abbreviations.
    SELECT DISTINCT `State_ab`
    FROM us_household_income_cleaned
    GROUP BY `State_ab`
    ;


    -- Checking for blank place values.
    SELECT *
    FROM us_household_income_cleaned
    WHERE `Place` = '';

-- STANDARDIZATION STEPS
    -- Setting one blank row to Autaugaville.
    UPDATE us_household_income_cleaned
    SET `Place` = 'Autaugaville'
    WHERE `County` = 'Autauga County' 
    AND `City` = 'Vinemont'
    ;

    -- Setting all texts to uppercase.
    UPDATE us_household_income_cleaned
    SET `County` = UPPER(County);

    UPDATE us_household_income_cleaned
    SET `City` = UPPER(City);

    UPDATE us_household_income_cleaned
    SET `Place` = UPPER(Place);

    UPDATE us_household_income_cleaned
    SET `State_Name` = UPPER(State_Name);


    -- Checking for Type inaccuracies.
    SELECT `Type`, COUNT(`Type`)
    FROM us_household_income_cleaned
    GROUP BY `Type`
    ;


    -- Updating wrong 'Borough' type row.
    UPDATE us_household_income_cleaned
    SET `Type` = 'Borough'
    WHERE `Type` = 'Boroughs' 
    ;

END$$
DELIMITER ;

-- Calling the Stored Procedure.
CALL copy_and_clean_data();


-- Creating the Event to schedule the cleaning every 30 days.
DROP EVENT run_data_cleaning;
CREATE EVENT run_data_cleaning
    ON SCHEDULE EVERY 30 DAY
    DO CALL copy_and_clean_data();


-- Creating a trigger to clean data when data is inserted into the table.
DELIMITER $$
CREATE Trigger transfer_clean_data
    AFTER INSERT ON us_household_income
    FOR EACH ROW
BEGIN
    CALL copy_and_clean_data();
END $$
DELIMITER ; 



-- Checking to see latest timestamp pull.
SELECT DISTINCT TIMESTAMP
FROM us_household_income_cleaned;

-- Checking old data to make sure changes have been made.
SELECT *
FROM (
    SELECT row_id, id,
    ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) row_num
    FROM us_household_income
    ) duplicates
WHERE row_num > 1
;
    
SELECT COUNT(row_id)
FROM us_household_income;

SELECT `State_Name`, COUNT(`State_Name`)
FROM us_household_income
GROUP BY `State_Name`;


-- Running the same checks on cleaned data.
SELECT *
FROM (
    SELECT row_id, id,
    ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) row_num
    FROM us_household_income_cleaned
    ) duplicates
WHERE row_num > 1
;
    
SELECT COUNT(row_id)
FROM us_household_income_cleaned;

SELECT `State_Name`, COUNT(`State_Name`)
FROM us_household_income_cleaned
GROUP BY `State_Name`;