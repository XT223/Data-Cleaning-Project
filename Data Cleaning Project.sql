-- DATA CLEANING

SELECT *
FROM layoffs;

-- 1. Removing duplicates
-- 2. Standardisation
-- 3. Null/blank values
-- 4. Removing unnecessary columns

-- Create a new table for staging (this ensures that in case any mistakes are made during data manipulation, the original raw database is still intact) 
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging 
SELECT *
FROM layoffs;    # this inserts data from layoffs into layoffs_staging

-- Removal of duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_CTE as 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_CTE
WHERE row_num > 1; #since those with row number greater than 1 are duplicates (as they're not unique)

SELECT *
FROM layoffs_staging
WHERE company = "Cazoo"; # to check if query has worked well and show proof of duplicate

# Can't directly delete from CTE in MySQL

CREATE TABLE `layoffs_staging_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging_2;
INSERT INTO layoffs_staging_2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

set SQL_SAFE_UPDATES = 0; # to allow the delete command to run in MySQL

DELETE
FROM layoffs_staging_2
WHERE row_num >= 2;

SELECT *
FROM layoffs_staging_2;

-- Standardisation (making data look similar across the database)

SELECT company, TRIM(company) # Trim to remove white space
FROM layoffs_staging_2;

UPDATE layoffs_staging_2
SET company = TRIM(company);

SELECT DISTINCT industry  # ensures that only unique values are returned
FROM layoffs_staging_2
ORDER BY 1; 

SELECT *
FROM layoffs_staging_2
WHERE industry like "crypto%";

UPDATE layoffs_staging_2
SET industry = "Cryptocurrency"
WHERE industry like "crypto%";

SELECT DISTINCT country, TRIM(TRAILING "." FROM country)
FROM layoffs_staging_2
ORDER BY 1; 

UPDATE layoffs_staging_2
SET country =  TRIM(TRAILING "." FROM country)
WHERE country like "United States";

SELECT distinct country from layoffs_staging_2;

select `date`,
str_to_date (`date`, "%m/%d/%Y") # to make dates have one format
from layoffs_staging_2;

UPDATE layoffs_staging_2
SET `date` = str_to_date (`date`, "%m/%d/%Y");

ALTER TABLE layoffs_staging_2 
MODIFY COLUMN `date` DATE;  # to convert the dates from text datatype to date datatype

-- NULL/BLANK VALUES 
SELECT * 
from layoffs_staging_2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

update layoffs_staging_2
set industry = NULL
where industry = "";

SELECT *
FROM layoffs_staging_2
WHERE industry IS NULL    # Use "is null" not "= null" for where statements
or industry = "";

SELECT *
FROM layoffs_staging_2
WHERE company like "Ball%";

SELECT t1.industry, t2.industry
FROM layoffs_staging_2 as t1
JOIN layoffs_staging_2 as t2
	ON t1.company = t2.company
	AND t1.location = t2.location
WHERE (t1.industry is null or t1.industry = "")
and t2.industry is not null;

UPDATE layoffs_staging_2 as t1
JOIN layoffs_staging_2 as t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry is null
and t2.industry is not null;

SELECT *
FROM layoffs_staging_2
where total_laid_off is null
and percentage_laid_off is null;

DELETE
FROM layoffs_staging_2
where total_laid_off is null
and percentage_laid_off is null;

-- GETTING RID OF COLUMNS
ALTER TABLE layoffs_staging_2
DROP COLUMN row_num; # to get rid of a column