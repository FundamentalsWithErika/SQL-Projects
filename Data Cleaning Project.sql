-- Data Cleaning

SELECT *
FROM layoffs;
-- Typical steps for data cleaning
-- 0. Create a duplicate of the raw table and work in the duplicate only
-- 1. Remove duplicates
-- 2. Standardize the Data
-- 3. Null values or blank values
-- 4. Remove unnecessary columns
#Step 0 begins
CREATE TABLE layoffs_staging
LIKE layoffs;
#Remember the above step only creates the columns of the table; 
#You have to insert the data after this

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;
##Step 0 ends

#Step 1 begins
#We want to assign a row number to each row, partition by every column to find out if 2 rows have the same information
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`)
AS row_num
FROM layoffs_staging;
# We use a Common Table Expression to highlight which row number is greater than 2 
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`)
AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

Select *
FROM layoffs_staging
WHERE company = 'Casper';

#Upon checking, we realize that we have to partition by every column to get the duplicates;
#we will rewrite the code above and include every column
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

#Checking to see if we can delete duplicates using delete function
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE FROM duplicate_cte
WHERE row_num > 1;

#We cannot update/delete rows in a CTE. We will have to create a separate table and assign them a row number

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE FROM layoffs_staging2
WHERE row_num > 1;

-- Standardizing data
SELECT company, trim(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';
#Do the same for every column where you suspect there could be issue, Location, Country, etc
select distinct country
from layoffs_staging2
order by 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';
# We noticed that some rows in the industry column are blank and should not be

SELECT * 
FROM layoffs_staging2 AS emp_table
JOIN layoffs_staging2 AS full_table
ON emp_table.company = full_table.company
	WHERE (emp_table.industry IS NULL
    OR emp_table.industry = '')
    AND full_table.industry IS NOT NULL
    ;
#The below is not working for some reason; we might have to change blank into null
UPDATE layoffs_staging2 AS emp_table
JOIN layoffs_staging2 AS full_table
	ON emp_table.company = full_table.company
SET emp_table.industry = full_table.industry
	WHERE (emp_table.industry IS NULL
    OR emp_table.industry = '')
    AND full_table.industry IS NOT NULL;
    
SELECT emp_table.industry, full_table.industry
FROM layoffs_staging2 AS emp_table
JOIN layoffs_staging2 AS full_table
	ON emp_table.company = full_table.company
    WHERE emp_table.industry = ''
    AND full_table.industry IS NOT NULL;
    
SELECT *
FROM layoffs_staging2 AS emp_table
JOIN layoffs_staging2 AS full_table
ON emp_table.industry = full_table.industry
	WHERE (emp_table.industry IS NULL
    OR emp_table.industry = '')
    AND full_table.industry IS NOT NULL;
    
#We are not able to update/replace the blank values with the corresponding value we want for each company
#Instead we will update the blank values to null values first, then we will see whether we can update the whole table

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2 AS emp_table
JOIN layoffs_staging2 AS full_table
	ON emp_table.company = full_table.company
    WHERE emp_table.industry IS NULL;
    
UPDATE layoffs_staging2 AS emp_table
JOIN layoffs_staging2 AS full_table
	ON emp_table.company = full_table.company
    SET emp_table.industry = full_table.industry
    WHERE emp_table.industry IS NULL
    AND full_table.industry IS NOT NULL;

    SELECT *
    FROM layoffs_staging2
    ORDER BY 1;
    
    SELECT *
    FROM layoffs_staging2
    WHERE total_laid_off IS NULL
    AND percentage_laid_off IS NULL;
    
    DELETE FROM layoffs_staging2
    WHERE total_laid_off IS NULL
    AND percentage_laid_off IS NULL;
    
    SELECT *
    FROM layoffs_staging2
    ORDER BY 1;
    
    ALTER TABLE layoffs_staging2
    DROP column row_num;