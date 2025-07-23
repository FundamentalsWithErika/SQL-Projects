-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

SELECT max(total_laid_off), max(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT min(`date`), max(`date`)
FROM layoffs_staging2;

SELECT industry, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT year(`date`), company, sum(total_laid_off)
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
GROUP BY year(`date`), company
ORDER BY 1 DESC,3 DESC;

SELECT substring(`date`, 1, 7) AS `MONTH`, sum(total_laid_off)
FROM layoffs_staging2
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

#We want to get the rolling total of these laid off month after month
WITH Rolling_Total AS
(
SELECT substring(`date`, 1, 7) AS `MONTH`, sum(total_laid_off) AS Total_off
FROM layoffs_staging2
WHERE substring(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT *, sum(Total_off) OVER(ORDER BY `MONTH`) AS Rolling
from Rolling_Total
GROUP BY `MONTH`;

#Ranking companies per total number of layoffs per year
-- Step 1 - We first group layoffs per company and per year
-- Step 2 - We put it in a cte and we rank the resulting table
-- Step 3 - We put that 2nd query into a CTE then we query for the top 5 companies
SELECT company, year(`date`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company, year(`date`)
ORDER BY 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, year(`date`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company, year(`date`)
)
select *, dense_rank() OVER (partition by years order by total_laid_off DESC) AS `Rank`
FROM Company_Year
WHERE years IS NOT NULL;
#we use dense_rank in the above to rank the companies in descending order of number of layoffs
#we partition by years to restart the ranking every year

-- We want to display the top 5 companies who laid off the most employees per year
#We will use multiple CTEs to accomplish that
WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, year(`date`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company, year(`date`)
),
Company_Rank AS
(
select *, dense_rank() OVER (partition by years order by total_laid_off DESC) AS `Rank`
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Rank
WHERE `Rank` < 6;

-- Which company raised the most money
