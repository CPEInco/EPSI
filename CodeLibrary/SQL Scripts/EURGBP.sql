 -- --rignore
 SET NOCOUNT ON;
 
 --DECLARE @startDate DATETIME2(0) = '2021-11-12 00:00:00.000'
 --DECLARE @endDate DATETIME2(0)   = '2022-11-22 00:00:00.000'
 
 DECLARE @EURGBP AS DECIMAL(10,4); 

 SELECT 
[Date], [Value]
FROM pub.dimCurrency_Conversion_EUR
WHERE CurrencyFrom = 'GBP'
AND date <= @endDate
AND Date >= @startDate

