 --rignore
	DECLARE @inputdate_1 DATETIME2(0)
	DECLARE @inputdate_2 DATETIME2(0)

	SET @inputdate_1 = '2022-04-25'
	SET @inputdate_2 = '2022-05-03'
 
--end

SELECT [Date], GBPEUR = [Value] FROM pub.dimCurrency_Conversion_EUR
WHERE CurrencyFrom = 'GBP'
	AND [date] >= @inputdate_1
	AND [date] < @inputdate_2