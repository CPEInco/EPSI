  --rignore
	DECLARE @startDate DATETIME
	DECLARE @endDate DATETIME
	DECLARE @forecastDate DATETIME

	SET @startDate = '2020-11-30 23:00:00'
	SET @endDate = '2021-01-31 23:00:00'
	SET @forecastDate = '2021-01-09 08:00:00'
 --end

 SET NOCOUNT ON

  SELECT UTCHour AS DateTimeUTC
		,CETCESTHour AS [DateTimeCET]
 INTO #dates
 FROM [dbo].[V_DSTHourLookup]
 WHERE CETCESTHour >= DATEADD(DAY, -60, @startDate)
	AND CETCESTHour < @endDate

SELECT * FROM(
SELECT [CurveId]
      ,[ForecastDateTimeUTC]
	  ,d2.DateTimeCET AS ForecastDateTimeCET
      ,[ValueDateTimeUTC]
	  ,d1.DateTimeCET AS ValueDateTimeCET
      ,[Value]
      ,[InsertedDateTimeUTC]
      ,[ModifiedDateTimeUTC]
      ,[Country]
      ,[CountryCode]
	  ,maxforecastdate = MAX(ForecastDateTimeUTC) OVER(PARTITION BY ValueDateTimeUTC, CurveID)
  FROM [raw].[Meteologica_TimeSeries] as m
  LEFT JOIN #dates d1 ON m.ValueDateTimeUTC = d1.DateTimeUTC
  LEFT JOIN #dates d2 ON dateadd(hour, datediff(hour, 0, m.ForecastDateTimeUTC) + 1, 0) = d2.DateTimeUTC
  WHERE CurveId IN(1000110122,1000110114, 1000110073,1000110065,1000110118,1000110134,10001100691000110085)
  AND ValueDateTimeUTC >= @startDate
  AND ValueDateTimeUTC < @endDate
  AND ForecastDateTimeUTC <= @endDate
  AND ForecastDateTimeUTC <= @forecastDate
  AND ForecastDateTimeUTC >= '1900-01-01 00:00:00'
  AND ((DATEDIFF(DAY, d2.DateTimeCET, d1.DateTimeCET) < 2
  AND [ForecastDateTimeUTC] >= DATEADD(DAY, -2, @startDate) 
  AND DATEDIFF(DAY, d2.DateTimeCET, d1.DateTimeCET) > 0
  AND DATEPART(HOUR, d2.DateTimeCET) < 12)
  OR (DATEDIFF(DAY, d2.DateTimeCET, d1.DateTimeCET) > 0
		AND[ForecastDateTimeUTC] >= DATEADD(DAY, -2, @startDate)
		AND ValueDateTimeUTC > @forecastDate))

  --AND CountryCode IN('DK', 'AT', 'DE', 'FR', 'NO', 'SE', 'HU', 'SK', 'HR', 'RS', 'RO', 'PL', 'SK', 'CZ', 'ES', 'PT', 'BE', 'NL', 'GB')
  ) AS t1
  WHERE ForecastDateTimeUTC = maxforecastdate
   OPTION(RECOMPILE, MAXDOP 0) 

  DROP TABLE #dates

-- <QueryInfo> QueryName: X:\DA\Tools\EPSI\FlowForecast\SQL\data_meteo.sql, CriticalQuery: True </QueryInfo>