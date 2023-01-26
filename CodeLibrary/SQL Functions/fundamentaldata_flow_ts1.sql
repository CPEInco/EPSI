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
 WHERE CETCESTHour >= DATEADD(DAY, -2, @startDate)
	AND CETCESTHour < @endDate

/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [CurveId]
      ,DATEADD(MINUTE, -DATEPART(MINUTE,[ValueDateUTC]),[ValueDateUTC]) AS ValueDateUTC
      ,AVG([Value]) AS [Value]
FROM(
SELECT CurveId,
		ValueDateUTC,
		maxforecastdate = MAX(ForecastDateUTC) OVER(PARTITION BY ValueDateUTC, CurveID),
		ForecastDateUTC,
		Value
  FROM [pub].[TimeSeries1_v02] AS m
   LEFT JOIN #dates d1 ON dateadd(hour, datediff(hour, 0, m.ValueDateUTC) + 1, 0) = d1.DateTimeUTC
  LEFT JOIN #dates d2 ON dateadd(hour, datediff(hour, 0, m.ForecastDateUTC) + 1, 0) = d2.DateTimeUTC
  WHERE CurveId IN(910002592,910002588,910002590,910002598)
  AND ValueDateUTC >= @startDate
  AND ValueDateUTC < @endDate
  AND ForecastDateUTC <= @endDate
  AND ForecastDateUTC <= @forecastDate
  AND ((DATEDIFF(DAY, d2.DateTimeCET, d1.DateTimeCET) < 60
  AND [ForecastDateUTC] >= DATEADD(DAY, -2, @startDate) 
  AND DATEDIFF(DAY, d2.DateTimeCET, d1.DateTimeCET) > 0
  AND DATEPART(HOUR, d2.DateTimeCET) < 12)
  OR (DATEDIFF(DAY, d2.DateTimeCET, d1.DateTimeCET) > 0
		AND[ForecastDateUTC] >= DATEADD(DAY, -2, @startDate)
		AND ValueDateUTC > @forecastDate))
  ) T1
  WHERE ForecastDateUTC = maxforecastdate
  GROUP BY DATEADD(MINUTE, -DATEPART(MINUTE,[ValueDateUTC]),[ValueDateUTC]), CurveId
  ORDER BY ValueDateUTC
  OPTION(RECOMPILE, MAXDOP 0) 
  
  DROP TABLE #dates