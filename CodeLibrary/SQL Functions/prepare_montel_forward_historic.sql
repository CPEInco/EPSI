--rignore
   DECLARE @startDate DATETIME2(0)
   DECLARE @endDate DATETIME2(0)
   DECLARE @dayslag AS INTEGER
   DECLARE @hourbefore AS INTEGER

   SET @startDate = '2020-12-10 23:00'
   SET @endDate = '2020-12-20 23:00'
   SET @dayslag = 1
   SET @hourbefore = 11

 --end
 SET NOCOUNT ON;

 SELECT * 
 INTO #dates
 FROM [pub].[dimDateTimeUTC] 
 WHERE DateTimeUTC >= DATEADD(DAY, -@dayslag-30, @startDate)
	AND DateTimeUTC < @endDate

SELECT CurveId,
		IssueDateTimeUTC,
		ValueDateTimeUTC,
		[Value],
		dateadd(hour, datediff(hour, 0, IssueDateTimeUTC) + 1, 0) AS ForecastJoin,
		d1.DateTimeCET AS ValueDateCET,
		d2.DateTimeCET AS ForecastDateCET,
		DATEDIFF(DAY, CAST(d1.[DateTimeCET] as DATE), d2.[DateTimeCET]) AS test
INTO #forecasts
FROM [raw].Montel_Fundamental_TimeSeries m
LEFT JOIN #dates d1 ON m.ValueDateTimeUTC = d1.DateTimeUTC
LEFT JOIN #dates d2 ON dateadd(hour, datediff(hour, 0, m.IssueDateTimeUTC) + 1, 0) = d2.DateTimeUTC
WHERE ValueDateTimeUTC >= CAST(@startDate AS datetime2(0))
	AND	ValueDateTimeUTC < CAST(@endDate AS datetime2(0))
	AND IssueDateTimeUTC >= DATEADD(DAY, -31, CAST(d1.DateTimeCET AS DATE))
	AND ValueDateTimeUTC > '1900-01-01'
	AND IssueDateTimeUTC < CAST(@endDate AS datetime2(0))
	AND (DATEDIFF(DAY, CAST(d1.[DateTimeCET] as DATE), d2.[DateTimeCET]) < -@dayslag
		OR (DATEDIFF(DAY, CAST(d1.[DateTimeCET] as DATE), d2.[DateTimeCET]) = -@dayslag AND DATEPART(HOUR, d2.[DateTimeCET]) <= @hourbefore))
	--AND CurveID IN(1000330007)
	AND CurveID IN@inputids

--SELECT * FROM #forecasts ORDER BY ValueDateCET

SELECT CurveID
		,ForecastDate
		,[ValueDate]
		,[Value]
FROM(
SELECT [CurveID]
      ,CONVERT(VARCHAR, IssueDateTimeUTC,120) AS ForecastDate
	  ,f.ForecastJoin
	  ,ForecastDateCET
      ,CONVERT(VARCHAR, ValueDateTimeUTC,120) AS [ValueDate]
	  ,ValueDateCET
      ,[Value]
	  ,max(IssueDateTimeUTC) OVER(PARTITION BY ValueDateTimeUTC, CurveID) AS maxForecastDate
  FROM #forecasts as f
   WHERE ValueDateTimeUTC >= CAST(@startDate AS datetime2(0))
	AND	ValueDateTimeUTC < CAST(@endDate AS datetime2(0))
	--AND CurveID IN(1000080082, 1000070014)
	--AND CurveID IN@inputids
	) AS T1
WHERE ForecastDate = maxForecastDate
--ORDER BY CurveId, ValueDate, ForecastDate

DROP TABLE #dates
DROP TABLE #forecasts

-- <QueryInfo> QueryName: X:\DA\Tools\EPSI\upload\SQL\prepare_montel_forward_historic.sql, CriticalQuery: True </QueryInfo>