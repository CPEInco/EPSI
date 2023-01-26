--rignore
  -- DECLARE @startDate DATETIME2(0)
  -- DECLARE @endDate DATETIME2(0)
  -- DECLARE @dayslag AS INTEGER
  -- DECLARE @hourbefore AS INTEGER

 --  SET @startDate = '2021-08-29 23:00'
--  SET @endDate = '2021-10-05 23:00'
 -- SET @dayslag = 0
 -- SET @hourbefore = 11

 --end
 SET NOCOUNT ON;

 SELECT * 
 INTO #dates
 FROM [pub].[dimDateTimeUTC] 
 WHERE DateTimeUTC >= DATEADD(DAY, -@dayslag-30, @startDate)
	AND DateTimeUTC < @endDate

SELECT CurveId,
		ForecastDateUTC,
		DATEADD(MINUTE,-DATEPART(MINUTE,ValueDateUTC), ValueDateUTC) AS ValueDateUTC,
		round(AVG([Value]),0) AS [Value],
		dateadd(hour, datediff(hour, 0, ForecastDateUTC) + 1, 0) AS ForecastJoin,
		d1.DateTimeCET AS ValueDateCET,
		d2.DateTimeCET AS ForecastDateCET,
		DATEDIFF(DAY, CAST(d1.[DateTimeCET] as DATE), d2.[DateTimeCET]) AS test
INTO #forecasts
FROM [pub].[TimeSeries1_v02] m
LEFT JOIN #dates d1 ON m.ValueDateUTC = d1.DateTimeUTC
LEFT JOIN #dates d2 ON dateadd(hour, datediff(hour, 0, m.ForecastDateUTC) + 1, 0) = d2.DateTimeUTC
WHERE ValueDateUTC >= CAST(@startDate AS datetime2(0))
	AND	ValueDateUTC < CAST(@endDate AS datetime2(0))
	AND ForecastDateUTC >= DATEADD(DAY, -31, CAST(d1.DateTimeCET AS DATE))
	AND ValueDateUTC > '1900-01-01'
	AND ForecastDateUTC < CAST(@endDate AS datetime2(0))
	AND (DATEDIFF(DAY, CAST(d1.[DateTimeCET] as DATE), d2.[DateTimeCET]) < -@dayslag
		OR (DATEDIFF(DAY, CAST(d1.[DateTimeCET] as DATE), d2.[DateTimeCET]) = -@dayslag AND DATEPART(HOUR, d2.[DateTimeCET]) <= @hourbefore))
	--AND CurveID IN(1000300127,1000300128,1000300129,1000300130,1000300131)
	AND CurveID IN@inputids
GROUP BY DATEADD(MINUTE,-DATEPART(MINUTE,ValueDateUTC), ValueDateUTC), ForecastDateUTC, CurveId, d1.DateTimeCET,d2.DateTimeCET

--SELECT * FROM #forecasts ORDER BY ValueDateCET

SELECT CurveID
		,ForecastDate
		,[ValueDate]
		,[Value]
FROM(
SELECT [CurveID]
      ,CONVERT(VARCHAR, ForecastDateUTC,120) AS ForecastDate
	  ,f.ForecastJoin
	  ,ForecastDateCET
      ,CONVERT(VARCHAR, ValueDateUTC,120) AS [ValueDate]
	  ,ValueDateCET
      ,[Value]
	  ,max(ForecastDateUTC) OVER(PARTITION BY ValueDateUTC, CurveID) AS maxForecastDate
  FROM #forecasts as f
   WHERE ValueDateUTC >= CAST(@startDate AS datetime2(0))
	AND	ValueDateUTC < CAST(@endDate AS datetime2(0))
	--AND CurveID IN(1000080082, 1000070014)
	AND CurveID IN@inputids
	) AS T1
--WHERE ForecastDate = maxForecastDate
--ORDER BY CurveId, ValueDate, ForecastDate

DROP TABLE #dates
DROP TABLE #forecasts

-- <QueryInfo> QueryName: X:\DA\Tools\EPSI\upload\SQL\prepare_timeseries1_v02_forward_historic.sql, CriticalQuery: True </QueryInfo>