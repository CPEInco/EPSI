 -- --rignore
 DECLARE @startDate DATETIME2(0)
 DECLARE @endDate DATETIME2(0)
 SET @startDate = CONVERT([DATETIME],((cast(dateadd(day,+1,dateadd(day,datediff(day,0,SYSDATETIME()),0)) as datetime)  AT TIME ZONE 'Central European Standard Time' ) AT TIME ZONE 'UTC'))
 SET @endDate = dateadd(day,+1,@startdate)

-- print DATEADD(DAY, -2, @startDate)
  SET NOCOUNT ON;
  
 SELECT UTCHour AS DateTimeUTC
		,CETCESTHour AS [DateTimeCET]
 INTO #dates
 FROM [dbo].[V_DSTHourLookup]
 WHERE CETCESTHour >= DATEADD(DAY, -60, @startDate)
	AND CETCESTHour < @endDate
  
SELECT CurveId,
		ForecastDateUTC,
		DATEADD(MINUTE,-DATEPART(MINUTE,ValueDateUTC), ValueDateUTC) AS ValueDateUTC,
		[Value],
		dateadd(hour, datediff(hour, 0, ForecastDateUTC) + 1, 0) AS ForecastJoin,
		d1.DateTimeCET AS ValueDateCET,
		dateadd(minute, DATEDIFF(MINUTE,dateadd(hour, datediff(hour, 0, ForecastDateUTC) + 1, 0),ForecastDateUTC), d2.DateTimeCET) AS ForecastDateCET,
		DATEDIFF(DAY, CAST(d1.[DateTimeCET] as DATE), d2.[DateTimeCET]) AS test
		--RowInsertedOrModifiedDateUTC
INTO #forecasts
FROM [pub].[TimeSeries1_v02] m
LEFT JOIN #dates d1 ON m.ValueDateUTC = d1.DateTimeUTC
LEFT JOIN #dates d2 ON dateadd(hour, datediff(hour, 0, m.ForecastDateUTC) + 1, 0) = d2.DateTimeUTC
WHERE CurveId IN(1000341698, 1000341699, 1000341700, 1000341701, 1000341702, 1000341703, 1000341704, 1000341705, 1000341706, 1000341707, 1000341708, 1000341709, 1000342870, 1000341711, 1000341712,1000342869,1000342868,1000343027,1000343031,1000343033,1000343037)
	AND ScenarioId = 99
	AND d1.[DateTimeCET] >= @startDate
	AND d1.[DateTimeCET] < @endDate
	AND [ValueDateUTC] > '1900-01-01'
	AND ForecastDateUTC > '1900-01-01'
	AND (DATEDIFF(DAY, CAST(d1.[DateTimeCET] as DATE), d2.[DateTimeCET]) < -1
		OR (DATEDIFF(DAY, CAST(d1.[DateTimeCET] as DATE), d2.[DateTimeCET]) = -1 AND DATEPART(HOUR, d2.[DateTimeCET]) <= 12))
OPTION(recompile, MAXDOP 0)
--GROUP BY DATEADD(MINUTE,-DATEPART(MINUTE,ValueDateUTC), ValueDateUTC), ForecastDateUTC, CurveId, d1.DateTimeCET,d2.DateTimeCET

--SELECT * FROM #forecasts




SELECT CASE WHEN CurveID = 1000341698 THEN 'DE'
			WHEN CurveID = 1000341699 THEN 'FR'
			WHEN CurveID = 1000341700 THEN 'AT'
			WHEN CurveID = 1000341701 THEN 'BE'
			WHEN CurveID = 1000341702 THEN 'NL'
			WHEN CurveID = 1000341703 THEN 'HU'
			WHEN CurveID = 1000341704 THEN 'CZ'
			WHEN CurveID = 1000341705 THEN 'ES'
			WHEN CurveID = 1000341706 THEN 'CH'
			WHEN CurveID = 1000341707 THEN 'UK'
			WHEN CurveID = 1000341708 THEN 'DK1'
			WHEN CurveID = 1000341709 THEN 'DK2'
			WHEN CurveID = 1000342870 THEN 'SE4'
			WHEN CurveID = 1000342869 THEN 'SE3'
			WHEN CurveID = 1000342868 THEN 'FI'
			WHEN CurveID = 1000341711 THEN 'SI'
			WHEN CurveID = 1000341712 THEN 'IT Nord'
			WHEN CurveID = 1000343027 THEN 'HR'
			WHEN CurveID = 1000343031 THEN 'PL'
			WHEN CurveID = 1000343033 THEN 'RO'
			WHEN CurveID = 1000343037 THEN 'SK'
		END AS Area
		,ForecastDateCET
		,ValueDateCET
		,[Value]
FROM(
SELECT [CurveID]
      ,CONVERT(VARCHAR, ForecastDateUTC,120) AS ForecastDate
	  ,f.ForecastJoin
	  ,max(ForecastDateCET) OVER() AS ForecastDateCET
      ,CONVERT(VARCHAR, ValueDateUTC,120) AS [ValueDate]
	  ,ValueDateCET
      ,[Value]
	  ,max(CONVERT(VARCHAR, ForecastDateUTC,120)) OVER(PARTITION BY ValueDateUTC, CurveID) AS maxForecastDate
  FROM #forecasts as f
   WHERE ValueDateCET >= @startDate
	AND	ValueDateCET < @endDate
	) AS T1
WHERE ForecastDate = maxForecastDate

--ORDER BY CurveId, ValueDate, ForecastDate

DROP TABLE #dates
DROP TABLE #forecasts

-- <QueryInfo> QueryName: X:\DA\Tools\EPSI\sql\DApriceforecast.sql, CriticalQuery: True </QueryInfo>