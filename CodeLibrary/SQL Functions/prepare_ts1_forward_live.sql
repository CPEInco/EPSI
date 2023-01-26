--rignore
   --DECLARE @startDate DATETIME
   --DECLARE @endDate DATETIME
   --DECLARE @inputids nvarchar(255)
   --SET @startDate = '2022-04-01 22:00'
   --SET @endDate = '2022-12-31 23:00'
   --SET @inputids='910003152'
--end
 SELECT *
FROM(
SELECT [CurveID]
      ,CONVERT(VARCHAR, [ForecastDateUTC],120) AS ForecastDate
      ,CONVERT(VARCHAR, [ValueDateUTC],120) AS [ValueDate]
      ,[Value]
	  ,CONVERT(VARCHAR,max([ForecastDateUTC]) OVER(PARTITION BY [ValueDateUTC], CurveID),120) AS maxForecastDate
  FROM [InCo].[pub].[TimeSeries1_v02]
  WHERE [ValueDateUTC] >= @startDate
	AND [ValueDateUTC] <= @endDate
	AND [ForecastDateUTC] >= DATEADD(DAY, -5, @startDate)
	--AND CurveId = @inputids
	AND CurveID IN@inputids
	AND ScenarioID = 0
	) T1
WHERE ForecastDate = maxForecastDate

-- <QueryInfo> QueryName: X:\DA\Tools\EPSI\upload\SQL\prepare_ts1_forward_live2.sql, CriticalQuery: True </QueryInfo>