--rignore
   --DECLARE @startDate DATETIME
   --DECLARE @endDate DATETIME
   --DECLARE @inputids nvarchar(255)
   --SET @startDate = '2021-08-09 22:00'
   --SET @endDate = '2020-12-31 23:00'
   --SET @inputids=1000190019
 --end
 SELECT *
FROM(
SELECT [CurveID]
      ,CONVERT(VARCHAR, [ForecastDateTimeUTC],120) AS ForecastDate
      ,CONVERT(VARCHAR, [ValueDateTimeUTC],120) AS [ValueDate]
      ,[Value]
	  ,max([ForecastDateTimeUTC]) OVER(PARTITION BY [ValueDateTimeUTC], CurveID) AS maxForecastDate
  FROM [InCo].[raw].[Meteologica_TimeSeries]
  WHERE [ValueDateTimeUTC] >= @startDate
	AND [ForecastDateTimeUTC] >= DATEADD(DAY, -2, @startDate)
	AND CurveID IN @inputids ) T1
WHERE ForecastDate = maxForecastDate

-- <QueryInfo> QueryName: X:\DA\Tools\EPSI\upload\SQL\prepare_metologicadata_forward_live.sql, CriticalQuery: True </QueryInfo>