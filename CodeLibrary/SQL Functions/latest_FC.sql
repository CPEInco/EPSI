--rignore
   DECLARE @start DATETIME = GETDATE() - 3
   DECLARE @end DATETIME = @start + 3
   DECLARE @hours_add INT = 0
   DECLARE @startF DATETIME = GETDATE() - 2
   DECLARE @endF DATETIME = @start + 4
   --end

   SET NOCOUNT ON
   
   SELECT * INTO #data FROM [raw].[@table]
     WHERE ((@CurveId IN @ids) AND (@ValueDate >= @start) AND (@ValueDate <= @end) AND (@ForecastDate <= DATEADD("HOUR", @hours_add, @ValueDate)) AND (@ForecastDate > @startF) AND (@ForecastDate < @endF))

   SELECT rt.@CurveId, rt.@ValueDate, rt.@ForecastDate, rt.[Value]
   FROM (SELECT @CurveId, @ValueDate, MAX(@ForecastDate) as maxForecast
         FROM #data
         GROUP BY @CurveId, @ValueDate) rt1
         INNER JOIN ( SELECT * FROM #data) as rt
         ON rt1.@CurveId = rt.@CurveId AND rt1.@ValueDate = rt.@ValueDate AND rt1.maxForecast = rt.@ForecastDate
   
   DROP TABLE #data