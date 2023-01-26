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

     SELECT @CurveId, @ValueDate, @ForecastDate, [Value] FROM(
		SELECT @CurveId, @ValueDate, [Value],
			ROW_NUMBER() OVER(PARTITION BY @CurveId, @ValueDate ORDER BY @ForecastDate DESC) as rn, @ForecastDate
		FROM #data) as rt
		WHERE rn = 2
    
       DROP TABLE #data