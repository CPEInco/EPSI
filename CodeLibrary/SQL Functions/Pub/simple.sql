    --rignore
	DECLARE @start DATETIME = GETDATE() - 1
	DECLARE @end DATETIME = @start + 3
	DECLARE @startF DATETIME = GETDATE() - 2
	DECLARE @endF DATETIME = @start + 4
	--end

	SET NOCOUNT ON

	SELECT @CurveId, @ValueDate, @ForecastDate, [Value]
	FROM [InCo].[pub].[@table]
	WHERE ((@CurveId IN @ids) AND (@ValueDate >= @start) AND (@ValueDate <= @end) AND (@ForecastDate > @startF OR @ForecastDate = '1900-01-01 00:00:00.000' OR @ForecastDate = '2000-01-01 00:00:00.000') AND (@ForecastDate < @endF))