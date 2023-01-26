  --rignore
   DECLARE @startDate DATETIME
   DECLARE @endDate DATETIME

   SET @startDate = '2022-05-10 23:00'
   SET @endDate = '2022-05-12 01:00'
 --end

SELECT Product
		,Area
		,FirstSequenceItemName
		,PeriodStart
		,PeriodEnd
		,SUM(Volume*Price)/SUM(Volume) AS AveragePrice
		,SUM(Volume) AS TotalVolume
		,DATEPART(WEEKDAY, PeriodStart) AS wewe
		--,dd
		--,pd = DATEDIFF(DAY, PeriodStart, PeriodEnd)
		FROM(
SELECT 
 CASE WHEN InstID IN(10001129, 10641712) THEN 'Germany Peaks'
	 WHEN InstID IN(10001111, 10001077) THEN 'France Peaks'
	 WHEN InstID IN(10001061) THEN 'Netherlands Peaks'
	 WHEN InstID IN(10000051) THEN 'UK Peaks'
	 WHEN InstID IN(10001141,10011038) THEN 'Hungary Peaks'
	 END AS Product
,CASE WHEN InstID IN(10001129, 10641712) THEN 'DE'
	 WHEN InstID IN(10001111, 10001077) THEN 'FR'
	 WHEN InstID IN(10001061)  THEN 'NL'
	 WHEN InstID IN(10000051) THEN 'GB'
	 WHEN InstID IN(10001141,10011038) THEN 'HU'
	 END AS Area
,abs(round(PERCENTILE_CONT(0.5) 
    WITHIN GROUP (ORDER BY PRICE) OVER ( 
     PARTITION BY InstID, PeriodStart) - Price,0)) AS MEDIANCONT 
,InstID
,InstName
,FirstSequenceItemName
,FirstSequenceItemID
,Volume
,Price
,si.PeriodStart
,si.PeriodEnd
,DATEDIFF(DAY,[Datetime], PeriodStart) AS dd
  FROM [pub].[Trayport_Trades_Public] trades
  LEFT JOIN [pub].[Trayport_SequenceItems] si ON trades.FirstSequenceItemID = si.ItemID AND trades.FirstSequenceID = si.SeqID
  LEFT JOIN [dbo].[V_DSTHourLookup] dd ON dateadd(hour,datediff(hour,0,[DateTime]),0) = dd.UTCHour
  WHERE [Datetime]>= DATEADD(DAY,-1,@startDate)
	AND [Datetime] < DATEADD(DAY,-1,@endDate)
	AND ((DATEDIFF(DAY,[Datetime], PeriodStart) = 1 AND InstID NOT IN(10000051)) OR (DATEDIFF(DAY,[Datetime], PeriodStart) = 0 AND InstID IN(10000051)))
		AND DATEPART(HOUR,CETCESTHour) >= 9
		AND DATEPART(HOUR,CETCESTHour) < 11
	AND DATEDIFF(DAY, PeriodStart, PeriodEnd) = 1
	AND InstID IN(10001129, 10641712, 10001111,10001077,10001061,10000051,10001141,10011038)
	--AND ExecutionWorkflow
	--AND FirstSequenceItemName = 'Thu 18/03/21'
	AND PeriodStart >= @startDate
	AND PeriodEnd < @endDate
	AND RouteName = 'House'
	AND [Action] IN('Insert', 'Query')
	AND ((DATEPART(WEEKDAY, si.PeriodStart) IN(3,4,5,6,7) AND  InstID NOT IN(10000051)) OR (DATEPART(WEEKDAY, si.PeriodStart) IN(2,3,4,5,6) AND InstID IN(10000051)))) AS t1
	WHERE MEDIANCONT < 50
	GROUP BY Product, FirstSequenceItemName, FirstSequenceItemID,t1.PeriodStart, t1.PeriodEnd, dd, Area
	HAVING SUM(Volume) >= 100
	ORDER BY Product, PeriodStart

-- <QueryInfo> QueryName: X:\DA\Tools\EPSI\sql\PeakVWAP.sql, CriticalQuery: True </QueryInfo>