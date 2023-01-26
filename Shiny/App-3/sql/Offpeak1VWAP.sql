  --rignore
   DECLARE @startDate DATETIME
   DECLARE @endDate DATETIME

   SET @startDate = '2021-10-31 23:00'
   SET @endDate = '2022-06-19 01:00'
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
 CASE WHEN InstID IN(10001123) THEN 'Germany 0-6'
	 WHEN InstID IN(10000071) THEN 'UK 1+2'
	 END AS Product
,CASE WHEN InstID IN(10001123) THEN 'DE'
	 WHEN InstID IN(10000071) THEN 'GB'
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
	AND ((DATEDIFF(DAY,[Datetime], PeriodStart) = 1 AND InstID NOT IN(10000071)) OR (DATEDIFF(DAY,[Datetime], PeriodStart) = 0 AND InstID IN(10000071)))
	AND DATEDIFF(DAY, PeriodStart, PeriodEnd) = 1
	AND DATEPART(HOUR,CETCESTHour) >= 9
	AND DATEPART(HOUR,CETCESTHour) < 11
	AND InstID IN(
				  10001123,
				  10000071)
	--AND ExecutionWorkflow
	--AND FirstSequenceItemName = 'Thu 18/03/21'
	AND PeriodStart >= @startDate
	AND PeriodEnd < @endDate
	AND RouteName = 'House'
	AND [Action] IN('Insert', 'Query')
	AND ((DATEPART(WEEKDAY, si.PeriodStart) IN(3,4,5,6,7) AND  InstID NOT IN(10000050, 10000085, 10000095, 10640415, 10640419)) OR (DATEPART(WEEKDAY, si.PeriodStart) IN(2,3,4,5,6) AND InstID IN(10000050, 10000085, 10000095, 10640415, 10640419)))) AS t1
	--WHERE MEDIANCONT < 25
	GROUP BY Product, FirstSequenceItemName, FirstSequenceItemID,t1.PeriodStart, t1.PeriodEnd,dd, Area
	HAVING SUM(Volume) >= 100
	ORDER BY Product, PeriodStart

-- <QueryInfo> QueryName: X:\DA\Tools\EPSI\sql\Offpeak1VWAP.sql, CriticalQuery: True </QueryInfo>