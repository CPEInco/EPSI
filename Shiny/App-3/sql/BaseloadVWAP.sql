  --rignore
   DECLARE @startDate DATETIME
   DECLARE @endDate DATETIME

   SET @startDate = '2022-03-31 23:00'
   SET @endDate = '2022-06-30 03:00'
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
 CASE WHEN InstID IN(10001126, 10641710) THEN 'Germany Baseload'
	 WHEN InstID IN(10001109, 10001075) THEN 'France Baseload'
	 WHEN InstID IN(10001006, 10641714) THEN 'Austria Baseload'
	 WHEN InstID IN(10001058, 10011910) THEN 'Netherlands Baseload'
	 WHEN InstID IN(10001016, 10011906, 10012566) THEN 'Belgium Baseload'
	 WHEN InstID IN(10001183, 10012528, 10644580, 10011175, 10001855) THEN 'Spain Baseload'
	 WHEN InstID IN(10000050, 10000085, 10000095, 10640415, 10640419) THEN 'UK Baseload'
	 WHEN InstID IN(10001137, 10011036, 10001732) THEN 'Hungary Baseload'
	 WHEN InstID IN(10001027, 10001650, 10001660, 10001996) THEN 'Czech Baseload'
	 WHEN InstID IN(10001197, 10011215, 10100484) THEN 'Swiss Baseload'
	 END AS Product
,CASE WHEN InstID IN(10001126, 10641710) THEN 'DE'
	 WHEN InstID IN(10001109, 10001075) THEN 'FR'
	 WHEN InstID IN(10001006, 10641714) THEN 'AT'
	 WHEN InstID IN(10001058, 10011910) THEN 'NL'
	 WHEN InstID IN(10001016, 10011906, 10012566) THEN 'BE'
	 WHEN InstID IN(10001183, 10012528, 10644580, 10011175, 10001855) THEN 'ES'
	 WHEN InstID IN(10000050, 10000085, 10000095, 10640415, 10640419) THEN 'GB'
	 WHEN InstID IN(10001137, 10011036, 10001732) THEN 'HU'
	 WHEN InstID IN(10001027, 10001650, 10001660, 10001996) THEN 'CZ'
	 WHEN InstID IN(10001197, 10011215, 10100484) THEN 'CH'
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
,[DateTime]
,DATEDIFF(DAY,[Datetime], PeriodStart) AS dd
  FROM [pub].[Trayport_Trades_Public] trades
  LEFT JOIN [pub].[Trayport_SequenceItems] si ON trades.FirstSequenceItemID = si.ItemID AND trades.FirstSequenceID = si.SeqID
  LEFT JOIN [dbo].[V_DSTHourLookup] dd ON dateadd(hour,datediff(hour,0,[DateTime]),0) = dd.UTCHour
  WHERE [Datetime]>= DATEADD(DAY,-1,@startDate)
	AND [Datetime] < DATEADD(DAY,+1,@endDate)
	AND dd.UTCHour >= DATEADD(DAY,-1,@startDate)
	AND dd.UTCHour < DATEADD(DAY,+1,@endDate)
	AND ((DATEDIFF(DAY,[Datetime], PeriodStart) = 1 AND InstID NOT IN(10000050, 10000085, 10000095, 10640415, 10640419)) OR (DATEDIFF(DAY,[Datetime], PeriodStart) = 0 AND InstID IN(10000050, 10000085, 10000095, 10640415, 10640419)))
	AND DATEDIFF(DAY, PeriodStart, PeriodEnd) = 1
		AND DATEPART(HOUR,CETCESTHour) >= 9
		AND DATEPART(HOUR,CETCESTHour) <= 11
	AND InstID IN(
				  10641710,10001126,
				  10001109, 10001075,
				  10001006,10641714,
				  10001058, 10011910,
				  10001016, 10011906, 10012566,
				  10001183, 10012528, 10644580, 10011175, 10001855,
				  10000050, 10000085, 10000095, 10640415, 10640419,
				  10001137, 10011036, 10001732,
				  10001027, 10001650, 10001660, 10001996,
				  10001197, 10011215, 10100484)
	--AND ExecutionWorkflow
	--AND FirstSequenceItemName = 'Thu 18/03/21'
	AND PeriodStart >= @startDate
	AND PeriodEnd < @endDate
	AND RouteName = 'House'
	AND [Action] IN('Insert', 'Query')
	AND ((DATEPART(WEEKDAY, si.PeriodStart) IN(3,4,5,6,7) AND  InstID NOT IN(10000050, 10000085, 10000095, 10640415, 10640419)) OR (DATEPART(WEEKDAY, si.PeriodStart) IN(2,3,4,5,6) AND InstID IN(10000050, 10000085, 10000095, 10640415, 10640419)))) AS t1
	WHERE MEDIANCONT < 50
	GROUP BY Product, FirstSequenceItemName, FirstSequenceItemID,t1.PeriodStart, t1.PeriodEnd, dd, Area
	HAVING SUM(Volume) >= 100
	ORDER BY Product, PeriodStart

-- <QueryInfo> QueryName: X:\DA\Tools\EPSI\sql\BaseloadVWAP.sql, CriticalQuery: True </QueryInfo>