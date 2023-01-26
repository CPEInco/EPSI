--rignore
   DECLARE @start DATETIME = GETDATE() - 3
   DECLARE @end DATETIME = @start + 3
   DECLARE @DA_days INT = -1
   DECLARE @DA_hour INT = 12
   DECLARE @startF DATETIME = GETDATE() - 2
   DECLARE @endF DATETIME = @start + 4
   --end

   SET NOCOUNT ON
   
   SELECT * INTO #data
     FROM (SELECT "@CurveId", "@ForecastDate", "@ValueDate", "Value", "V_DateTimeCET", "F_DateTimeCET", DATEADD("HOUR", @DA_hour, DATEADD("dd", @DA_days, DATEDIFF("dd", 0.0, "V_DateTimeCET"))) AS "DAH"
           FROM (SELECT "TBL_LEFT"."@CurveId" AS "@CurveId", "TBL_LEFT"."@ForecastDate" AS "@ForecastDate", "TBL_LEFT"."@ValueDate" AS "@ValueDate", "TBL_LEFT"."Value" AS "Value", "TBL_LEFT"."V_DateTimeCET" AS "V_DateTimeCET", "TBL_RIGHT"."F_DateTimeCET" AS "F_DateTimeCET"
                 FROM (SELECT "TBL_LEFT"."@CurveId" AS "@CurveId", "TBL_LEFT"."@ForecastDate" AS "@ForecastDate", "TBL_LEFT"."@ValueDate" AS "@ValueDate", "TBL_LEFT"."Value" AS "Value", "TBL_RIGHT"."V_DateTimeCET" AS "V_DateTimeCET"
                          FROM (SELECT *
                             FROM [pub].[@table]
                             WHERE ((CurveId IN @ids) AND ("@ValueDate" >= @start) AND ("@ValueDate" <= @end) AND (@ForecastDate > @startF) AND (@ForecastDate < @endF))
                             ) "TBL_LEFT"
                             LEFT JOIN (SELECT "DateTimeUTC" AS "@ValueDate", "DateTimeCET" AS "V_DateTimeCET"
                                        FROM pub.dimDateTimeUTC) "TBL_RIGHT"
                             ON (DATEADD(MINUTE,-DATEPART(MINUTE,"TBL_LEFT"."@ValueDate"),DATEADD(SECOND,-DATEPART(SECOND,"TBL_LEFT"."@ValueDate"),"TBL_LEFT"."@ValueDate")) = "TBL_RIGHT"."@ValueDate")
                       ) "TBL_LEFT"
                       LEFT JOIN (SELECT "DateTimeUTC" AS "@ForecastDate", "DateTimeCET" AS "F_DateTimeCET"
                                  FROM pub.dimDateTimeUTC) "TBL_RIGHT"
                       ON (DATEADD(MINUTE,-DATEPART(MINUTE,"TBL_LEFT"."ForecastDateUTC"),DATEADD(SECOND,-DATEPART(SECOND,"TBL_LEFT"."ForecastDateUTC"),DATEADD(MS,-DATEPART(MS,"TBL_LEFT"."ForecastDateUTC"),"TBL_LEFT"."ForecastDateUTC"))) = "TBL_RIGHT"."ForecastDateUTC")
					   -- ON (DATEADD(MINUTE,-DATEPART(MINUTE,"TBL_LEFT"."ForecastDateUTC"),DATEADD(SECOND,-DATEPART(SECOND,"TBL_LEFT"."ForecastDateUTC"),"TBL_LEFT"."ForecastDateUTC")) = "TBL_RIGHT"."ForecastDateUTC")
               ) "mqkrutlhuy"
           ) "htrxvzfkag"
   WHERE ("F_DateTimeCET" < "DAH")

   SELECT rt.@CurveId, rt.@ValueDate, rt.@ForecastDate, rt.[Value]
   FROM (SELECT CurveId, @ValueDate, MAX(@ForecastDate) as maxForecast
         FROM #data
         GROUP BY CurveId, @ValueDate) rt1
         INNER JOIN ( SELECT * FROM #data) as rt
         ON rt1.CurveId = rt.CurveId AND rt1.@ValueDate = rt.@ValueDate AND rt1.maxForecast = rt.@ForecastDate
   
   DROP TABLE #data