/****** Script for SelectTopNRows command from SSMS  ******/
/****** Script for SelectTopNRows command from SSMS  ******/
select DT5.*, DT3.DK1,DT3.NO1,DT3.SE3

from(

Select ValueDateUTC, DK1, SE3,NO1
From(

Select * 

FROM(
SELECT 
       [ValueDateUTC]

      ,[Value]
	  ,rownumberEventID = ROW_NUMBER() OVER(PARTITION BY ValueDateUTC, t1.Curveid ORDER BY ForecastDateUTC DESC)
--	  ,DateTimeCET
	  ,PowerPriceAreaCode
  FROM [pub].[TimeSeries1_v02] t1
  left join pub.dimDateTimeUTC t2 on t1.ForecastDateUTC=t2.DateTimeUTC 
  left join dimMetadata t3 on t1.CurveId=t3.CurveId
  where t1.curveid in (802500176,802500874,802500274) and DATEDIFF(day,ForecastDateUTC,ValuedateUTC)=1 and ValueDateUTC>='2022-03-30 01:00:00.000' and ValueDateUTC<='2022-09-14 21:00:00' and cast(DateTimeCET as time)<='12:00:00.000'
  ) DT1 Where rownumberEventID=1 
   --order by ValueDateUTC desc
  ) PIV

  Pivot(
  avg(Value) for PowerPriceAreaCode in (DK1,SE3,NO1)
  ) DT2


  ) DT3
  left join 
  (select ValueDateUTC, ISNULL([NO2-DK1-FLOW],0) [NO2-DK1-FLOW],ISNULL([DK1-NO2-FLOW],0) [DK1-NO2-FLOW],[NO2-DK1-CAP],[DK1-NO2-CAP],ISNULL([DK1-SE3-FLOW],0) [DK1-SE3-FLOW],ISNULL([SE3-DK1-FLOW],0) [SE3-DK1-FLOW],[SE3-DK1-CAP],[DK1-SE3-CAP],[DK1-NO1->SE3-CAP],[SE3->DK1-NO1-CAP],[DK1 Price],[SE3 Price],[NO1 Price],[NO2 Price],[NO1-SE3-FLOW],[SE3-NO1-FLOW],[SE3-NO1-CAP],[NO1-SE3-CAP]
From(

SELECT [ValueDateUTC]
      ,Value
	  ,Case 
		WHEN T1.CurveId=910004672 THEN 'SE3->DK1-NO1-CAP'
		WHEN T1.CurveId=910004671 THEN 'DK1-NO1->SE3-CAP'
		WHEN T1.Curveid=910001192 THEN 'DK1-SE3-CAP'
		WHEN T1.Curveid=910001200 THEN 'SE3-DK1-CAP'
		WHEN T1.Curveid=910002019 THEN 'DK1-SE3-FLOW'
		WHEN T1.Curveid=910002025 THEN 'SE3-DK1-FLOW'
		WHEN T1.Curveid=910001191 THEN 'DK1-NO2-CAP'
		WHEN T1.CurveId=910001197 THEN 'NO2-DK1-CAP'
		WHEN T1.Curveid=910002018 THEN 'DK1-NO2-FLOW'
		WHEN T1.CurveId=910002022 THEN 'NO2-DK1-FLOW'
		WHEN T1.CurveId=910000074 THEN 'DK1 Price'
		WHEN T1.CurveId=910001254 THEN 'SE3 Price'
		WHEN T1.CurveId=910000090 THEN 'NO1 Price'
		WHEN T1.Curveid=910001248 THEN 'NO2 Price'
		WHEN T1.CurveId=1000342600 THEN 'NO1-SE3-FLOW'
		WHEN T1.CurveId=1000342631 THEN 'SE3-NO1-FLOW'
		WHEN T1.CurveId=910001183 THEN 'SE3-NO1-CAP'
		WHEN T1.Curveid=910001164 THEN 'NO1-SE3-CAP'
	  END as navn

  FROM [pub].[TimeSeries1_v02] T1
  left join pub.dimMetadata T2 on T1.Curveid=T2.Curveid
  where T1.CurveId in (910002018,910002022,910004671,910001192,910002019,910001200,910002025,910004672,910001191,910001197,910000074,910001254,910000090,1000342600,1000342631,910001248,910001183,910001164) and ValueDateUTC>'2022-03-30'

  ) Piv
  PIVOT(
  avg(Value) for navn in ([NO2-DK1-FLOW],[DK1-NO2-FLOW],[NO2-DK1-CAP],[DK1-NO2-CAP],[DK1-SE3-FLOW],[SE3-DK1-FLOW],[SE3-DK1-CAP],[DK1-SE3-CAP],[DK1-NO1->SE3-CAP],[SE3->DK1-NO1-CAP],[DK1 Price],[SE3 Price],[NO1 Price],[NO2 Price],[NO1-SE3-FLOW],[SE3-NO1-FLOW],[SE3-NO1-CAP],[NO1-SE3-CAP])
  )DT4)DT5
  on (DT3.ValuedateUTC=DT5.ValuedateUTC)
  order by ValueDateUTC desc