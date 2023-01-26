/****** Script for SelectTopNRows command from SSMS  ******/
Select ValueDate, [SE3 PC],[DK1 PC],[NO1 PC]
From(
SELECT 
      [ValueDate]
      ,[Value]
	  ,CASE 
		 WHEN CurveID=106317203 THEN 'NO1 PC'
		 WHEN CurveID=106317206 THEN 'DK1 PC'
		 WHEN CurveID=106317188 THEN 'SE3 PC'
	  END as navn
	  ,rownumberEventID = ROW_NUMBER() OVER(PARTITION BY ValueDate, Curveid ORDER BY ForecastDate DESC)
  FROM [FDMData].[dbo].[V_RawTimeseries]
    
	where CurveId in (106317203,106317206,106317188) and ValueDate<='2022-09-14 21:00:00' and ValueDate>'2022-03-30' --and ForecastDate>dateadd(day,-1,SYSDATETIME())
	) PIV
	Pivot
	(
	Avg(Value) for navn in ([SE3 PC], [DK1 PC],[NO1 PC])
	) DT3
		Where rownumbereventid=1

	order by ValueDate asc