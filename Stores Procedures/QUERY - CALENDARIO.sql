SET DATEFIRST  1 -- 1 = Lunes, 7 = Domingo
    -- DATEFORMAT dmy

DECLARE @StartDate  date = '20200101',
        @n int = 10;

DECLARE @CutoffDate date = DATEADD(DAY, -1, DATEADD(YEAR, @n+1, @StartDate)); -- Cantidad de años que se ejecutarán = n+1

;WITH seq(n) AS 
(
  SELECT 0 UNION ALL SELECT n + 1 FROM seq
  WHERE n < DATEDIFF(DAY, @StartDate, @CutoffDate)
),
d(d) AS 
(
  SELECT DATEADD(DAY, n, @StartDate) FROM seq
),
src AS
(
  SELECT
    fecha         = CONVERT(date, d),
    dia_mes          = DATEPART(DAY,       d),
    dia_desc      = DATENAME(WEEKDAY,   d),
    semana         = DATEPART(WEEK,      d),
    semana_ISO      = DATEPART(ISO_WEEK,  d),
    dia_semana    = DATEPART(WEEKDAY,   d),
    mes        = DATEPART(MONTH,     d),
    mes_desc    = DATENAME(MONTH,     d),
    trimestre      = DATEPART(Quarter,   d),
    año         = DATEPART(YEAR,      d),
    primer_dia_mes = DATEFROMPARTS(YEAR(d), MONTH(d), 1),
    ultimo_dia_mes   = DATEFROMPARTS(YEAR(d), 12, 31),
    dia_año    = DATEPART(DAYOFYEAR, d)
  FROM d
)
SELECT * 
INTO OPPM.CALENDARIO
FROM src
  ORDER BY 1
  OPTION (MAXRECURSION 0);