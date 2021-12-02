SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[sp_alterra_rpt_4_historico_precios_Kairos] AS

TRUNCATE TABLE Reportes.[4_APEPS_historico_precios_Kairos];

BEGIN ------- LLENA LA TABLA -------
    WITH y AS -- MEDIANA
    (
        SELECT Cod_Prod
            , YEAR(pe.UpdateDate) [AÑO]
            , DATEPART(MONTH, pe.UpdateDate) [MES]
            , DATEPART(quarter, pe.UpdateDate) [Trimestre]
        FROM OPPM.Precios pe
        GROUP BY Cod_Prod
            , YEAR(pe.UpdateDate)
            , DATEPART(MONTH, pe.UpdateDate)        
            , DATEPART(quarter, pe.UpdateDate)
    ), yy AS
    (
        SELECT DISTINCT 
            y.Cod_Prod
            , y.AÑO [AÑO]
            , y.Trimestre [TRIM]
            , y.MES [MES]
            , PERCENTILE_CONT(0.5) WITHIN
                GROUP (
                        ORDER BY Amount
                    ) OVER (PARTITION BY y.Cod_Prod, y.AÑO, y.MES) AS Mediana_Precio            
        FROM OPPM.Precios pe
        INNER JOIN OPPM.Establecimientos EST
        ON pe.Cod_Estab = EST.CODIGO_ESTABLECIMIENTO
        INNER JOIN y
        ON y.Cod_Prod = pe.Cod_Prod
        AND(
                (
                DATEPART(MONTH, pe.UpdateDate) >= y.MES + 6
                AND DATEPART(YEAR, pe.UpdateDate) = y.AÑO - 1
                )
            OR (
                DATEPART(MONTH, pe.UpdateDate) < y.MES
                AND DATEPART(YEAR, pe.UpdateDate) = y.AÑO
                )
            ) 
    WHERE EST.SEGMENTACION IN ('cadena', 'independiente')
    --    WHERE pe.Cod_Prod = '50584' 
    ---------FIN MEDIANA -------------
    ), x --- KAIROS
    AS
    (
        SELECT 
            CM.GTIN
            , DK.Kairos_CODIGO
            , DK.OPPM_CODIGO
            , KP.PVF
            , CASE WHEN ISNULL(KP.PPS,0) = 0  THEN (KP.PVF*1.33) ELSE KP.PPS END PPS
            , (KP.PVF/CM.Fracciones) PVF_UNIT
            , CASE WHEN ISNULL(KP.PPS,0) = 0 THEN ((KP.PVF*1.33)/CM.Fracciones) ELSE (KP.PPS/CM.Fracciones) END PPS_UNIT
            , KP.AÑO
            , KP.MES
        FROM OPPM.DIGEMID_Kairos DK
        INNER JOIN OPPM.Catalogo_Medicamentos CM
            ON DK.OPPM_CODIGO = CM.Cod_Prod
        INNER JOIN Kairos.Precios KP
            ON DK.Kairos_CODIGO = KP.CODIGO
        WHERE ISNULL(PVF,0) <> 0
    ---------FIN Kairos -------------
    ), z AS --- CREACIÓN DE MAESTRO HISTÓRICO ----
    (
        SELECT D.*,CC.GTIN, CJ.*
        FROM  OPPM.DIGEMID_Kairos D
        INNER JOIN OPPM.Catalogo_Medicamentos CC
        ON OPPM_CODIGO = Cod_Prod
            CROSS JOIN
            (
                SELECT YEAR(pe.UpdateDate) [AÑO], MONTH(pe.UpdateDate) [MES]
                FROM OPPM.Precios pe
                GROUP BY YEAR(pe.UpdateDate), MONTH(pe.UpdateDate)
            ) CJ
    ---------FIN MAESTRO -------------
    )  
    INSERT INTO Reportes.[4_APEPS_historico_precios_Kairos]
    SELECT 
      ROW_NUMBER() OVER (ORDER BY z.OPPM_CODIGO, z.AÑO, z.MES) [ID]
    , ROW_NUMBER() OVER (PARTITION BY z.OPPM_CODIGO ORDER BY z.AÑO, z.MES ASC) [ROW]
    , z.GTIN
    , z.OPPM_CODIGO
    , z.Kairos_CODIGO [Cod_Kairos]
    , C.Nom_Prod
    , C.Laboratory
    , C.Prin_Activo
    , C.Clasi_ATC
    , C.[Nivel 1]
    , C.[Nivel 1 Desc]
    , C.[Nivel 2]
    , C.[Nivel 2 Desc]
    , C.[Nivel 3]
    , C.[Nivel 3 Desc]
    , C.[Nivel 4]
    , C.[Nivel 4 Desc]
    , C.[Nivel 5]
    , C.[Nivel 5 Desc]
    , C.nom_form_farm_simp
    , C.Concent
    , C.Fracciones
    , x.PVF
    , x.PVF_UNIT
    , x.PPS
    , x.PPS_UNIT
    , yy.Mediana_Precio [OPPM_Unit]
    , ISNULL(AG.AfectoIGV,AD.AfectoIGV) AfectoIGV
    , z.AÑO
    , z.MES
    , CC.Descripcion
    FROM z
    --INTO Reportes.[4_APEPS_historico_precios_Kairos]
    INNER JOIN OPPM.Catalogo_Medicamentos C
    ON z.OPPM_CODIGO = C.Cod_Prod
    LEFT JOIN x --- Kairos
    ON z.OPPM_CODIGO = x.OPPM_CODIGO
    AND z.AÑO = x.AÑO
    AND z.MES = x.MES
    LEFT JOIN OPPM.Afecto_IGV AG --- AfectoIGV GTIN
    ON z.GTIN = AG.Cod_Prod
    AND z.MES = AG.MES
    AND z.AÑO = AG.AÑO
    LEFT JOIN OPPM.Afecto_IGV AD --- AfectoIGV Digemid
    ON z.OPPM_CODIGO = AD.Cod_Prod
    AND z.AÑO = AD.AÑO
    AND z.MES = AD.MES
    LEFT JOIN yy --- Mediana
    ON z.OPPM_CODIGO = yy.Cod_Prod
    AND z.AÑO = yy.AÑO
    AND z.MES = yy.MES
    LEFT JOIN Catalogo.Condicion_comercial CC
    ON C.Condicion_comercial = CC.id
    --WHERE yy.Mediana_Precio IS NOT NULL
END

BEGIN ------- LAST NON NULL PVF Y PPS -------
    WITH x AS
    (
    SELECT id, [row], rpt.PVF, rpt.PPS, rpt.PVF_UNIT, rpt.PPS_UNIT, relevantid,
    MAX(relevantid) OVER(ORDER BY id
                ROWS UNBOUNDED PRECEDING ) AS grp
    FROM Reportes.[4_APEPS_historico_precios_Kairos] rpt
    CROSS APPLY ( VALUES(CASE WHEN rpt.PVF IS NOT NULL OR [ROW] = 1 THEN id END ) )
        AS A(relevantid)
    --   ORDER BY ID
    ), xx AS
    ( SELECT *,
    MAX(x.PVF) OVER( PARTITION BY grp
            ORDER BY id
            ROWS UNBOUNDED PRECEDING ) AS maxcol1ingrp
    ,MAX(x.PPS) OVER( PARTITION BY grp
            ORDER BY id
            ROWS UNBOUNDED PRECEDING ) AS maxcol2ingrp
    ,MAX(x.PVF_UNIT) OVER( PARTITION BY grp
            ORDER BY id
            ROWS UNBOUNDED PRECEDING ) AS maxcol3ingrp
    ,MAX(x.PPS_UNIT) OVER( PARTITION BY grp
            ORDER BY id
            ROWS UNBOUNDED PRECEDING ) AS maxcol4ingrp
    FROM x
    ) --SELECT xx.*
    UPDATE rpt
    SET rpt.PVF = xx.maxcol1ingrp,
        rpt.PPS = xx.maxcol2ingrp,
        rpt.PVF_UNIT = xx.maxcol3ingrp,
        rpt.PPS_UNIT = xx.maxcol4ingrp
    FROM Reportes.[4_APEPS_historico_precios_Kairos] rpt
    INNER JOIN xx
    ON rpt.ID = xx.ID
    WHERE rpt.PVF IS NULL;

END

BEGIN ----- LAST NON NULL IGV -----
    WITH x AS
    (
    SELECT id, [row], rpt.AfectoIGV, relevantid,
    MAX(relevantid) OVER(ORDER BY id
                ROWS UNBOUNDED PRECEDING) AS grp
    FROM Reportes.[4_APEPS_historico_precios_Kairos] rpt
    CROSS APPLY ( VALUES(CASE WHEN rpt.AfectoIGV IS NOT NULL OR [ROW] = 1 THEN id END ) )
        AS A(relevantid)
    --   ORDER BY ID
    ), xx AS
    ( SELECT *
    , MAX(x.AfectoIGV) OVER( PARTITION BY grp
            ORDER BY id
            ROWS UNBOUNDED PRECEDING ) AS maxcol1ingrp
    FROM x
    ) --SELECT xx.*
    UPDATE rpt
    SET rpt.AfectoIGV = xx.maxcol1ingrp
    FROM Reportes.[4_APEPS_historico_precios_Kairos] rpt
    INNER JOIN xx
    ON rpt.ID = xx.ID
    --ORDER BY xx.ID
    WHERE rpt.AfectoIGV IS NULL;

END

BEGIN ----- FIRST NON NULL IGV -----
    WITH x AS
    (
    SELECT id, [row], rpt.AfectoIGV, relevantid,
    MIN(relevantid) OVER(ORDER BY id DESC
                ROWS UNBOUNDED PRECEDING) AS grp
    FROM Reportes.[4_APEPS_historico_precios_Kairos] rpt
    CROSS APPLY ( VALUES(CASE WHEN rpt.AfectoIGV IS NOT NULL OR [ROW] = 13 THEN id END ) )
        AS A(relevantid)
    --   ORDER BY ID
    ), xx AS
    ( SELECT *
    , MIN(x.AfectoIGV) OVER( PARTITION BY grp
            ORDER BY id DESC
            ROWS UNBOUNDED PRECEDING ) AS maxcol1ingrp
    FROM x
    ) --SELECT xx.*
    UPDATE rpt
    SET rpt.AfectoIGV = xx.maxcol1ingrp
    FROM Reportes.[4_APEPS_historico_precios_Kairos] rpt
    INNER JOIN xx
    ON rpt.ID = xx.ID
    --ORDER BY xx.ID
    WHERE rpt.AfectoIGV IS NULL;

END

BEGIN ----- LAST NON NULL OPPM_UNIT -----
    WITH x AS
    (
    SELECT id, [row], rpt.OPPM_Unit, relevantid,
    MAX(relevantid) OVER(ORDER BY id
                ROWS UNBOUNDED PRECEDING) AS grp
    FROM Reportes.[4_APEPS_historico_precios_Kairos] rpt
    CROSS APPLY ( VALUES(CASE WHEN rpt.OPPM_Unit IS NOT NULL OR [ROW] = 1 THEN id END ) )
        AS A(relevantid)
    --   ORDER BY ID
    ), xx AS
    ( SELECT *
    , MAX(x.OPPM_Unit) OVER( PARTITION BY grp
            ORDER BY id
            ROWS UNBOUNDED PRECEDING ) AS maxcol1ingrp
    FROM x
    ) --SELECT xx.*
    UPDATE rpt
    SET rpt.OPPM_Unit = xx.maxcol1ingrp
    FROM Reportes.[4_APEPS_historico_precios_Kairos] rpt
    INNER JOIN xx
    ON rpt.ID = xx.ID
    --ORDER BY xx.ID
    WHERE rpt.OPPM_Unit IS NULL;

END

UPDATE Reportes.[4_APEPS_historico_precios_Kairos]
SET OPPM_Unit = CASE WHEN AfectoIGV = 1 THEN OPPM_Unit / 1.18 ELSE OPPM_Unit END,
    PVF_UNIT = CASE WHEN AfectoIGV = 1 THEN PVF_UNIT / 1.18 ELSE PVF_UNIT END,
    PPS_UNIT = CASE WHEN AfectoIGV = 1 THEN PPS_UNIT / 1.18 ELSE PPS_UNIT END;

-- BEGIN ----- FIRST NON NULL OPPM_UNIT -----
--     WITH x AS
--     (
--     SELECT id, [row], rpt.OPPM_Unit, relevantid,
--     MIN(relevantid) OVER(ORDER BY id DESC
--                 ROWS UNBOUNDED PRECEDING) AS grp
--     FROM Reportes.[4_APEPS_historico_precios_Kairos] rpt
--     CROSS APPLY ( VALUES(CASE WHEN rpt.OPPM_Unit IS NOT NULL OR [ROW] = 13 THEN id END ) )
--         AS A(relevantid)
--     --   ORDER BY ID
--     ), xx AS
--     ( SELECT *
--     , MIN(x.OPPM_Unit) OVER( PARTITION BY grp ---NOTA IMPORTANTE: EL MIN O MAX DE ESTA SECCIÓN ES INDIFERENTE
--             ORDER BY id DESC
--             ROWS UNBOUNDED PRECEDING ) AS maxcol1ingrp
--     FROM x
--     ) --SELECT xx.*
--     UPDATE rpt
--     SET rpt.OPPM_Unit = xx.maxcol1ingrp
--     FROM Reportes.[4_APEPS_historico_precios_Kairos] rpt
--     INNER JOIN xx
--     ON rpt.ID = xx.ID
--     --ORDER BY xx.ID
--     WHERE rpt.OPPM_Unit IS NULL;

-- END
GO
