SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[sp_alterra_rpt_5_pacifico_historico_precios]
AS

DECLARE @time_init DATETIME2,
        @Misilec INT,
        @Msg VARCHAR (300)


TRUNCATE TABLE Reporte.[05_Pacifico_evolucion_precios]

BEGIN --MODA ESTABLECIMIENTOS
    SET @Msg = '1. Procesando #M... '

    RAISERROR(@Msg, 10, 1) WITH NOWAIT;

	SET @Time_Init = GETDATE();

    SELECT P.Cod_Prod, CM.GTIN, E.Fam_Establec, COUNT(P.Amount) [Moda], P.Amount, YEAR(P.UpdateDate) [YEAR], MONTH(P.UpdateDate) [MES]
    INTO #M
    FROM OPPM.Precios P
    INNER JOIN OPPM.Catalogo_Medicamentos CM
    ON P.Cod_Prod = CM.Cod_Prod
    INNER JOIN Pacifico.Establecimientos E
    ON P.Cod_Estab = E.CODIGO_ESTABLECIMIENTO
    WHERE (E.Fam_Establec IS NOT NULL) AND (E.SubFam_Establec NOT IN ('Sanna')) AND Dpto_Establec = 'Lima'
    GROUP BY P.Cod_Prod, CM.GTIN, E.Fam_Establec, P.Amount, YEAR(P.UpdateDate), MONTH(P.UpdateDate);

    SET @Misilec = DATEDIFF(MILLISECOND, @time_init, GETDATE())

    SET @Msg = @Msg + 'OK | ' + CONVERT(varchar, DATEADD(ms, @Misilec, 0), 114)

    RAISERROR(@Msg , 10, 1) WITH NOWAIT;

END

BEGIN
    SET @Msg = '2. Procesando #MC... '

    RAISERROR(@Msg, 10, 1) WITH NOWAIT;

	SET @Time_Init = GETDATE();

    SELECT ROW_NUMBER() OVER(PARTITION BY Cod_Prod, Fam_Establec, [YEAR], [MES] ORDER BY Moda, Amount ASC) ROW
    , Moda
    , #M.Cod_Prod
    , #M.GTIN
    , #M.Fam_Establec
    , #M.Amount
    , #M.[YEAR]
    , #M.MES
    INTO #MC
    FROM #M;

    SET @Misilec = DATEDIFF(MILLISECOND, @time_init, GETDATE())

    SET @Msg = @Msg + 'OK | ' + CONVERT(varchar, DATEADD(ms, @Misilec, 0), 114)

    RAISERROR(@Msg , 10, 1) WITH NOWAIT;

END

BEGIN --MODA SUB ESTABLECIMIENTOS
    SET @Msg = '3. Procesando #MM... '

    RAISERROR(@Msg, 10, 1) WITH NOWAIT;

	SET @Time_Init = GETDATE();

    SELECT P.Cod_Prod, CM.GTIN, E.Fam_Establec, E.SubFam_Establec, COUNT(P.Amount) [Moda], P.Amount, YEAR(P.UpdateDate) [YEAR], MONTH(P.UpdateDate) [MES]
    INTO #MM
    FROM OPPM.Precios P
    INNER JOIN OPPM.Catalogo_Medicamentos CM
    ON P.Cod_Prod = CM.Cod_Prod
    INNER JOIN Pacifico.Establecimientos E
    ON P.Cod_Estab = E.CODIGO_ESTABLECIMIENTO
    WHERE (E.SubFam_Establec IS NOT NULL) AND (E.SubFam_Establec NOT IN ('Sanna')) AND Dpto_Establec = 'Lima'
    GROUP BY P.Cod_Prod, CM.GTIN, E.Fam_Establec, E.SubFam_Establec, P.Amount, YEAR(P.UpdateDate), MONTH(P.UpdateDate);

    SET @Misilec = DATEDIFF(MILLISECOND, @time_init, GETDATE())

    SET @Msg = @Msg + 'OK | ' + CONVERT(varchar, DATEADD(ms, @Misilec, 0), 114)

    RAISERROR(@Msg , 10, 1) WITH NOWAIT;

END

BEGIN
    SET @Msg = '4. Procesando #MS... '

    RAISERROR(@Msg, 10, 1) WITH NOWAIT;

	SET @Time_Init = GETDATE();

    SELECT ROW_NUMBER() OVER(PARTITION BY Cod_Prod, Fam_Establec, SubFam_Establec, [YEAR], [MES] ORDER BY Moda, Amount ASC) ROW
    , Moda
    , #MM.Cod_Prod
    , #MM.GTIN
    , #MM.Fam_Establec
    , SubFam_Establec
    , #MM.Amount
    , #MM.[YEAR]
    , #MM.MES
    INTO #MS
    FROM #MM;

    SET @Misilec = DATEDIFF(MILLISECOND, @time_init, GETDATE())

    SET @Msg = @Msg + 'OK | ' + CONVERT(varchar, DATEADD(ms, @Misilec, 0), 114)

    RAISERROR(@Msg , 10, 1) WITH NOWAIT;

END

BEGIN -- UNION MODA
    SET @Msg = '5. Procesando #MODA... '

    RAISERROR(@Msg, 10, 1) WITH NOWAIT;

	SET @Time_Init = GETDATE();

    SELECT 
        #MC.Cod_Prod
        , #MC.GTIN
        , #MC.Fam_Establec
        , CASE 
            WHEN #MC.Fam_Establec = 'Cadena'
                THEN 'Resto Farmacias'
            ELSE 'Resto Clínicas'
            END [SubFam_Establec]
        , #MC.Amount
        , #MC.[YEAR]
        , #MC.MES
    INTO #MODA
    FROM #MC
    WHERE #MC.[ROW] = 1
    UNION
    SELECT #MS.Cod_Prod
        , #MS.GTIN
        , #MS.Fam_Establec
        , #MS.SubFam_Establec
        , #MS.Amount
        , #MS.[YEAR]
        , #MS.MES
    FROM #MS
    WHERE #MS.[ROW] = 1;

    SET @Misilec = DATEDIFF(MILLISECOND, @time_init, GETDATE())

    SET @Msg = @Msg + 'OK | ' + CONVERT(varchar, DATEADD(ms, @Misilec, 0), 114)

    RAISERROR(@Msg , 10, 1) WITH NOWAIT;

END

BEGIN -- #CALENDARIO MAESTRO
    SET @Msg = '6. Procesando #CAL... '

    RAISERROR(@Msg, 10, 1) WITH NOWAIT;

	SET @Time_Init = GETDATE();

    SELECT 
        #MODA.Fam_Establec
        , #MODA.SubFam_Establec
        , #MODA.Cod_Prod
        , #MODA.GTIN
    INTO #CAL
    FROM #MODA
    -- WHERE Cod_Prod NOT IN (SELECT Cod_Prod FROM Temp.Analisis_Precios20210913)
    --    WHERE Cod_Prod = '43948'
    GROUP BY 
        #MODA.Fam_Establec
        , #MODA.SubFam_Establec
        , #MODA.Cod_Prod
        , #MODA.GTIN;
    
    SET @Misilec = DATEDIFF(MILLISECOND, @time_init, GETDATE())

    SET @Msg = @Msg + 'OK | ' + CONVERT(varchar, DATEADD(ms, @Misilec, 0), 114)

    RAISERROR(@Msg , 10, 1) WITH NOWAIT;

END

BEGIN
    SET @Msg = '7. Procesando #ENDARIO... '

    RAISERROR(@Msg, 10, 1) WITH NOWAIT;

	SET @Time_Init = GETDATE();

    SELECT YEAR(pe.UpdateDate) [AÑO], MONTH(pe.UpdateDate) [MES]
    INTO #ENDARIO
    FROM OPPM.Precios pe
    GROUP BY YEAR(pe.UpdateDate), MONTH(pe.UpdateDate);

    SET @Misilec = DATEDIFF(MILLISECOND, @time_init, GETDATE())

    SET @Msg = @Msg + 'OK | ' + CONVERT(varchar, DATEADD(ms, @Misilec, 0), 114)

    RAISERROR(@Msg , 10, 1) WITH NOWAIT;

END

BEGIN
    SET @Msg = '8. Procesando #CALENDARIO... '

    RAISERROR(@Msg, 10, 1) WITH NOWAIT;

	SET @Time_Init = GETDATE();

    SELECT *
    INTO #CALENDARIO
    FROM #CAL
    CROSS JOIN #ENDARIO;

    SET @Misilec = DATEDIFF(MILLISECOND, @time_init, GETDATE())

    SET @Msg = @Msg + 'OK | ' + CONVERT(varchar, DATEADD(ms, @Misilec, 0), 114)

    RAISERROR(@Msg , 10, 1) WITH NOWAIT;

END

BEGIN -- INSERCIÓN A TABLA FINAL
    SET @Msg = '9. Procesando #CALENDARIO... '

    RAISERROR(@Msg, 10, 1) WITH NOWAIT;

	SET @Time_Init = GETDATE();

    INSERT INTO Reporte.[05_Pacifico_evolucion_precios]
    SELECT ROW_NUMBER() OVER(ORDER BY #CALENDARIO.Cod_Prod, #CALENDARIO.Fam_Establec, #CALENDARIO.SubFam_Establec, #CALENDARIO.[AÑO], #CALENDARIO.MES) [ID]
        , ROW_NUMBER() OVER(PARTITION BY #CALENDARIO.Cod_Prod ORDER BY #CALENDARIO.Fam_Establec, #CALENDARIO.SubFam_Establec, #CALENDARIO.[AÑO], #CALENDARIO.MES) [ROW]
        , ROW_NUMBER() OVER(PARTITION BY #CALENDARIO.Cod_Prod, #CALENDARIO.SubFam_Establec ORDER BY #CALENDARIO.Fam_Establec, #CALENDARIO.SubFam_Establec, #CALENDARIO.[AÑO], #CALENDARIO.MES) [ROW2]
        , CM.Prin_Activo
        , #CALENDARIO.Cod_Prod
        , CS.Cod_SahSac
        , PA.Descripción_SahSac
        , PA.AfectoIGV [Pacifico_IGV]
        , ISNULL(GI.AfectoIGV,DI.AfectoIGV) [OPPM_IGV]
        , #CALENDARIO.Fam_Establec
        , #CALENDARIO.SubFam_Establec
        , #MODA.Amount [Pacifico_Amount]
        , #MODA.Amount [OPPM_Amount]
        , #CALENDARIO.[AÑO]
        , #CALENDARIO.MES
    -- INTO Reporte.[05_Pacifico_evolucion_precios]
    FROM #CALENDARIO--OPPM.Catalogo_Medicamentos CM
    LEFT JOIN Pacifico.Codigos_Sanna CS
    ON #CALENDARIO.Cod_Prod = CS.Cod_Prod
    LEFT JOIN OPPM.Catalogo_Medicamentos CM
    ON #CALENDARIO.Cod_Prod = CM.Cod_Prod
    LEFT JOIN #MODA
        ON #CALENDARIO.Cod_Prod = #MODA.Cod_Prod
        AND #CALENDARIO.AÑO = #MODA.[YEAR]
        AND #CALENDARIO.MES = #MODA.MES
        AND #CALENDARIO.SubFam_Establec = #MODA.SubFam_Establec
    LEFT JOIN OPPM.Afecto_IGV DI
        ON #CALENDARIO.[AÑO] = DI.AÑO
            AND #CALENDARIO.MES = DI.MES
            AND #CALENDARIO.Cod_Prod = DI.Cod_Prod
    LEFT JOIN OPPM.Afecto_IGV GI
        ON #CALENDARIO.[AÑO] = GI.AÑO
            AND #CALENDARIO.MES = GI.MES
            AND #CALENDARIO.GTIN = GI.Cod_Prod
    LEFT JOIN Pacifico.Addons PA
        ON CS.Cod_SahSac = PA.CodGeneral
    ORDER BY 1,2,3
    --WHERE CM.Cod_Prod = '43948'
    SET @Misilec = DATEDIFF(MILLISECOND, @time_init, GETDATE())

    SET @Msg = @Msg + 'OK | ' + CONVERT(varchar, DATEADD(ms, @Misilec, 0), 114)

    RAISERROR(@Msg , 10, 1) WITH NOWAIT;

END

BEGIN ----- LAST NON NULL OPPM_UNIT -----
    SET @Msg = '10. Procesando LAST NON NULL Monto... '

    RAISERROR(@Msg, 10, 1) WITH NOWAIT;

	SET @Time_Init = GETDATE();

    WITH x AS
    (
    SELECT id, [row], rpt.Pacifico_Amount, OPPM_Amount, relevantid,
    MAX(relevantid) OVER(ORDER BY id
                ROWS UNBOUNDED PRECEDING) AS grp
    FROM Reporte.[05_Pacifico_evolucion_precios] rpt
    CROSS APPLY ( VALUES(CASE WHEN rpt.Pacifico_Amount IS NOT NULL OR [ROW2] = 1 THEN id END ) )
        AS A(relevantid)
    --   ORDER BY ID
    ), xx AS
    ( SELECT *
    , MAX(x.Pacifico_Amount) OVER( PARTITION BY grp
            ORDER BY id
            ROWS UNBOUNDED PRECEDING ) AS maxcol1ingrp
    , MAX(x.OPPM_Amount) OVER( PARTITION BY grp
            ORDER BY id
            ROWS UNBOUNDED PRECEDING ) AS maxcol2ingrp
    FROM x
    ) --SELECT xx.*
    UPDATE rpt
    SET rpt.Pacifico_Amount = xx.maxcol1ingrp,
        rpt.OPPM_Amount = xx.maxcol2ingrp
    FROM Reporte.[05_Pacifico_evolucion_precios] rpt
    INNER JOIN xx
    ON rpt.ID = xx.ID
    --ORDER BY xx.ID
    WHERE rpt.Pacifico_Amount IS NULL;

    SET @Misilec = DATEDIFF(MILLISECOND, @time_init, GETDATE())

    SET @Msg = @Msg + 'OK | ' + CONVERT(varchar, DATEADD(ms, @Misilec, 0), 114)

    RAISERROR(@Msg , 10, 1) WITH NOWAIT;

END

BEGIN
    SET @Msg = '11. Procesando LAST NON NULL IGV... '

    RAISERROR(@Msg, 10, 1) WITH NOWAIT;

	SET @Time_Init = GETDATE();

    UPDATE Reporte.[05_Pacifico_evolucion_precios]
    SET Pacifico_Amount = CASE WHEN Pacifico_IGV = 1 THEN Pacifico_Amount / 1.18 ELSE Pacifico_Amount END,
        OPPM_Amount = CASE WHEN OPPM_IGV = 1 THEN OPPM_Amount / 1.18 ELSE OPPM_Amount END

    SET @Misilec = DATEDIFF(MILLISECOND, @time_init, GETDATE())

    SET @Msg = @Msg + 'OK | ' + CONVERT(varchar, DATEADD(ms, @Misilec, 0), 114)

    RAISERROR(@Msg , 10, 1) WITH NOWAIT;

END
GO
