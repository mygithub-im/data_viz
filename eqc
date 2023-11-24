DECLARE year_date DATE DEFAULT DATE_TRUNC(CURRENT_DATE, YEAR) ; 
DECLARE year_N1_date DATE DEFAULT DATE_SUB(year_date, INTERVAL 1 YEAR) ; 
DECLARE year_N2_date DATE DEFAULT DATE_SUB(year_date, INTERVAL 2 YEAR) ; 
DECLARE last_month_start_date DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH) ; 
DECLARE last_month_start_N1_date DATE DEFAULT DATE_SUB(last_month_start_date, INTERVAL 1 YEAR) ; 
DECLARE last_month_start_N2_date DATE DEFAULT DATE_SUB(last_month_start_date, INTERVAL 2 YEAR) ; 
DECLARE last_month_end_date DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 DAY) ; 
DECLARE last_month_end_N1_date DATE DEFAULT DATE_SUB(last_month_end_date, INTERVAL 1 YEAR) ; 
DECLARE last_month_end_N2_date DATE DEFAULT DATE_SUB(last_month_end_date, INTERVAL 2 YEAR) ; 

-- Incremental
CREATE OR REPLACE TABLE `marketing-dev-237914.Dash_Equation_Commercial_Fidelite.DS_EQC_Details` 
PARTITION BY  Date_Mois
CLUSTER BY Enseigne, Region, Magasin
OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 40 DAY)) 
AS 
WITH 
    pass AS (
        SELECT loy_card_key, MAX(crea_dt) AS crea_dt, 1 AS flagPass 
        FROM `marketing-dev-237914.Data_tables_Teradata.d_indiv_loy_card_month_pass` 
        GROUP BY loy_card_key
    ),
    eqc_details AS (
        SELECT
             DATE_TRUNC(t.dateKey, MONTH) AS Date_Mois
            ,t.Enseigne
            ,t.Magasin
            ,t.Region

            -- Clients 
            ,CASE WHEN p.hhd_num IS NOT NULL THEN 1 ELSE 0 END flagPrime
            ,CASE WHEN au.hhd_num IS NOT NULL THEN 1 ELSE 0 END AS flagAppUser
            ,CASE WHEN cp.hhd_num IS NOT NULL THEN 1 ELSE 0 END AS flagAbonnes
            ,COALESCE(pa.flagPass,0) AS flagPass

             -- Segments
             ,COALESCE(sg.Segment_consostyle, 'Aucun') AS Segment_consostyle
             ,COALESCE(sg.Segment_lifestage, 'Aucun') AS Segment_lifestage
             ,COALESCE(sg.Segment_struct, 'Aucun') AS Segment_struct
             ,CASE WHEN sg.Segment_mixeur='Mixeur' THEN 1 ELSE 0 END AS Segment_mixeur
             ,CASE WHEN sg.Segment_mixeurEcom='Mixeur' THEN 1 ELSE 0 END AS Segment_mixeurEcom             

            -- TOTAL
            ,COUNT(DISTINCT transactionId) AS N_TRX_TOTAL
            ,SUM(COALESCE(t.CA,0)) AS CA_TOTAL 

            -- PORTEURS IDs
            ,ARRAY_AGG(DISTINCT CAST(t.hhd_num AS INT64) IGNORE NULLS) AS ARRAY_PORTEURS
            
            -- TRX PORTEURS
            ,COUNT(DISTINCT CASE WHEN t.hhd_num IS NOT NULL THEN transactionId END) AS N_TRX

            -- CA PORTEURS
            ,SUM(CASE WHEN t.hhd_num IS NOT NULL THEN COALESCE(t.CA,0) ELSE 0 END) AS CA 
            ,SUM(CASE WHEN t.hhd_num IS NOT NULL THEN COALESCE(t.CA_MDC,0) ELSE 0 END) AS CA_MDC   
            ,SUM(CASE WHEN t.hhd_num IS NOT NULL THEN COALESCE(t.CA_ALIM,0) ELSE 0 END) AS CA_ALIM 
            ,SUM(CASE WHEN t.hhd_num IS NOT NULL THEN COALESCE(t.CA_ALIM_MDC,0) ELSE 0 END) AS CA_ALIM_MDC  

            --QUANTITE PORTEURS
            ,SUM(CASE WHEN t.hhd_num IS NOT NULL THEN COALESCE(t.Q,0) ELSE 0 END) AS Q 
            ,SUM(CASE WHEN t.hhd_num IS NOT NULL THEN COALESCE(t.Q_MDC,0) ELSE 0 END) AS Q_MDC   
            ,SUM(CASE WHEN t.hhd_num IS NOT NULL THEN COALESCE(t.Q_ALIM,0) ELSE 0 END) AS Q_ALIM  
            ,SUM(CASE WHEN t.hhd_num IS NOT NULL THEN COALESCE(t.Q_ALIM,0) ELSE 0 END) AS Q_ALIM_MDC  

            -- AVTG PORTEURS
            ,SUM(CASE WHEN t.hhd_num IS NOT NULL THEN COALESCE(t.RI,0) ELSE 0 END) AS RI
            ,SUM(CASE WHEN t.hhd_num IS NOT NULL THEN COALESCE(t.RD,0) ELSE 0 END) AS RD

        FROM `marketing-dev-237914.Dash_Equation_Commercial_Fidelite.DS_Ref_Transactions_Foyers` t
        LEFT JOIN `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Client_Prime`  p
        ON t.hhd_num = p.hhd_num
            AND p.cal_mm <= DATE_TRUNC(DATE(t.datekey), MONTH)
        LEFT JOIN pass pa
        ON t.cardKey = pa.loy_card_key
            AND pa.crea_dt <= t.datekey
        LEFT JOIN `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_App_User` au
        ON t.hhd_num = au.hhd_num
            AND DATE_TRUNC(DATE(t.datekey), MONTH) = au.dateKey
        LEFT JOIN `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Abonnement_Cplus` cp
        ON t.hhd_num = cp.hhd_num
            AND DATE_TRUNC(DATE(t.datekey), MONTH) BETWEEN cp.SubscriptionStartDate AND cp.SubscriptionEndDate
        LEFT JOIN `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Segments` sg
            ON CAST(t.hhd_num AS INT64) = sg.hhdNum
            AND DATE_TRUNC(DATE(t.datekey), MONTH) = sg.monthUpdatedate

        WHERE 1=1 
            AND t.Enseigne!='6- AUTRES'
            AND (
                t.dateKey BETWEEN "2023-09-01" AND "2023-10-31"
                OR
                t.dateKey BETWEEN "2022-12-01" AND "2022-12-31"
                OR
                t.dateKey BETWEEN "2022-09-01" AND "2022-10-31"
                OR 
                t.dateKey BETWEEN "2021-12-01" AND "2021-12-31"
                ) 
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
    )
SELECT * FROM eqc_details 
;


-- Recompute
CREATE OR REPLACE TABLE `marketing-dev-237914.Dash_Equation_Commercial_Fidelite.DS_EQC_Agg` 
PARTITION BY  Date_Ref
CLUSTER BY Enseigne, Region, Magasin
OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 40 DAY)) 
AS 
WITH details AS(
  SELECT 
         Date_Mois
        ,CASE 
          WHEN Date_Mois BETWEEN DATE_TRUNC(CURRENT_DATE, YEAR) AND CURRENT_DATE THEN DATE_TRUNC(CURRENT_DATE, YEAR)
          WHEN Date_Mois BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE, YEAR), INTERVAL 1 YEAR) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR) THEN  DATE_SUB(DATE_TRUNC(CURRENT_DATE, YEAR), INTERVAL 1 YEAR)
         ELSE NULL
         END AS Date_CAD
        ,CASE 
          WHEN Date_Mois BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR) AND CURRENT_DATE THEN DATE_TRUNC(CURRENT_DATE, YEAR)
          WHEN Date_Mois BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 2 YEAR) AND DATE_SUB(CURRENT_DATE -1 , INTERVAL 1 YEAR) THEN  DATE_SUB(DATE_TRUNC(CURRENT_DATE, YEAR), INTERVAL 1 YEAR)
         ELSE NULL
         END AS Date_CAM
        ,Enseigne
        ,Region
        ,Magasin
        ,flagPrime
        ,flagAppUser 
        ,flagAbonnes
        ,flagPass
        ,Segment_consostyle
        ,Segment_lifestage
        ,Segment_mixeur
        ,Segment_mixeurEcom
        ,Segment_struct
        ,N_TRX_TOTAL
        ,CA_TOTAL
        ,ARRAY_PORTEURS
        ,ARRAY_LENGTH(ARRAY_PORTEURS) AS LENGTH_ARRAY_PORTEURS
        ,N_TRX
        ,CA
        ,CA_MDC
        ,CA_ALIM
        ,CA_ALIM_MDC
        ,Q
        ,Q_MDC
        ,Q_ALIM
        ,Q_ALIM_MDC
        ,RI
        ,RD
  FROM `marketing-dev-237914.Dash_Equation_Commercial_Fidelite.DS_EQC_Details`
  WHERE 1=1
  AND Enseigne!='6- AUTRES'
),
agg AS ( 
    SELECT 
         Date_Mois
        ,Date_CAD
        ,Date_CAM
        ,Enseigne
        ,Region
        ,Magasin
        ,flagPrime
        ,flagAppUser 
        ,flagAbonnes
        ,flagPass
        ,Segment_consostyle
        ,Segment_lifestage
        ,Segment_struct
        ,Segment_mixeur
        ,Segment_mixeurEcom
        ,SUM(N_TRX_TOTAL/LENGTH_ARRAY_PORTEURS) AS N_TRX_TOTAL
        ,SUM(CA_TOTAL/LENGTH_ARRAY_PORTEURS) AS CA_TOTAL

        ,SUM(N_TRX/LENGTH_ARRAY_PORTEURS) AS N_TRX
        
        ,APPROX_COUNT_DISTINCT(arr) AS N_PORTEURS

        ,SUM(CA/LENGTH_ARRAY_PORTEURS) AS CA
        ,SUM(CA_MDC/LENGTH_ARRAY_PORTEURS) AS CA_MDC
        ,SUM(CA_ALIM/LENGTH_ARRAY_PORTEURS) AS CA_ALIM
        ,SUM(CA_ALIM_MDC/LENGTH_ARRAY_PORTEURS) AS CA_ALIM_MDC

        ,SUM(Q/LENGTH_ARRAY_PORTEURS) AS Q
        ,SUM(Q_MDC/LENGTH_ARRAY_PORTEURS) AS Q_MDC
        ,SUM(Q_ALIM/LENGTH_ARRAY_PORTEURS) AS Q_ALIM
        ,SUM(Q_ALIM_MDC/LENGTH_ARRAY_PORTEURS) AS Q_ALIM_MDC
        
        ,SUM(RI/LENGTH_ARRAY_PORTEURS) AS RI
        ,SUM(RD/LENGTH_ARRAY_PORTEURS) AS RD

    FROM details, UNNEST(ARRAY_PORTEURS) arr
    GROUP BY GROUPING SETS(
        ROLLUP(Date_Mois, Enseigne, Region, Magasin, flagPrime),
        (Date_Mois, Enseigne, Region, Magasin, flagAppUser),
        (Date_Mois, Enseigne, Region, Magasin, flagPass),
        (Date_Mois, Enseigne, Region, Magasin, flagAbonnes),
        (Date_Mois, Enseigne, Region, Magasin, Segment_consostyle),
        (Date_Mois, Enseigne, Region, Magasin, Segment_lifestage),
        (Date_Mois, Enseigne, Region, Magasin, Segment_struct),
        (Date_Mois, Enseigne, Region, Magasin, Segment_mixeur),
        (Date_Mois, Enseigne, Region, Magasin, Segment_mixeurEcom),

        (Date_Mois, Enseigne, Region, flagPrime),
        (Date_Mois, Enseigne, Region, flagAppUser),
        (Date_Mois, Enseigne, Region, flagPass),
        (Date_Mois, Enseigne, Region, flagAbonnes),
        (Date_Mois, Enseigne, Region, Segment_consostyle),
        (Date_Mois, Enseigne, Region, Segment_lifestage),
        (Date_Mois, Enseigne, Region, Segment_struct),
        (Date_Mois, Enseigne, Region, Segment_mixeur),
        (Date_Mois, Enseigne, Region, Segment_mixeurEcom),

        (Date_Mois, Enseigne, flagPrime),
        (Date_Mois, Enseigne, flagAppUser),
        (Date_Mois, Enseigne, flagPass),
        (Date_Mois, Enseigne, flagAbonnes),
        (Date_Mois, Enseigne, Segment_consostyle),
        (Date_Mois, Enseigne, Segment_lifestage),
        (Date_Mois, Enseigne, Segment_struct),
        (Date_Mois, Enseigne, Segment_mixeur),
        (Date_Mois, Enseigne, Segment_mixeurEcom),

        (Date_Mois, flagPrime),
        (Date_Mois, flagAppUser),
        (Date_Mois, flagPass),
        (Date_Mois, flagAbonnes),
        (Date_Mois, Segment_consostyle),
        (Date_Mois, Segment_lifestage),
        (Date_Mois, Segment_struct),
        (Date_Mois, Segment_mixeur),
        (Date_Mois, Segment_mixeurEcom),

        ROLLUP(Date_CAD, Enseigne, Region, Magasin, flagPrime),
        (Date_CAD, Enseigne, Region, Magasin, flagAppUser),
        (Date_CAD, Enseigne, Region, Magasin, flagPass),
        (Date_CAD, Enseigne, Region, Magasin, flagAbonnes),
        (Date_CAD, Enseigne, Region, Magasin, Segment_consostyle),
        (Date_CAD, Enseigne, Region, Magasin, Segment_lifestage),
        (Date_CAD, Enseigne, Region, Magasin, Segment_struct),
        (Date_CAD, Enseigne, Region, Magasin, Segment_mixeur),
        (Date_CAD, Enseigne, Region, Magasin, Segment_mixeurEcom),

        (Date_CAD, Enseigne, Region, flagPrime),
        (Date_CAD, Enseigne, Region, flagAppUser),
        (Date_CAD, Enseigne, Region, flagPass),
        (Date_CAD, Enseigne, Region, flagAbonnes),
        (Date_CAD, Enseigne, Region, Segment_consostyle),
        (Date_CAD, Enseigne, Region, Segment_lifestage),
        (Date_CAD, Enseigne, Region, Segment_struct),
        (Date_CAD, Enseigne, Region, Segment_mixeur),
        (Date_CAD, Enseigne, Region, Segment_mixeurEcom),

        (Date_CAD, Enseigne, flagPrime),
        (Date_CAD, Enseigne, flagAppUser),
        (Date_CAD, Enseigne, flagPass),
        (Date_CAD, Enseigne, flagAbonnes),
        (Date_CAD, Enseigne, Segment_consostyle),
        (Date_CAD, Enseigne, Segment_lifestage),
        (Date_CAD, Enseigne, Segment_struct),
        (Date_CAD, Enseigne, Segment_mixeur),
        (Date_CAD, Enseigne, Segment_mixeurEcom),

        (Date_CAD, flagPrime),
        (Date_CAD, flagAppUser),
        (Date_CAD, flagPass),
        (Date_CAD, flagAbonnes),
        (Date_CAD, Segment_consostyle),
        (Date_CAD, Segment_lifestage),
        (Date_CAD, Segment_struct),
        (Date_CAD, Segment_mixeur),
        (Date_CAD, Segment_mixeurEcom),


        ROLLUP(Date_CAM, Enseigne, Region, Magasin, flagPrime),
        (Date_CAM, Enseigne, Region, Magasin, flagAppUser),
        (Date_CAM, Enseigne, Region, Magasin, flagPass),
        (Date_CAM, Enseigne, Region, Magasin, flagAbonnes),
        (Date_CAM, Enseigne, Region, Magasin, Segment_consostyle),
        (Date_CAM, Enseigne, Region, Magasin, Segment_lifestage),
        (Date_CAM, Enseigne, Region, Magasin, Segment_struct),
        (Date_CAM, Enseigne, Region, Magasin, Segment_mixeur),
        (Date_CAM, Enseigne, Region, Magasin, Segment_mixeurEcom),

        (Date_CAM, Enseigne, Region, flagPrime),
        (Date_CAM, Enseigne, Region, flagAppUser),
        (Date_CAM, Enseigne, Region, flagPass),
        (Date_CAM, Enseigne, Region, flagAbonnes),
        (Date_CAM, Enseigne, Region, Segment_consostyle),
        (Date_CAM, Enseigne, Region, Segment_lifestage),
        (Date_CAM, Enseigne, Region, Segment_struct),
        (Date_CAM, Enseigne, Region, Segment_mixeur),
        (Date_CAM, Enseigne, Region, Segment_mixeurEcom),

        (Date_CAM, Enseigne, flagPrime),
        (Date_CAM, Enseigne, flagAppUser),
        (Date_CAM, Enseigne, flagPass),
        (Date_CAM, Enseigne, flagAbonnes),
        (Date_CAM, Enseigne, Segment_consostyle),
        (Date_CAM, Enseigne, Segment_lifestage),
        (Date_CAM, Enseigne, Segment_struct),
        (Date_CAM, Enseigne, Segment_mixeur),
        (Date_CAM, Enseigne, Segment_mixeurEcom),

        (Date_CAM, flagPrime),
        (Date_CAM, flagAppUser),
        (Date_CAM, flagPass),
        (Date_CAM, flagAbonnes),
        (Date_CAM, Segment_consostyle),
        (Date_CAM, Segment_lifestage),
        (Date_CAM, Segment_struct),
        (Date_CAM, Segment_mixeur),
        (Date_CAM, Segment_mixeurEcom)
  )
)
SELECT 
     COALESCE(FORMAT_DATE('M-%m', Date_Mois), CASE WHEN Date_CAD IS NOT NULL THEN 'CAD' END, CASE WHEN Date_CAM IS NOT NULL THEN 'CAM' END) AS Periode_desc
    ,COALESCE(Date_Mois, Date_CAD, Date_CAM) AS Date_Ref 
    ,COALESCE(Enseigne, '0- GROUPE') AS Enseigne
    ,COALESCE(Region, 'TOUTES') AS Region
    ,COALESCE(Magasin, 'TOUS') AS Magasin
    ,COALESCE(
      --Flag
       CASE WHEN flagPrime=1 THEN 'Prime' END
      ,CASE WHEN flagAppUser=1 THEN 'AppUser' END
      ,CASE WHEN flagPass=1 THEN 'Pass' END
      ,CASE WHEN flagAbonnes=1 THEN 'Abonnes' END

      --Segments
      ,CASE WHEN Segment_mixeur=1 THEN 'Mixeur-Oui' END
      ,CASE WHEN Segment_mixeur=0 THEN 'Mixeur-Non' END
      ,CASE WHEN Segment_mixeurEcom=1 THEN 'Mixeur-Ecom-Oui' END
      ,CASE WHEN Segment_mixeurEcom=0 THEN 'Mixeur-Ecom-Non' END

      ,CASE WHEN Segment_consostyle='Nutrition' THEN 'Consostyle-Nutrition' END
      ,CASE WHEN Segment_consostyle='Gourmet' THEN 'Consostyle-Gourmet' END
      ,CASE WHEN Segment_consostyle='Budget' THEN 'Consostyle-Budget' END
      ,CASE WHEN Segment_consostyle='Bons plans' THEN 'Consostyle-BonPlans' END
      ,CASE WHEN Segment_consostyle='Pratique' THEN 'Consostyle-Pratique' END
      ,CASE WHEN Segment_consostyle='Aucun' THEN 'Consostyle-Aucun' END

      ,CASE WHEN Segment_lifestage='Familles -5ans' THEN 'Lifestage-Familles -5ans' END
      ,CASE WHEN Segment_lifestage='Familles +5ans' THEN 'Lifestage-Familles +5ans' END
      ,CASE WHEN Segment_lifestage='Sans enfants -65ans' THEN 'Lifestage-Sans enfants -65ans' END
      ,CASE WHEN Segment_lifestage='Sans enfants +65ans' THEN 'Lifestage-Sans enfants +65ans' END
      ,CASE WHEN Segment_lifestage='Aucun' THEN 'Lifestage-Aucun' END

      ,CASE WHEN Segment_struct='Bi-Mensuels' THEN 'Struct-Bi-Mensuels' END
      ,CASE WHEN Segment_struct='Hebdomadaires' THEN 'Struct-Hebdomadaires' END
      ,CASE WHEN Segment_struct='Nouveaux-Réactivés' THEN 'Struct-Nouveaux-Réactivés' END
      ,CASE WHEN Segment_struct='Irréguliers' THEN 'Struct-Irréguliers' END
      ,CASE WHEN Segment_struct='Aucun' THEN 'Struct-Aucun' END
      ,'TOUS'
    ) AS Clients
    ,N_TRX_TOTAL
    ,CA_TOTAL
    ,N_TRX
    ,N_PORTEURS
    ,CA
    ,CA_MDC
    ,CA_ALIM
    ,CA_ALIM_MDC
    ,Q
    ,Q_MDC
    ,Q_ALIM
    ,Q_ALIM_MDC
    ,RI
    ,RD
FROM agg
WHERE 1=1
    AND COALESCE(flagPrime, 2)!=0 
    AND COALESCE(flagAppUser, 2)!=0
    AND COALESCE(flagPass, 2)!=0 
    AND COALESCE(flagAbonnes, 2)!=0
;



CREATE OR REPLACE TABLE `marketing-dev-237914.Dash_Equation_Commercial_Fidelite.DS_EQC_Looker` 
PARTITION BY  Date_Ref
CLUSTER BY Enseigne, Clients
OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 40 DAY)) 
AS 
WITH 
    pivot AS (
        SELECT * 
        FROM (
            SELECT
                Periode_Desc
                ,Date_Ref
                ,Enseigne
                ,Region
                ,Magasin
                ,Clients
                ,N_TRX_TOTAL
                ,CA_TOTAL
                ,N_TRX
                ,N_PORTEURS
                ,CA
                ,CA_MDC
                ,CA_ALIM
                ,CA_ALIM_MDC
                ,Q
                ,Q_MDC
                ,Q_ALIM
                ,Q_ALIM_MDC
                ,RI
                ,RD
                ,CASE WHEN Date_Ref < DATE_TRUNC(CURRENT_DATE, YEAR) THEN 'N_1' ELSE 'N' END AS Pivot_Col
            FROM `marketing-dev-237914.Dash_Equation_Commercial_Fidelite.DS_EQC_Agg` 
            WHERE Periode_desc IS NOT NULL
        )
        PIVOT(
            MAX(Date_Ref) AS Date_Ref
            ,MAX(N_TRX_TOTAL) AS N_TRX_TOTAL
            ,MAX(CA_TOTAL) AS CA_TOTAL
            ,MAX(N_TRX) AS N_TRX
            ,MAX(N_PORTEURS) AS N_PORTEURS
            ,MAX(CA) AS CA
            ,MAX(CA_MDC) AS CA_MDC
            ,MAX(CA_ALIM) AS CA_ALIM
            ,MAX(CA_ALIM_MDC) AS CA_ALIM_MDC
            ,MAX(Q) AS Q
            ,MAX(Q_MDC) AS Q_MDC
            ,MAX(Q_ALIM) AS Q_ALIM
            ,MAX(Q_ALIM_MDC) AS Q_ALIM_MDC
            ,MAX(RI) AS RI
            ,MAX(RD) AS RD
            FOR Pivot_Col IN ('N_1', 'N')
        )
    ),
    metriques AS (
        SELECT 
            Periode_Desc
            ,COALESCE(Date_Ref_N, Date_Ref_N_1)  AS Date_Ref
            ,Enseigne
            ,Region
            ,Magasin
            ,Clients
        
            -- TOTAL
            ,COALESCE(CAST(CA_TOTAL_N AS INT64),0) AS CA_TOTAL_N
            ,COALESCE(CAST(CA_TOTAL_N_1 AS INT64),0) AS CA_TOTAL_N_1
        
            ,COALESCE(CAST(N_TRX_TOTAL_N AS INT64),0) AS N_TRX_TOTAL_N
            ,COALESCE(CAST(N_TRX_TOTAL_N_1 AS INT64),0) AS N_TRX_TOTAL_N_1

            -- TRANSACTIONS PORTEURS
            ,COALESCE(CAST(N_TRX_N AS INT64),0) AS N_TRX_N
            ,COALESCE(CAST(N_TRX_N_1 AS INT64),0) AS N_TRX_N_1
        
            -- CA PORTEURS
            ,COALESCE(CAST(CA_N AS INT64),0) AS CA_N
            ,COALESCE(CAST(CA_N_1 AS INT64),0) AS CA_N_1

            ,COALESCE(CAST(CA_MDC_N AS INT64),0) AS CA_MDC_N
            ,COALESCE(CAST(CA_MDC_N_1 AS INT64),0) AS CA_MDC_N_1

            ,COALESCE(CAST(CA_ALIM_N AS INT64),0) AS CA_ALIM_N
            ,COALESCE(CAST(CA_ALIM_N_1 AS INT64),0) AS CA_ALIM_N_1

            ,COALESCE(CAST(CA_ALIM_MDC_N AS INT64),0) AS CA_ALIM_MDC_N
            ,COALESCE(CAST(CA_ALIM_MDC_N_1 AS INT64),0) AS CA_ALIM_MDC_N_1

            ,COALESCE(ROUND(SAFE_DIVIDE(CA_N, CA_TOTAL_N), 2),0.00) AS POIDS_CA_N
            ,COALESCE(ROUND(SAFE_DIVIDE(CA_N_1, CA_TOTAL_N_1), 2),0.00) AS POIDS_CA_N_1

            ,COALESCE(ROUND(SAFE_DIVIDE(CA_MDC_N, CA_N), 2),0.00) AS POIDS_CA_MDC_N
            ,COALESCE(ROUND(SAFE_DIVIDE(CA_MDC_N_1, CA_N_1), 2),0.00) AS POIDS_CA_MDC_N_1 

            ,COALESCE(ROUND(SAFE_DIVIDE(CA_ALIM_MDC_N, CA_ALIM_N), 2),0.00) AS POIDS_CA_ALIM_MDC_N
            ,COALESCE(ROUND(SAFE_DIVIDE(CA_ALIM_MDC_N_1, CA_ALIM_N_1), 2),0.00) AS POIDS_CA_ALIM_MDC_N_1 

            -- QUANTITE
            ,COALESCE(CAST(Q_N AS INT64),0) AS Q_N
            ,COALESCE(CAST(Q_N_1 AS INT64),0) AS Q_N_1

            ,COALESCE(CAST(Q_MDC_N AS INT64),0) AS Q_MDC_N
            ,COALESCE(CAST(Q_MDC_N_1 AS INT64),0) AS Q_MDC_N_1

            ,COALESCE(CAST(Q_ALIM_N AS INT64),0) AS Q_ALIM_N
            ,COALESCE(CAST(Q_ALIM_N_1 AS INT64),0) AS Q_ALIM_N_1

            ,COALESCE(CAST(Q_ALIM_MDC_N AS INT64),0) AS Q_ALIM_MDC_N
            ,COALESCE(CAST(Q_ALIM_MDC_N_1 AS INT64),0) AS Q_ALIM_MDC_N_1

            -- QUANTITE / PASSAGE EN CAISSE
            ,COALESCE(ROUND(SAFE_DIVIDE(Q_N, N_TRX_N),2),0) AS QM_N
            ,COALESCE(ROUND(SAFE_DIVIDE(Q_N_1, N_TRX_N_1),2),0) AS QM_N_1

            ,COALESCE(ROUND(SAFE_DIVIDE(Q_MDC_N, N_TRX_N),2),0) AS QM_MDC_N
            ,COALESCE(ROUND(SAFE_DIVIDE(Q_MDC_N_1, N_TRX_N_1),2),0) AS QM_MDC_N_1
            
            ,COALESCE(ROUND(SAFE_DIVIDE(Q_ALIM_N, N_TRX_N),2),0) AS QM_ALIM_N
            ,COALESCE(ROUND(SAFE_DIVIDE(Q_ALIM_N_1, N_TRX_N_1),2),0) AS QM_ALIM_N_1

            -- PANIER MOYEN
            ,COALESCE(ROUND(SAFE_DIVIDE(CA_N, N_TRX_N),2),0.00) AS PM_N
            ,COALESCE(ROUND(SAFE_DIVIDE(CA_N_1, N_TRX_N_1),2),0.00) AS PM_N_1

            ,COALESCE(ROUND(SAFE_DIVIDE(CA_MDC_N, N_TRX_N),2),0.00) AS PM_MDC_N
            ,COALESCE(ROUND(SAFE_DIVIDE(CA_MDC_N_1, N_TRX_N_1),2),0.00) AS PM_MDC_N_1

            ,COALESCE(ROUND(SAFE_DIVIDE(CA_ALIM_N, N_TRX_N),2),0.00) AS PM_ALIM_N
            ,COALESCE(ROUND(SAFE_DIVIDE(CA_ALIM_N_1, N_TRX_N_1),2),0.00) AS PM_ALIM_N_1

            -- PRIX MOYEN / ARTICLES
            ,COALESCE(ROUND(SAFE_DIVIDE(CA_N, Q_N),2),0.00) AS CA_Q_N
            ,COALESCE(ROUND(SAFE_DIVIDE(CA_N_1, Q_N_1),2),0.00) AS CA_Q_N_1

            ,COALESCE(ROUND(SAFE_DIVIDE(CA_MDC_N, Q_MDC_N),2),0.00) AS CA_Q_MDC_N
            ,COALESCE(ROUND(SAFE_DIVIDE(CA_MDC_N_1, Q_MDC_N_1),2),0.00) AS CA_Q_MDC_N_1

            ,COALESCE(ROUND(SAFE_DIVIDE(CA_ALIM_N, Q_ALIM_N),2),0.00) AS CA_Q_ALIM_N
            ,COALESCE(ROUND(SAFE_DIVIDE(CA_ALIM_N_1, Q_ALIM_N_1),2),0.00) AS CA_Q_ALIM_N_1

            -- PORTEURS
            ,COALESCE(N_PORTEURS_N, 0) AS N_PORTEURS_N
            ,COALESCE(N_PORTEURS_N_1, 0) AS N_PORTEURS_N_1

            -- FREQUENCE
            ,COALESCE(ROUND(SAFE_DIVIDE(N_TRX_N, N_PORTEURS_N),2),0.00) AS FREQ_N
            ,COALESCE(ROUND(SAFE_DIVIDE(N_TRX_N_1, N_PORTEURS_N_1),2),0.00) AS FREQ_N_1

            --RD
            ,COALESCE(CAST(RD_N AS INT64),0) AS RD_N
            ,COALESCE(CAST(RD_N_1 AS INT64),0) AS RD_N_1
            ,COALESCE(ROUND(SAFE_DIVIDE(RD_N, CA_N),2),0.00) AS RDCA_N
            ,COALESCE(ROUND(SAFE_DIVIDE(RD_N_1, CA_N_1),2),0.00) AS RDCA_N_1

            -- RI
            ,COALESCE(CAST(RI_N AS INT64),0) AS RI_N
            ,COALESCE(CAST(RI_N_1 AS INT64),0) AS RI_N_1

        FROM pivot
    )
SELECT 
     *
    -- DEPENSE PERIODE
    ,COALESCE(ROUND(PM_N*FREQ_N, 2), 0.00) AS DP_N
    ,COALESCE(ROUND(PM_N_1*FREQ_N_1, 2), 0.00) AS DP_N_1
    
    -- EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(N_TRX_N-N_TRX_N_1, N_TRX_N_1), 2),0.00) AS N_TRX_EVOL

    ,COALESCE(ROUND(SAFE_DIVIDE(CA_N-CA_N_1, CA_N_1), 2),0.00) AS CA_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(CA_MDC_N-CA_MDC_N_1, CA_MDC_N_1), 2),0.00) AS CA_MDC_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(CA_ALIM_N-CA_ALIM_N_1, CA_ALIM_N_1), 2),0.00) AS CA_ALIM_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(POIDS_CA_N-POIDS_CA_N_1, POIDS_CA_N_1), 2),0.00) AS POIDS_CA_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(POIDS_CA_MDC_N-POIDS_CA_MDC_N_1, POIDS_CA_MDC_N_1), 2),0.00) AS POIDS_CA_MDC_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(POIDS_CA_ALIM_MDC_N-POIDS_CA_ALIM_MDC_N_1, POIDS_CA_ALIM_MDC_N_1), 2),0.00) AS POIDS_CA_ALIM_MDC_EVOL

    ,COALESCE(ROUND(SAFE_DIVIDE(Q_N-Q_N_1, Q_N_1), 2),0.00) AS Q_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(Q_MDC_N-Q_MDC_N_1, Q_MDC_N_1), 2),0.00) AS Q_MDC_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(Q_ALIM_N-Q_ALIM_N_1, Q_ALIM_N_1), 2),0.00) AS Q_ALIM_EVOL

    ,COALESCE(ROUND(SAFE_DIVIDE(QM_N-QM_N_1, QM_N_1), 2),0.00) AS QM_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(QM_MDC_N-QM_MDC_N_1, QM_MDC_N_1), 2),0.00) AS QM_MDC_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(QM_ALIM_N-QM_ALIM_N_1, QM_ALIM_N_1), 2),0.00) AS QM_ALIM_EVOL

    ,COALESCE(ROUND(SAFE_DIVIDE(PM_N-PM_N_1, PM_N_1), 2),0.00) AS PM_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(PM_MDC_N-PM_MDC_N_1, PM_MDC_N_1), 2),0.00) AS PM_MDC_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(PM_ALIM_N-PM_ALIM_N_1, PM_ALIM_N_1), 2),0.00) AS PM_ALIM_EVOL

    ,COALESCE(ROUND(SAFE_DIVIDE(CA_Q_N-CA_Q_N_1, CA_Q_N_1), 2),0.00) AS CA_Q_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(CA_Q_MDC_N-CA_Q_MDC_N_1, CA_Q_MDC_N_1), 2),0.00) AS CA_Q_MDC_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(CA_Q_ALIM_N-CA_Q_ALIM_N_1, CA_Q_ALIM_N_1), 2),0.00) AS CA_Q_ALIM_EVOL

    ,COALESCE(ROUND(SAFE_DIVIDE(N_PORTEURS_N-N_PORTEURS_N_1, N_PORTEURS_N_1), 2),0.00) AS N_PORTEURS_EVOL
    ,COALESCE(ROUND(SAFE_DIVIDE(FREQ_N-FREQ_N_1, FREQ_N_1), 2),0.00) AS FREQ_EVOL

    ,COALESCE(ROUND(SAFE_DIVIDE(RDCA_N-RDCA_N_1, RDCA_N_1), 2),0.00) AS RDCA_EVOL

    ,COALESCE(ROUND(SAFE_DIVIDE(ROUND(PM_N*FREQ_N, 2)-ROUND(PM_N_1*FREQ_N_1, 2), ROUND(PM_N_1*FREQ_N_1, 2)), 2),0.00) AS DP_EVOL
FROM metriques
;
