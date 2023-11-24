DECLARE year_date DATE DEFAULT DATE_TRUNC(CURRENT_DATE, YEAR) ; 
DECLARE year_N1_date DATE DEFAULT DATE_SUB(year_date, INTERVAL 1 YEAR) ; 
DECLARE year_N2_date DATE DEFAULT DATE_SUB(year_date, INTERVAL 2 YEAR) ; 
DECLARE last_month_start_date DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 2 MONTH) ; 
DECLARE last_month_start_N1_date DATE DEFAULT DATE_SUB(last_month_start_date, INTERVAL 1 YEAR) ; 
DECLARE last_month_start_N2_date DATE DEFAULT DATE_SUB(last_month_start_date, INTERVAL 2 YEAR) ; 
DECLARE last_month_end_date DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 DAY) ; 
DECLARE last_month_end_N1_date DATE DEFAULT DATE_SUB(last_month_end_date, INTERVAL 1 YEAR) ; 
DECLARE last_month_end_N2_date DATE DEFAULT DATE_SUB(last_month_end_date, INTERVAL 2 YEAR) ; 

CREATE OR REPLACE FUNCTION Dash_Equation_Commercial_Fidelite.ARRAY_DISTINCT(value ANY TYPE) AS ((
  SELECT ARRAY_AGG(a.b)
  FROM (SELECT DISTINCT * FROM UNNEST(value) b) a
));

CREATE OR REPLACE TABLE `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Magasin`
OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 40 DAY)) 
AS
SELECT 
    CASE 
        WHEN FORMAT LIKE '%HYPERMARCHES%' THEN '1- HYPER'
        WHEN FORMAT LIKE '%SUPERMARCHES%' THEN '2- MARKET'
        WHEN FORMAT LIKE '%PROXIMITE%' THEN '3- PROXI'
    ELSE '4- DRIVE'
    END AS Enseigne
    ,locationName AS Magasin
    ,organisation.satelliteDesc AS Bassin
    ,organisation.regionDesc AS Region
    ,locationKey,stoIntFlag,stoIntDesc 
    ,stoSimGrpdesc AS Type_magasin --urbain rural
    ,locationType.locationTypeKey
    ,locationType.locationTypeCode AS chainTypeDesc
    ,formatKey ,FORMAT ,bannerKey ,banner
    ,financialManagement.aclGrpDesc
    ,MAX(CASE WHEN a.additionalPartyIdentificationTypeCode  ='ANABEL' THEN a.additionalPartyIdentification END) AS Anabel
    ,MAX(CASE WHEN a.additionalPartyIdentificationTypeCode  ='GLN' THEN a.additionalPartyIdentification END) AS StoEan
    ,MAX(CASE WHEN a.additionalPartyIdentificationTypeCode  ='PPSF' THEN a.additionalPartyIdentification END) AS StoFinKey
    ,MAX(CASE WHEN a.additionalPartyIdentificationTypeCode  ='WLEC' THEN a.additionalPartyIdentification END) AS SITEKEY
    ,MAX(CASE WHEN a.additionalPartyIdentificationTypeCode  ='SYCRON_CODE' THEN a.additionalPartyIdentification END) AS syc_key
FROM `fr-darwin-prd.sites_referential.bv_location`  lns
    ,UNNEST( additionalPartyIdentifications) a
    ,UNNEST( organisation) organisation
    ,UNNEST( financialManagement) financialManagement
WHERE locationKey NOT IN ('15532','15184','14291','14293','4291','14289','14290','14292','14839') -- exclusion magasins : sites jouets de noel, rentrée des classes, FAVs
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
;

CREATE OR REPLACE TABLE `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Cartes_Foyers`
OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 40 DAY)) 
AS 
SELECT DISTINCT
    a.loyaltyCardKey AS idLoyCard
    ,a.loyaltyAccountKey -- new
    ,c.householdKey AS hhd_num -- id foyer
FROM `fr-darwin-prd.customers_referential.bv_loyalty_card` a
INNER JOIN `fr-darwin-prd.customers_referential.bv_loyalty_account` b
    ON a.loyaltyAccountKey = b.loyaltyAccountKey
INNER JOIN `fr-darwin-prd.customers_referential.bv_individual_household` c
    ON b.individualKey = c.individualKey
WHERE 1=1
    AND b.loyaltyAccountTestFlag IS FALSE
    AND c.recordSource = '8'
;

CREATE OR REPLACE TABLE `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Abonnement_Cplus`
OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 40 DAY)) 
AS 
SELECT DISTINCT
    b.hhd_num
    ,date(a.individualSubscriptionStartDatetime) AS SubscriptionStartDate
    ,date(a.individualSubscriptionEndDatetime) AS SubscriptionEndDate
FROM `fr-darwin-prd.customers_referential.bv_individual_subscription` a
LEFT JOIN `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Cartes_Foyers` b 
    ON a.individualSubscriptionChannelValue = b.loyaltyAccountKey
WHERE 1=1
    AND individualSubscriptionTypeCode LIKE "CPLUS%"
    AND individualSubscriptionActiveFlag=True
    AND date(individualSubscriptionStartDatetime) BETWEEN date_sub(DATE_TRUNC(DATE(current_date()), MONTH), interval 25 month)
    AND (SELECT cal_end_month FROM `marketing-dev-237914.Dash_Equation_Commercial_Fidelite.DS_ProgFid_Date`)
;

CREATE OR REPLACE TABLE `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_App_User` 
OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 40 DAY)) 
AS 
WITH 
    appOne AS ( --identification du lien entre individualKey (comptes Carrefour One) et hhd_num (numeros de foyer Fid)
        SELECT 
            a.individualKey
            ,b.loyaltyCardKey
            ,e.householdKey AS hhd_num
            ,d.loyaltyAccountTestFlag
        FROM `fr-darwin-prd.customers_referential.bv_individual` AS a
        LEFT JOIN `fr-darwin-prd.customers_referential.bv_individual_loyalty_card_link` AS b
            ON a.individualKey = b.individualKey
                AND a.recordSource = b.recordSource
                AND b.defaultFlag = true
        LEFT JOIN `fr-darwin-prd.customers_referential.bv_loyalty_card` AS c
            ON b.loyaltyCardKey = c.loyaltyCardKey
        LEFT JOIN `fr-darwin-prd.customers_referential.bv_loyalty_account` AS d
            ON c.loyaltyAccountKey = d.loyaltyAccountKey
        LEFT JOIN `fr-darwin-prd.customers_referential.bv_individual_household` AS e
        ON d.individualKey = e.individualKey
        WHERE a.recordSource = '0'
    ),
    linkBetweenAppOneAndApp AS ( -- lien entre appUserKey et individualKey (comptes Carrefour One)
        SELECT DISTINCT 
            appUserKey
            ,individualKey 
        FROM `carrefour-170216.darwin.bv_app`
        WHERE appUserStatus = 'identified'
    )
-- identification des clients ayant ouverts l'app sur la periode
SELECT c.hhd_num, a.dateKey
FROM (
    SELECT 
        appUserKey
        ,DATE_TRUNC(DATE(date), MONTH) AS dateKey, 
        MAX(openAppFlag) AS ouvertureApp
    FROM `carrefour-170216.darwin.bv_app_business_event_agg`
    WHERE 1=1 
        AND date BETWEEN  last_month_end_N2_date AND last_month_end_date
        AND appUserStatus = 'identified'
    GROUP BY 1,2
    HAVING ouvertureApp = 1
    ) a
INNER JOIN linkBetweenAppOneAndApp AS b 
    ON a.appUserKey = b.appUserKey
INNER JOIN appOne AS c 
    ON b.individualKey = c.individualKey
WHERE hhd_num is not null -- deleting users id not linked to household id
;

CREATE OR REPLACE TABLE `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Segments`
PARTITION BY  monthUpdatedate
OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 40 DAY)) 
AS
WITH 
    ctx_segments AS(
        SELECT 
            CAST(householdId  AS int64) AS hhdNum
            ,segmentId
            ,monthUpdatedate
            ,MAX(CASE WHEN context.contextCode = 'SCORE_02' THEN CAST(context.contextValue AS float64) ELSE 0 END) AS SCORE_02
            ,MAX(CASE WHEN context.contextCode = 'SCORE_35' THEN CAST(context.contextValue AS float64) ELSE 0 END) AS SCORE_35
        FROM `fr-darwin-prd.customers_segmentation.bv_household_segment_context`,UNNEST(segmentContextInfomationList) context
        WHERE monthUpdatedate >= "2021-10-01"
        GROUP BY 1, 2, 3
    ),
    segments_histo AS (
        SELECT 
            CAST(bvhs.householdId  AS int64) AS hhdNum
            ,bvhs.monthUpdatedate
            ,CASE  
                WHEN bvhs.SegmentationCode='FOOD_MLT' THEN 'consostyle' 
                WHEN bvhs.SegmentationCode='MIX_MLT' THEN 'mixeur' 
                WHEN bvhs.SegmentationCode='MIX_ECOM' THEN 'mixeurEcom' 
                WHEN bvhs.SegmentationCode='STRUCT_MLT' THEN 'struct' 
                WHEN bvhs.SegmentationCode='LSCRF_MLT' THEN 'lifestage' 
            END AS Type
            ,MAX (
                CASE 
                    -- consostyle
                    WHEN bvhs.SegmentationCode='FOOD_MLT' AND bvs.segmentName LIKE 'Bons plans%' THEN 'Bons plans'
                    WHEN bvhs.SegmentationCode='FOOD_MLT' AND bvs.segmentName LIKE 'Gourmet%' THEN 'Gourmet'
                    WHEN bvhs.SegmentationCode='FOOD_MLT' AND bvs.segmentName LIKE 'Budget%' THEN 'Budget'
                    WHEN bvhs.SegmentationCode='FOOD_MLT' AND bvs.segmentName LIKE 'Nutrition%' THEN 'Nutrition'
                    WHEN bvhs.SegmentationCode='FOOD_MLT' AND bvs.segmentName LIKE 'Pratique%' THEN 'Pratique'
                    -- mixeur
                    WHEN bvhs.SegmentationCode='MIX_MLT' THEN bvs.segmentName 
                    -- mixeur ecom
                    WHEN bvhs.SegmentationCode='MIX_ECOM' THEN bvs.segmentName 
                    -- struct 
                    WHEN bvhs.SegmentationCode='STRUCT_MLT' AND bvs.segmentCode IN ('1','2','3','4') THEN 'Nouveaux-Réactivés'
                    WHEN bvhs.SegmentationCode='STRUCT_MLT' AND bvs.segmentCode in ('5') THEN  'Irréguliers'
                    WHEN bvhs.SegmentationCode='STRUCT_MLT' AND bvs.segmentCode in ('6') THEN  'Mensuels'
                    WHEN bvhs.SegmentationCode='STRUCT_MLT' AND bvs.segmentCode in ('7') THEN  'Bi-Mensuels'
                    WHEN bvhs.SegmentationCode='STRUCT_MLT' AND bvs.segmentCode in ('8') THEN  'Hebdomadaires'
                    -- lifestage 
                    WHEN bvhs.SegmentationCode='LSCRF_MLT' AND bvs.segmentCode = '1' AND (ctx.SCORE_02 > 0 OR ctx.SCORE_35 > 0) THEN '1. Familles -5ans'
                    WHEN bvhs.SegmentationCode='LSCRF_MLT' AND bvs.segmentCode = '1' AND ctx.SCORE_02 = 0 AND ctx.SCORE_35 = 0 THEN '2. Familles +5ans'
                        WHEN bvhs.SegmentationCode='LSCRF_MLT' AND bvs.segmentCode in ('2', '3', '4') THEN '3. Sans enfants -65ans'
                    WHEN bvhs.SegmentationCode='LSCRF_MLT' AND bvs.segmentCode = '5' THEN '4. Sans enfants +65ans' 
                END
            ) AS Segment
        FROM `fr-darwin-prd.customers_segmentation.bv_household_segment` bvhs
        INNER JOIN `fr-darwin-prd.customers_segmentation.bv_segment` bvs
            ON bvhs.segmentId = bvs.segmentId
        LEFT JOIN ctx_segments ctx
            ON CAST(bvhs.householdId  AS int64) = ctx.hhdNum
                AND bvhs.segmentId = ctx.segmentId
                AND bvhs.monthUpdatedate = ctx.monthUpdatedate
        WHERE 1=1 
            AND bvhs.SegmentationCode IN ('FOOD_MLT', 'MIX_MLT', 'MIX_ECOM', 'STRUCT_MLT', 'LSCRF_MLT')
            AND bvhs.monthUpdatedate >= "2021-10-01"
        GROUP BY 1, 2, 3
    ),
    segments_pivot AS (
        SELECT * 
        FROM segments_histo
        PIVOT (
            MAX(Segment) AS Segment FOR Type IN('consostyle', 'mixeur', 'mixeurEcom', 'struct', 'lifestage')
        )
    )
SELECT  hhdNum
    ,monthUpdatedate
    ,CASE 
        WHEN Segment_consostyle IS NULL THEN LAST_VALUE(Segment_consostyle IGNORE NULLS) OVER (PARTITION BY hhdNum ORDER BY monthUpdatedate RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        ELSE Segment_consostyle 
    END AS Segment_consostyle
    ,CASE 
        WHEN Segment_mixeur IS NULL THEN LAST_VALUE(Segment_mixeur IGNORE NULLS) OVER (PARTITION BY hhdNum ORDER BY monthUpdatedate RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        ELSE Segment_mixeur 
    END AS Segment_mixeur
    ,CASE 
        WHEN Segment_mixeurEcom IS NULL THEN LAST_VALUE(Segment_mixeurEcom IGNORE NULLS) OVER (PARTITION BY hhdNum ORDER BY monthUpdatedate RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        ELSE Segment_mixeurEcom 
    END AS Segment_mixeurEcom
    ,CASE 
        WHEN Segment_struct IS NULL THEN LAST_VALUE(Segment_struct IGNORE NULLS) OVER (PARTITION BY hhdNum ORDER BY monthUpdatedate RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        ELSE Segment_struct 
    END AS Segment_struct
    ,CASE 
        WHEN Segment_lifestage IS NULL THEN LAST_VALUE(Segment_lifestage IGNORE NULLS) OVER (PARTITION BY hhdNum ORDER BY monthUpdatedate RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        ELSE Segment_lifestage 
    END AS Segment_lifestage
FROM segments_pivot
;


CREATE OR REPLACE TABLE `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Client_Prime` 
OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 40 DAY)) 
AS 
WITH 
    actifs_12m AS (
        SELECT 
            hhd_num
        FROM `fr-darwin-prd.customers_sale.bv_transactions` a
        INNER JOIN `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Cartes_Foyers` b 
            ON b.idLoyCard=a.cardKey
        WHERE 1=1
            AND cardFlag<>'0' 
            AND trxSalesFlag='1'
            AND datekey BETWEEN last_month_end_N1_date + 1 AND last_month_end_date
        GROUP BY 1
    )
SELECT DISTINCT
    b.hhd_num
    ,d.cal_mm
FROM `fr-darwin-prd.customers_referential.bv_loyalty_account_club` a
INNER JOIN `marketing-dev-237914.Dash_Equation_Commercial_Fidelite.DS_CAL_DAY` d 
    ON CAST(a.creationDatetime AS DATE) = d.cal_day
LEFT JOIN `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Cartes_Foyers` b 
    ON A.loyaltyCardKey = b.idLoyCard
INNER JOIN actifs_12m e 
    ON b.hhd_num = e.hhd_num
WHERE 1=1
    AND (a.deletionDatetime IS NULL OR CAST(a.deletionDatetime AS DATE) = '9999-12-31')
    AND a.clubName NOT IN ('CLUB_BEBE','CLUB_PETFOOD')
    AND CAST (a.creationDatetime AS DATE) BETWEEN '2019-02-11' AND last_month_end_date
QUALIFY ROW_NUMBER () OVER (PARTITION BY b.hhd_num ORDER BY d.cal_mm) = 1
;


CREATE OR REPLACE PROCEDURE Ref_Transformation_Marketing.P_UPDATE_Ref_Transactions(startDate DATE, endDate DATE)
OPTIONS(strict_mode=false) 
BEGIN
    DECLARE interval_start DATE ;
    DECLARE interval_end DATE ;
    SET interval_start=startDate ;
    SET interval_end=endDate ;

    CREATE TABLE IF NOT EXISTS `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Transactions_Foyers` (
         transactionId STRING
        ,dateKey DATE
        ,hhd_num STRING
        ,cardKey STRING
        ,Enseigne STRING
        ,Magasin STRING
        ,Region STRING
        ,stoEan STRING
        ,posId STRING
        ,posTrxId STRING
        ,CA NUMERIC
        ,Q NUMERIC
        ,CA_MDC NUMERIC
        ,Q_MDC NUMERIC
        ,CA_ALIM NUMERIC
        ,Q_ALIM NUMERIC
        ,CA_ALIM_MDC NUMERIC
        ,Q_ALIM_MDC NUMERIC
        ,RI NUMERIC
        ,RD NUMERIC  
    ) 
    PARTITION BY dateKey
    CLUSTER BY Enseigne, Region, Magasin 
    ;

    INSERT INTO `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Transactions_Foyers`
    (transactionId,dateKey,hhd_num,cardKey,Enseigne,Magasin,Region,stoEan,posId,posTrxId,CA,Q,CA_MDC,Q_MDC,CA_ALIM,Q_ALIM,CA_ALIM_MDC,Q_ALIM_MDC,RI,RD) 
    SELECT 
        saleTransactionKey AS transactionId
        ,a.creationDate AS dateKey
        ,hhd_num
        ,loyaltyCardKey AS cardKey
        ,CASE
            WHEN (a.pointOfSaleCode='99' OR (a.pointOfSaleCode='95' AND a.deliveryChainTypeKey='SUP') OR (a.pointOfSaleCode='1' AND (a.recordSource='VJ' OR a.deliveryChainTypeKey='ECM'))) AND a.businessServiceName !="CLCV" THEN '4- DRIVE'
            WHEN Enseigne='1- HYPER' AND pointOfSaleCode NOT IN ('199','106','92','91') AND a.businessServiceName !="CLCV" THEN '1- HYPER'
            WHEN Enseigne='2- MARKET' AND pointOfSaleCode NOT IN ('98','97','92','91','96','82','83') AND a.businessServiceName !="CLCV" THEN '2- MARKET'
            WHEN Enseigne='3- PROXI' AND pointOfSaleCode NOT IN ('97','92','91','96','82','83') AND a.businessServiceName !="CLCV" THEN '3- PROXI'
            WHEN a.businessServiceName='CLCV' THEN '5- CLCV'
            ELSE '6- AUTRES'
        END AS Enseigne
        ,b.Magasin
        ,b.Region
        ,a.deliveryPlaceFunctionGlnKey AS stoEan
        ,a.pointOfSaleCode AS posId
        ,SUBSTRING(a.saleTransactionOperationalKey, 4, 2) AS posTrxId
        
        ,SUM(l.saleAmountWithTax) AS CA
        ,SUM(CASE WHEN l.saleAmountWithTax<0 AND l.saleUnitQuantity>0 THEN -1*ABS(l.saleUnitQuantity) ELSE l.saleUnitQuantity END) AS Q
        
        ,SUM(CASE WHEN bv.brandtypekey='C' THEN l.saleAmountWithTax ELSE 0 END) AS CA_MDC
        ,SUM(CASE WHEN bv.brandtypekey='C' AND l.saleAmountWithTax<0 AND l.saleUnitQuantity>0 THEN -1*ABS(l.saleUnitQuantity) WHEN bv.brandtypekey='C' AND l.saleAmountWithTax>0 AND l.saleUnitQuantity>0 THEN l.saleUnitQuantity  ELSE 0 END) AS Q_MDC
        
        ,SUM(CASE WHEN bv.structHyp.hypSectorKey IN ('1', '2') AND bv.sectorKey IN ('CAP05','CAP04','CAP99','CAP02') THEN l.saleAmountWithTax ELSE 0 END)AS CA_ALIM
        ,SUM(CASE WHEN bv.structHyp.hypSectorKey IN ('1', '2') AND bv.sectorKey IN ('CAP05','CAP04','CAP99','CAP02') AND l.saleAmountWithTax<0 AND l.saleUnitQuantity>0 THEN -1*ABS(l.saleUnitQuantity) WHEN bv.structHyp.hypSectorKey IN ('1', '2') AND bv.sectorKey IN ('CAP05','CAP04','CAP99','CAP02') AND l.saleAmountWithTax>0 AND l.saleUnitQuantity>0 THEN l.saleUnitQuantity  ELSE 0 END) AS Q_ALIM
        
        ,SUM(CASE WHEN bv.structHyp.hypSectorKey IN ('1', '2') AND bv.sectorKey IN ('CAP05','CAP04','CAP99','CAP02') AND bv.brandtypekey='C' THEN l.saleAmountWithTax ELSE 0 END)AS CA_ALIM_MDC
        ,SUM(CASE WHEN bv.structHyp.hypSectorKey IN ('1', '2') AND bv.sectorKey IN ('CAP05','CAP04','CAP99','CAP02') AND bv.brandtypekey='C'  AND l.saleAmountWithTax<0 AND l.saleUnitQuantity>0 THEN -1*ABS(l.saleUnitQuantity) WHEN bv.structHyp.hypSectorKey IN ('1', '2') AND bv.sectorKey IN ('CAP05','CAP04','CAP99','CAP02') AND bv.brandtypekey='C' AND l.saleAmountWithTax>0 AND l.saleUnitQuantity>0 THEN l.saleUnitQuantity  ELSE 0 END) AS Q_ALIM_MDC
        
        ,SUM(l.immediateRewardAmountWithTax) AS RI
        ,SUM(l.deferredRewardAmountWithTax) AS RD
    FROM `fr-darwin-prd.customers_sale.bv_sale_transaction` a, UNNEST(saleTransactionLineList) l
    INNER JOIN `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Magasin` b
        ON a.deliveryPlaceFunctionGlnKey = b.stoEan
    LEFT JOIN `fr-darwin-prd.products_referential.bv_barcode` bv 
        ON bv.barcode=l.barcode
    LEFT JOIN `fr-darwin-prd.products_referential.bv_peripheral_activity` c 
        ON c.chainTypeKey=(CASE WHEN (a.pointOfSaleCode='99' OR (a.pointOfSaleCode='95' AND a.deliveryChainTypeKey='SUP') OR (a.pointOfSaleCode='1' AND (a.recordSource='VJ' OR a.deliveryChainTypeKey='ECM'))) AND a.recordSource IN ('VJ','TRD') THEN 'SUP' ELSE a.deliveryChainTypeKey end)
            AND c.departmentKey=l.departmentKey AND a.creationDate BETWEEN c.beginDate AND c.endDate
    LEFT JOIN `marketing-dev-237914.Ref_Transformation_Marketing.DS_Ref_Cartes_Foyers` d
        ON a.loyaltyCardKey = d.idloycard
    WHERE a.creationDate BETWEEN interval_start AND interval_end
        AND saleTransactionLineTypeCode ='N'
        AND c.peripheralActivityFlag IS FALSE
    GROUP BY 1,2,3,4,5,6,7,8,9,10
    ;
END
;



	
