# Sears-SQL
--For Sears stores (A,B,C,D,M), for Apparel, for 9/1/2017, please provide:
/*
Query 1 fields:
store #, store city, member id, BU #, BU desc, Div #, Div desc, Line #, Line desc, Item #, item desc, sales, margin, trips, units
*/

/*
Query 2 fields:
Query 1 + outer join to bring in size information
*/

/*
Query 3 fields:
Query 2 + outer join to bring in color information
*/

SELECT 
a.OrignlFcltyNbr AS Store#
,fty_cty_nm AS Store_City 
,LyltyCardNbr AS member_id 
,Soar_no AS BU
,Soar_nm AS BU_Desc
,div_no AS Div#
,div_nm AS Div_Desc
,ln_no Line#
,ln_ds AS line_desc
,a.SrsItmNbr AS Item#
,prd_ds AS item_dsc
,SKU_SZ_DS
,SKU_COL_DS
,SUM(NetSellAmt) AS sales
,SUM(SlsMrgnAmt) AS margin 
,COUNT(DISTINCT LyltyCardNbr||TranDt) AS trips
,SUM(UnitQty) UNITS 
FROM L2_EnterpriseAnalytics.POSTranDtl A 
LEFT JOIN (SELECT TRIM(prd_irl_no)||TRIM(CASE WHEN LENGTH(TRIM(sku_no)) IN (1) THEN '00'||TRIM(SKU_NO) 
                                              WHEN LENGTH(TRIM(sku_no)) IN (2) THEN '0'||TRIM(SKU_NO)  
                                              ELSE SKU_No END) sku_id 
	   , SKU_NO,SKU_SZ_DS,SKU_COL_DS,SKU_DS FROM   lci_dw_views.sprs_product_sku )D    ----- For size and color table
ON TRIM(a.skuid)=D.sku_id
LEFT JOIN cbr_mart_tbls.sywr_srs_soar_bu B ON a.ProdIrlNbr=b.prd_irl_no
LEFT JOIN crm_mart1_tbls.crm_sears_retail_fty C ON a.OrignlFcltyNbr=C.fty_id_no
WHERE TranDt='2017-09-01' -- date to be changed according to the reqiurement
AND a.FmtSbtyp IN ('A','B','C','D','M')
AND b.soar_no  IN (101,102,103,104,105) --Apparel BU
AND lyltycardnbr IS NOT NULL
AND SrsKmtInd='S'
AND SrsDvsnNbr NOT IN (0,79)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13



/*
Query 4 fields:
store #, store city, Div #, Div desc, Item #, Item desc, units onhand
*/

SELECT 
 WK_NBR
,Store AS store#
,fty_cty_nm AS Store_City 
,div_no AS Div#
,div_nm AS Div_Desc
,itm_no AS Item#
,prd_ds AS item_dsc
,SUM(Inventory_in_hand) Units_on_hand
FROM
(
Select  
a.WK_NBR
,cast(a.locn_nbr as INT) as Store
,b.Prd_Irl_Nbr as Prd_Irl_No
,SUM(a.TTL_UN_QT) as Inventory_in_hand
from 
ALEX_ARP_VIEWS_PRD.FACT_SRS_WKLY_OPR_INS a
INNER JOIN 
ALEX_ARP_VIEWS_PRD.LU_SRS_PRODUCT_SKU b 
ON a.SKU_ID = b.SKU_ID
Where a.INS_TYP_CD in ('H') and a.INS_SUB_TYP_CD in ('A')  --filters to include only product considered On Hand and Available in a LOCN_NBR
group by 1,2,3) A 
LEFT  JOIN CBR_MART_TBLS.SYWR_SRS_SOAR_BU B on a.Prd_Irl_No = b.Prd_Irl_No
LEFT  JOIN crm_mart1_tbls.crm_sears_retail_fty C ON a.Store=C.fty_id_no
WHERE b.soar_no IN (101,102,103,104,105) --Apparel BU
GROUP BY 1,2,3,4,5,6
