
with period_sales as (
  select	a11.SKU_ID  SKU_ID,
    		  max(a14.SKU_DESC)  SKU_DESC,
    		  max(a14.DIV_NBR)  DIV_NBR,
    		  max(a14.ITM_NBR)  ITM_NBR,
    		  max(a14.SKU_NBR)  SKU_NBR,
    		  a12.MTH_NBR  MTH_NBR,
    		  a11.LOCN_NBR  LOCN_NBR,
    		  max(a15.LOCN_NM)  LOCN_NM,
    		  sum( CASE WHEN a11.SLS_TYP_CD not in ('M') THEN a11.TRS_SLL_DLR*a11.TY_CTR  end)  SEARSTOTALSALES,
    		  sum( CASE WHEN a11.SLS_TYP_CD not in ('M') THEN a11.TRS_UN_QT*a11.TY_CTR  end)  SEARSTOTALSALESUNITS,
    		  sum( CASE WHEN a11.SLS_TYP_CD not in ('M') THEN a11.TRS_CST_DLR*a11.TY_CTR  end)  SEARSCOSTOFMDSESOLD
    	from	`syw-analytics-repo-prod.alex_arp_views_prd.fact_srs_wkly_opr_sls_tyly`		    a11
          join        `syw-analytics-repo-prod.alex_arp_views_prd.lu_shc_weeks`           a12
            on         (a11.WK_NBR = a12.WK_NBR)
          join        `syw-analytics-repo-prod.alex_arp_views_prd.lu_shc_cds_gob_locn`    a13
            on         (a11.LOCN_NBR = a13.LOCN_NBR)
          join        `syw-analytics-repo-prod.alex_arp_views_prd.lu_srs_product_sku`     a14
            on         (a11.SKU_ID = a14.SKU_ID)
          join       `syw-analytics-repo-prod.alex_arp_views_prd.lu_shc_location`         a15
            on         (a11.LOCN_NBR = a15.LOCN_NBR)
    	where	(a15.STR_OPER_CD in ('1')
    	      and a13.GOB_IND in ('N')
    	      and a14.GRO_NBR in (803)
    	      and a12.MTH_NBR in (201809, 201810)
    	      and a11.TRS_TYP_CD in ('A', 'R', 'S')
    	      and a11.TYLY_DESC in ('TY'))
    	group by	a11.SKU_ID,
    		        a12.MTH_NBR,
    		        a11.LOCN_NBR
