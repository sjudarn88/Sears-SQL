with store_members_map as (
  select locnnbr, lyltycardnbr, SUM(UnitQty) as UNITS
    from `syw-analytics-repo-prod.l2_enterpriseanalytics.postrandtl` as a
      LEFT JOIN `syw-analytics-repo-prod.cbr_mart_tbls.sywr_srs_soar_bu` B 
      ON a.ProdIrlNbr=b.prd_irl_no
--where trandt between timestamp('2017-04-30') and timestamp('2017-12-30') -- 2017 fall winter in season
  WHERE TranDt >'2017-04-01' --week 14
    and TranDt <'2018-03-31' -- week 48 date to be changed according to the reqiurement
    AND FmtSbtyp IN ('A','B','C','D','M')
    AND lyltycardnbr IS NOT NULL  -- force member only sales
    AND SrsKmtInd='S'
    and trantypeind = 'S'
    AND SrsDvsnNbr NOT IN (0,79)
    and locnnbr <> 9300
    and ringinglocnnbr  <> 9300
    and b.soar_no in (101,102,103,104,105)
  group by 1,2
  having UNITS between 1 and 200
--order by 3
    --change it to last 12 months/ starting from 2017
)
,  t1 as (
select lyltycardnbr, locnnbr, UNITS, row_number() over(partition by lyltycardnbr order by UNITS desc) as rn
from store_members_map
)
, store_members_all_uf as (
select * from t1 where rn = 1
)
, member_rank as (
select locnnbr, lyltycardnbr, row_number() over(partition by locnnbr order by UNITS desc) as mem_rnk
from store_members_all_uf
)
, store_members_all as (
select * from member_rank
)
, store_members_all_filtered as (
select * from member_rank where mem_rnk <= 15000
)
, store_members as (
select * from store_members_all_filtered
--where locnnbr in (1125, 1733, 1840, 1205, 1905, 2215, 2373)
)
--Get Similar Members in Basic Apparel
--, similar_members as (
select z.locnnbr, a.entityA, a.entityB
--/*
, b.JaccardDistance as d_class_101_ss
, c.JaccardDistance as d_class_102_ss
, d.JaccardDistance as d_class_103_ss
, e.JaccardDistance as d_class_104_ss
, f.JaccardDistance as d_class_105_ss
--*/
, g.JaccardDistance as d_line_101_ss
, h.JaccardDistance as d_line_102_ss
, i.JaccardDistance as d_line_103_ss
, j.JaccardDistance as d_line_104_ss
, k.JaccardDistance as d_line_105_ss
--*/
, l.JaccardDistance as d_prod_101_ss
, m.JaccardDistance as d_prod_102_ss
, n.JaccardDistance as d_prod_103_ss
, o.JaccardDistance as d_prod_104_ss
, p.JaccardDistance as d_prod_105_ss
--*/
, q.JaccardDistance as d_na_101_ss
, r.JaccardDistance as d_na_102_ss
, s.JaccardDistance as d_na_103_ss
, t.JaccardDistance as d_na_104_ss
, u.JaccardDistance as d_na_105_ss
,
(
(
--/*
0.4 * 
(
coalesce(b.JaccardDistance, 0)
+ coalesce(c.JaccardDistance, 0)
+ coalesce(d.JaccardDistance, 0)
+ coalesce(e.JaccardDistance, 0)
+ coalesce(f.JaccardDistance, 0)
    )
+
0.3 * 
  (
coalesce(g.JaccardDistance, 0)
+ coalesce(h.JaccardDistance, 0)
+ coalesce(i.JaccardDistance, 0)
+ coalesce(j.JaccardDistance, 0)
+ coalesce(k.JaccardDistance, 0)
    )
+
0.2 * 
(
coalesce(l.JaccardDistance, 0)
+ coalesce(m.JaccardDistance, 0)
+ coalesce(n.JaccardDistance, 0)
+ coalesce(o.JaccardDistance, 0)
+ coalesce(p.JaccardDistance, 0)
 )
 +
0.1 * 
(
coalesce(q.JaccardDistance, 0)
+ coalesce(r.JaccardDistance, 0)
+ coalesce(s.JaccardDistance, 0)
+ coalesce(t.JaccardDistance, 0)
+ coalesce(u.JaccardDistance, 0)
 )
)
  /
  (
  0.4 * (
  case when b.JaccardDistance is null then 0 else 1 end
+ case when c.JaccardDistance is null then 0 else 1 end
+ case when d.JaccardDistance is null then 0 else 1 end
+ case when e.JaccardDistance is null then 0 else 1 end
+ case when f.JaccardDistance is null then 0 else 1 end
  ) + 
  0.3 * (
  case when g.JaccardDistance is null then 0 else 1 end
+ case when h.JaccardDistance is null then 0 else 1 end
+ case when i.JaccardDistance is null then 0 else 1 end
+ case when j.JaccardDistance is null then 0 else 1 end
+ case when k.JaccardDistance is null then 0 else 1 end
  ) + 
  0.2 * (
  case when l.JaccardDistance is null then 0 else 1 end
+ case when m.JaccardDistance is null then 0 else 1 end
+ case when n.JaccardDistance is null then 0 else 1 end
+ case when o.JaccardDistance is null then 0 else 1 end
+ case when p.JaccardDistance is null then 0 else 1 end
  )+ 
  0.1 * (
  case when q.JaccardDistance is null then 0 else 1 end
+ case when r.JaccardDistance is null then 0 else 1 end
+ case when s.JaccardDistance is null then 0 else 1 end
+ case when t.JaccardDistance is null then 0 else 1 end
+ case when u.JaccardDistance is null then 0 else 1 end
  )
 ) 
) 
as d_avg


from `apparel_jaccard_pp.pairs_AB_ss`  as a
left join store_members as z
on a.entityA = z.lyltycardnbr
--/*
left join `syw-analytics-ff.apparel_jaccard_output.class_101_ss` as b
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.class_102_ss` as c
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.class_103_ss` as d
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.class_104_ss` as e
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.class_105_ss` as f
using (entityA, entityB)

--*/
left join `syw-analytics-ff.apparel_jaccard_output.line_101_ss` as g
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.line_102_ss` as h
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.line_103_ss` as i
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.line_104_ss` as j
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.line_105_ss` as k
using (entityA, entityB)
--*/
left join `syw-analytics-ff.apparel_jaccard_output.prod_101_ss` as l
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.prod_102_ss` as m
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.prod_103_ss` as n
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.prod_104_ss` as o
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.prod_105_ss` as p
using (entityA, entityB)

--*/
left join `syw-analytics-ff.apparel_jaccard_output.na_101_ss` as q
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.na_102_ss` as r
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.na_103_ss` as s
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.na_104_ss` as t
using (entityA, entityB)
left join `syw-analytics-ff.apparel_jaccard_output.na_105_ss` as u
using (entityA, entityB)


where z.lyltycardnbr is not null
and a.entityA <> a.entityB
--)
