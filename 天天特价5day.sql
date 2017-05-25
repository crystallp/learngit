-- ************************************************************
-- **文件名：天天特价
-- **功能描述：对天天特价中的美食排序
-- **创建者：鲁平
--**创建日期：2017-04-24

set mapreduce.input.fileinputformat.split.maxsize=4294967296;

-- ***************step0:在天天特价入口下的订单************************************
-- 1、过去5天在天天特价上下单的用户的订单:
drop table if exists temp.special_price_01;
create table temp.special_price_01 as 
select dt,order_id,eleme_device_id,restaurant_id,phone_1,total,restaurant_type 
from dm.dm_log_app_order_source_detail_day_inc 
where dt>=get_date(-5) and title='天天特价';

---2、在天天特价中下单的订单中的情况主要获取用户的user_，GMV、subsidy、cut_money、user_id
drop table if exists temp.special_price_02;
create table temp.special_price_02 as 
select a.dt,a.order_id,b.user_id,a.eleme_device_id,b.total,b.eleme_order_total,
b.hongbao_amount,b.hongbao_source,b.restaurant_subsidy,b.cut_money,
b.restaurant_subsidy/b.total restaurant_subsidy_rate,b.cut_money/b.total cut_money_rate ,
b.deliver_fee
from  temp.special_price_01 a 
join
(select dt,id,user_id,total,eleme_order_total,hongbao_source,hongbao_amount,cut_money,restaurant_subsidy,deliver_fee
from dw.dw_trd_order_wide_day where dt>=get_date(-5)) b 
on a.order_id=b.id and a.dt=b.dt;


#===================选昨天有资格参加天天特价的菜品====================
#***********订单数据了解：*************
-- 2、用户维度：
--     用户是否是饿了么会员
--     用户天天特价订单占比
--     用户历史订单平均补贴力度
--     用户菜品
--     用户是否在该餐厅下单
#************step1：过去五天有资格参加天天特价的菜品：菜品主键表*****666W***************##1.1过去五天能参加天天特价活动的菜品
drop table temp.temp_tiantian_activity_00;
create table temp.temp_tiantian_activity_00 as
select get_date(-1) dt,shop_id, activity_id, sku_id, begin_time, 
end_time,row_number()over(partition by sku_id order by created_at desc) rank
from dw.dw_act_marketing_activity_participation where dt=get_date(-1)----活动与商品的关系，活动参与，商家、菜品。时间
and sku_id>0
and begin_time <> 'null'
and end_time <> 'null'
and get_date(begin_time) <= get_date(-1)
and get_date(end_time) >= get_date(-1)
and status=1
union all 
select get_date(-2) dt,shop_id, activity_id, sku_id, begin_time, 
end_time,row_number()over(partition by sku_id order by created_at desc) rank
from dw.dw_act_marketing_activity_participation where dt=get_date(-1)----活动与商品的关系，活动参与，商家、菜品。时间
and sku_id>0
and begin_time <> 'null'
and end_time <> 'null'
and get_date(begin_time) <= get_date(-2)
and get_date(end_time) >= get_date(-2)
and status=1 
union all 
select get_date(-3) dt,shop_id, activity_id, sku_id, begin_time, 
end_time,row_number()over(partition by sku_id order by created_at desc) rank
from dw.dw_act_marketing_activity_participation where dt=get_date(-1)----活动与商品的关系，活动参与，商家、菜品。时间
and sku_id>0
and begin_time <> 'null'
and end_time <> 'null'
and get_date(begin_time) <= get_date(-3)
and get_date(end_time) >= get_date(-3)
and status=1
union all 
select get_date(-4) dt,shop_id, activity_id, sku_id, begin_time, 
end_time,row_number()over(partition by sku_id order by created_at desc) rank
from dw.dw_act_marketing_activity_participation where dt=get_date(-1)----活动与商品的关系，活动参与，商家、菜品。时间
and sku_id>0
and begin_time <> 'null'
and end_time <> 'null'
and get_date(begin_time) <= get_date(-4)
and get_date(end_time) >= get_date(-4)
and status=1
union all 
select get_date(-5) dt,shop_id, activity_id, sku_id, begin_time, 
end_time,row_number()over(partition by sku_id order by created_at desc) rank
from dw.dw_act_marketing_activity_participation where dt=get_date(-1)----活动与商品的关系，活动参与，商家、菜品。时间
and sku_id>0
and begin_time <> 'null'
and end_time <> 'null'
and get_date(begin_time) <= get_date(-5)
and get_date(end_time) >= get_date(-5)
and status=1;
##1.1过去五天能参加天天特价活动的菜品关联其他信息价格、优惠力度等631W
drop table temp.temp_tiantian_activity_01;
create table temp.temp_tiantian_activity_01 as
select a.dt,a.shop_id, a.activity_id, a.begin_time, a.end_time,b.benefit_type,b.benefit_amt, a.sku_id, c.item_id, c.food_id, c.price, c.original_price, d.item_name
from temp.temp_tiantian_activity_00  a
inner join (
select shop_id, activity_id, benefit_type, benefit_amt
from dm.dm_mkt_act_order_restaurant_subsidy where dt=get_date(-1)----营销_订单活动_餐厅补贴规则表
and benefit_type in (2,4)
) b
on a.shop_id = b.shop_id
and a.activity_id = b.activity_id
left outer join (
select global_id as sku_id, shop_id, item_id, price, original_price, food_id
from dw.dw_prd_commodity_sku where dt=get_date(-1)----商品价格信息
) c
on a.shop_id = c.shop_id
and a.sku_id = c.food_id
left outer join (
select name as item_name, global_id as item_id
from dw.dw_prd_commodity_item where dt=get_date(-1)----菜品名称
) d
on c.item_id = d.item_id
where a.rank=1 and c.price is not null;


---???????--------------------step1:过去五天参加天天特价，过滤掉第二份半价的活动的菜品distinct food_id----607W----------
drop table temp.temp_tiantian_activity_02;
create table temp.temp_tiantian_activity_02 as
select distinct dt,shop_id, activity_id, begin_time, end_time,benefit_type,benefit_amt,     sku_id, item_id, food_id, price, original_price, item_name
from (select dt,shop_id, a.activity_id, begin_time, end_time,benefit_type,benefit_amt,     sku_id, item_id, food_id, price, original_price, item_name,    b.condition_type,b.condition_content,    (case when b.condition_type in (116,120) and b.condition_content>1 then 1 else 0 end) guolvfrom     (select dt,shop_id, activity_id, begin_time, end_time,benefit_type,benefit_amt,         sku_id, item_id, food_id, price, original_price, item_name     from temp.temp_tiantian_activity_01) a join    (select distinct  activity_id,condition_type,condition_content    from dm.dm_mkt_act_order_condition where dt=get_date(-1)     and is_valid=1) bon a.activity_id=b.activity_id) c
where guolv=0;


-- select dt,shop_id,food_id,count(1) from  temp.temp_tiantian_activity_02
-- group by dt,shop_id,food_id  limit 20;
#=================昨天有资格参加天天特价的菜品的菜品的feature、餐厅feature、用户餐厅feature===============
#***************step1:菜品，价格，优惠价格，餐厅信息********600W************
drop table if exists temp.every_day_special_price_feature_01;
create table temp.every_day_special_price_feature_01 as
select dt,food_id,price,(case when benefit_type=2 then  price-price * benefit_amt when benefit_type=4 then benefit_amt end )discount_price,(case when benefit_type=2 then price*benefit_amt when benefit_type=4 then price-benefit_amt end) activity_price,shop_id
from temp.temp_tiantian_activity_02;

#select count(1),sum(case when discount_price<0 then 1 else 0 end),sum(case when discount_price<0 then 1 else 0 end)/count(1) from temp.every_day_special_price_feature_01;

#*****************step2:菜feature:在昨天之前菜品价格、菜品销量、距离起送金额*******570W*********************
drop table if exists temp.every_day_special_price_feature_food_sales_count_02;
create table temp.every_day_special_price_feature_food_sales_count_02 as 
select dt,a.food_id,price,activity_price,discount_price,shop_id,is_gum,is_spicy,is_featured,recent_popularity,recent_rating,is_popular,has_activity,    packing_fee,sale_day,total_comment_cnt,favor_comment_cnt,    delivery_price_lack_rate,single_rate,unit_price_lack_rate,    quantity_1d,quantity_7d,total_7d,quantity_30d,    total_30d,is_xianshi
from (select dt,food_id,price,activity_price,discount_price,shop_idfrom temp.every_day_special_price_feature_01
) a 
join(select food_id,restaurant_id,is_gum,is_spicy,is_featured, recent_popularity,recent_rating,is_popular,has_activity,    packing_fee,sale_day,total_comment_cnt,favor_comment_cnt,    delivery_price_lack_rate,single_rate,unit_price_lack_rate,    quantity_1d,quantity_7d,total_7d,quantity_30d,    total_30d,is_xianshifrom analyst.persistent_sr_food_rank_info_summary where dt=get_date(-2)
) b
on a.food_id=b.food_id and a.shop_id=b.restaurant_id;

----说明：platform_temp.sr_food_rank_info_summary 此表是中间表只有一天的数据，所以需要基于此表创一个分区表analyst.persistent_sr_food_rank_info_summary，每天导入。
 -- insert overwrite table analyst.persistent_sr_food_rank_info_summary partition (dt='${day}') select * from platform_temp.sr_food_rank_info_summary


#*******************step3:菜品对应的餐厅的feature:在昨天之前的餐厅得分，开店时间等等***600W*****************
drop table if exists temp.every_day_special_price_feature_restaurant_info_03;
create table temp.every_day_special_price_feature_restaurant_info_03 as
select  a.dt,b.dt dt_1,a.shop_id restaurant_id,city_id,total_work_day,is_controlled_by_eleme,total_score,like_score,overall_score,order_score,subsidy_score,exposure_score,dinner_exposure_score,tea_exposure_score,night_exposure_score,negative_complaints_score,negative_bad_rating_score,order_exposure_score,dinner_order_exposure_score,tea_order_exposure_score,night_order_exposure_score,is_new_restaurant_carousel,is_qualitativus,price_level,back_user_rate,ptt_flag,like_rate_score,order_cnt_month,negative_user_withdraw_order_score,avg_cost,gmv_score 
from (select dt,food_id,price,activity_price,discount_price,shop_id from temp.every_day_special_price_feature_01) a 
join(select dt,restaurant_id,city_id,total_work_day,is_controlled_by_eleme,total_score,    like_score,overall_score,order_score,subsidy_score,exposure_score,dinner_exposure_score,tea_exposure_score,    
     night_exposure_score,negative_complaints_score,negative_bad_rating_score,negative_user_withdraw_order_score,    order_exposure_score,dinner_order_exposure_score,tea_order_exposure_score,night_order_exposure_score,is_new_restaurant_carousel,    
     is_qualitativus,price_level,back_user_rate,ptt_flag,like_rate_score,order_cnt_month,    avg_cost,gmv_score from  platform_dw.sr_shop_rank_score  where dt>=get_date(-6)
) b 
on a.shop_id=b.restaurant_id and date_sub(a.dt,1)=b.dt;

#*******************step4:整合step2和step3 每天菜品feature及餐厅feature******122W**********************
drop table if exists temp.every_day_special_price_feature_04;
create table temp.every_day_special_price_feature_04 as 
select distinct a.dt,food_id,restaurant_id,price,activity_price,discount_price,is_gum,is_spicy,is_featured,recent_popularity,recent_rating,is_popular,has_activity,    packing_fee,sale_day,total_comment_cnt,favor_comment_cnt,    delivery_price_lack_rate,single_rate,unit_price_lack_rate,    quantity_1d,quantity_7d,total_7d,quantity_30d,    total_30d,is_xianshi,city_id,total_work_day,is_controlled_by_eleme,total_score,like_score,overall_score,order_score,subsidy_score,exposure_score,dinner_exposure_score,tea_exposure_score,night_exposure_score,negative_complaints_score,negative_bad_rating_score,order_exposure_score,dinner_order_exposure_score,tea_order_exposure_score,night_order_exposure_score,is_new_restaurant_carousel,is_qualitativus,price_level,back_user_rate,ptt_flag,like_rate_score,order_cnt_month,negative_user_withdraw_order_score,avg_cost,gmv_score 
from 
(select dt,food_id,price,activity_price,discount_price,shop_id,is_gum,is_spicy,is_featured,recent_popularity,recent_rating,is_popular,has_activity,    packing_fee,sale_day,total_comment_cnt,favor_comment_cnt,    delivery_price_lack_rate,single_rate,unit_price_lack_rate,    quantity_1d,quantity_7d,total_7d,quantity_30d,    total_30d,is_xianshi
from temp.every_day_special_price_feature_food_sales_count_02)a 
join
(select dt, restaurant_id,city_id,total_work_day,is_controlled_by_eleme,total_score,like_score,overall_score,order_score,subsidy_score,exposure_score,dinner_exposure_score,tea_exposure_score,night_exposure_score,negative_complaints_score,negative_bad_rating_score,order_exposure_score,dinner_order_exposure_score,tea_order_exposure_score,night_order_exposure_score,is_new_restaurant_carousel,is_qualitativus,price_level,back_user_rate,ptt_flag,like_rate_score,order_cnt_month,negative_user_withdraw_order_score,avg_cost,gmv_score 
from temp.every_day_special_price_feature_restaurant_info_03) b
on a.shop_id=b.restaurant_id and a.dt=b.dt;


#*********************step6:天天特价入口在过去五天内的每天订单中购买的菜品信息 6W******************
drop table if exists temp.every_day_special_price_feature_05;
create table temp.every_day_special_price_feature_05 as 
select to_date(created_at) day,order_id,entity_id,restaurant_id,quantity,price 
from dw.dw_trd_order_item where dt=get_date(-1) and to_date(created_at)>=get_date(-5) ;


drop table if exists temp.every_day_special_price_feature_06;
create table temp.every_day_special_price_feature_06 as 
select dt,a.order_id,user_id,b.entity_id food_id,restaurant_id,quantity,price 
from (select  dt,order_id,user_id from temp.special_price_02) a 
join (select day,order_id,entity_id,restaurant_id,quantity,price from  temp.every_day_special_price_feature_05) b 
on a.order_id=b.order_id and a.dt=b.day;

#*****************step7:step6中的菜品哪些是在天天特价中搞活动的 121W******************
drop table if exists temp.every_day_special_price_feature_07;
create table temp.every_day_special_price_feature_07 as 
select a.dt,a.food_id,a.price/100 price,b.price food_item_price,b.quantity,activity_price/100 activity_price,discount_price/100 discount_price,is_gum,is_spicy,is_featured,recent_popularity,recent_rating,is_popular,has_activity,    packing_fee,sale_day,total_comment_cnt,favor_comment_cnt,    delivery_price_lack_rate,single_rate,unit_price_lack_rate,    quantity_1d,quantity_7d,total_7d,quantity_30d,    total_30d,is_xianshi,city_id,total_work_day,is_controlled_by_eleme,total_score,like_score,overall_score,order_score,subsidy_score,exposure_score,dinner_exposure_score,tea_exposure_score,night_exposure_score,negative_complaints_score,negative_bad_rating_score,tea_order_exposure_score,night_order_exposure_score,is_new_restaurant_carousel,is_qualitativus,price_level,back_user_rate,ptt_flag,like_rate_score,order_cnt_month,negative_user_withdraw_order_score,avg_cost,gmv_score ,user_id,b.restaurant_id
from 
(select dt,food_id,restaurant_id,price,activity_price,discount_price,is_gum,is_spicy,is_featured,recent_popularity,recent_rating,is_popular,has_activity,   
 packing_fee,sale_day,total_comment_cnt,favor_comment_cnt,    delivery_price_lack_rate,single_rate,unit_price_lack_rate,    quantity_1d,quantity_7d,total_7d,quantity_30d,    
 total_30d,is_xianshi,city_id,total_work_day,is_controlled_by_eleme,total_score,like_score,overall_score,order_score,subsidy_score,exposure_score,dinner_exposure_score,tea_exposure_score,
 night_exposure_score,negative_complaints_score,negative_bad_rating_score,order_exposure_score,dinner_order_exposure_score,tea_order_exposure_score,night_order_exposure_score,is_new_restaurant_carousel,
 is_qualitativus,price_level,back_user_rate,ptt_flag,like_rate_score,order_cnt_month,negative_user_withdraw_order_score,avg_cost,gmv_score from temp.every_day_special_price_feature_04 )a 
left join(select dt,food_id,price,quantity,restaurant_id,user_id from temp.every_day_special_price_feature_06) b 
on a.food_id=b.food_id and a.restaurant_id=b.restaurant_id and a.dt=b.dt;

-- -- #*****************step7.5:增加用户与餐厅的交互信息******************

-- #========================有资格参加天天特价的菜品中哪些是下过单的哪些是没有下单的，下单的用户是什么=============================================
-- drop table if exists temp.every_day_special_price_feature_07_05;
-- create table temp.every_day_special_price_feature_07_05 as 
-- select dt,a.food_id,price,food_item_price,quantity,activity_price,discount_price,is_gum,is_spicy,is_featured,recent_popularity,recent_rating,is_popular,has_activity,    packing_fee,sale_day,total_comment_cnt,favor_comment_cnt,    delivery_price_lack_rate,single_rate,unit_price_lack_rate,    quantity_1d,quantity_7d,total_7d,quantity_30d,    total_30d,is_xianshi,city_id,total_work_day,is_controlled_by_eleme,total_score,like_score,overall_score,order_score,subsidy_score,exposure_score,dinner_exposure_score,tea_exposure_score,night_exposure_score,negative_complaints_score,negative_bad_rating_score,tea_order_exposure_score,night_order_exposure_score,is_new_restaurant_carousel,
-- is_qualitativus,price_level,back_user_rate,ptt_flag,like_rate_score,order_cnt_month,negative_user_withdraw_order_score,avg_cost,gmv_score ,a.user_id,a.restaurant_id,
-- time_type,service_rating_avg_180_90,service_rating_avg_90_60,service_rating_avg_60_30,service_rating_avg_30_7,service_rating_avg_7_3,service_rating_avg_3,
-- buy_180_90,buy_90_60,buy_60_30,buy_30_7,buy_7_3,buy_3
-- from (select dt,food_id, price, food_item_price,quantity, activity_price, discount_price,is_gum,is_spicy,is_featured,recent_popularity,recent_rating,is_popular,has_activity,    
--       packing_fee,sale_day,total_comment_cnt,favor_comment_cnt,    delivery_price_lack_rate,single_rate,unit_price_lack_rate,    quantity_1d,quantity_7d,total_7d,quantity_30d,    
--       total_30d,is_xianshi,city_id,total_work_day,is_controlled_by_eleme,total_score,like_score,overall_score,order_score,subsidy_score,exposure_score,dinner_exposure_score,tea_exposure_score,
--       night_exposure_score,negative_complaints_score,negative_bad_rating_score,tea_order_exposure_score,night_order_exposure_score,is_new_restaurant_carousel,is_qualitativus,price_level,back_user_rate,ptt_flag,
--       ike_rate_score,order_cnt_month,negative_user_withdraw_order_score,avg_cost,gmv_score ,user_id,restaurant_idfrom temp.every_day_special_price_feature_07) a 
-- left join
-- (select user_id,restaurant_id,time_type,service_rating_avg_180_90,service_rating_avg_90_60,service_rating_avg_60_30,service_rating_avg_30_7,service_rating_avg_7_3,service_rating_avg_3,buy_180_90,
--     buy_90_60,buy_60_30,buy_30_7,buy_7_3,buy_3 
-- from analyst.persistent_sr_userinfo_temp where dt=get_date(-2)) b 
-- on a.user_id=b.user_id and a.restaurant_id=b.restaurant_id;

-- -----temp.sr_userinfo_temp_1_为中间表，要创建分区表---
-- -- create table analyst.persistent_sr_userinfo_temp (
-- -- user_id                 int,
-- -- restaurant_id           int,
-- -- time_type               string,
-- -- service_rating_avg_180_90   double,
-- -- service_rating_avg_90_60    double,
-- -- service_rating_avg_60_30    double,
-- -- service_rating_avg_30_7 double,
-- -- service_rating_avg_7_3  double,
-- -- service_rating_avg_3    double,
-- -- buy_180_90              double,
-- -- buy_90_60               double,
-- -- buy_60_30               double,
-- -- buy_30_7                double,
-- -- buy_7_3                 double,
-- -- buy_3                   double)
-- -- partitioned  by (dt string);
-- -- insert overwrite table analyst.persistent_sr_userinfo_temp partition (dt='${day}') select * from temp.sr_userinfo_temp_1_;



#*****************step8:step7中的菜品打标签，下过单是1没有下单的是0*******************

drop table if exists temp.every_day_special_price_feature_08;
create table temp.every_day_special_price_feature_08 as 
select  price,activity_price,discount_price,discount_price/price discount_price_rate,is_gum,is_spicy,is_featured,recent_popularity,recent_rating,is_popular,has_activity,    
packing_fee,sale_day,total_comment_cnt,favor_comment_cnt,    delivery_price_lack_rate,single_rate,unit_price_lack_rate,    quantity_1d,quantity_7d,total_7d,quantity_30d,    
total_30d,is_xianshi,city_id,total_work_day,is_controlled_by_eleme,total_score,like_score,overall_score,order_score,subsidy_score,exposure_score,dinner_exposure_score,tea_exposure_score,
night_exposure_score,negative_complaints_score,negative_bad_rating_score,tea_order_exposure_score,night_order_exposure_score,is_new_restaurant_carousel,is_qualitativus,price_level,back_user_rate,ptt_flag,
like_rate_score,order_cnt_month,negative_user_withdraw_order_score,avg_cost,gmv_score ,(case when food_item_price is null then 0 else 1 end) label
from 
    (select  dt,food_id,price,food_item_price,quantity,activity_price,discount_price,is_gum,is_spicy,is_featured,recent_popularity,recent_rating,is_popular,has_activity,    
      packing_fee,sale_day,total_comment_cnt,favor_comment_cnt,    delivery_price_lack_rate,single_rate,unit_price_lack_rate,    quantity_1d,quantity_7d,total_7d,quantity_30d,    
      total_30d,is_xianshi,city_id,total_work_day,is_controlled_by_eleme,total_score,like_score,overall_score,order_score,subsidy_score,exposure_score,dinner_exposure_score,tea_exposure_score,
      night_exposure_score,negative_complaints_score,negative_bad_rating_score,tea_order_exposure_score,night_order_exposure_score,is_new_restaurant_carousel,is_qualitativus,price_level,back_user_rate,
      ptt_flag,like_rate_score,order_cnt_month,negative_user_withdraw_order_score,avg_cost,gmv_score from temp.every_day_special_price_feature_07
) a ;

-- select label,count(1) from temp.every_day_special_price_feature_08 group by label;
#*****************step9:step8将定性指标量化******586*************
drop table if exists temp.every_day_special_price_feature_09;
create table temp.every_day_special_price_feature_09 as 
select price,activity_price,discount_price,discount_price/price discount_price_rate,is_gum,is_spicy,is_featured,recent_popularity,recent_rating,is_popular,has_activity,    
packing_fee,sale_day,total_comment_cnt,favor_comment_cnt,    delivery_price_lack_rate,single_rate,unit_price_lack_rate,    quantity_1d,quantity_7d,total_7d,quantity_30d,    
total_30d,is_xianshi,city_id,total_work_day,is_controlled_by_eleme,total_score,like_score,overall_score,order_score,subsidy_score,exposure_score,dinner_exposure_score,tea_exposure_score,
night_exposure_score,negative_complaints_score,negative_bad_rating_score,tea_order_exposure_score,night_order_exposure_score,is_new_restaurant_carousel,is_qualitativus,price_level,back_user_rate,ptt_flag,
like_rate_score,order_cnt_month,negative_user_withdraw_order_score,avg_cost,gmv_score,label
from temp.every_day_special_price_feature_08;

---select label,count(1) from temp.every_day_special_price_feature_09 group by label;
---hive -e "select * from temp.every_day_special_price_feature_09;">./train2.txt;
#****************创建最终表的分区表****************
drop table if exists analyst.persistent_every_day_special_price_feature_train_shell;
create table analyst.persistent_every_day_special_price_feature_train_shell (
price                   double,
activity_price          double,
discount_price          double,
discount_price_rate     double,
is_gum                  int,
is_spicy                int,
is_featured             int,
recent_popularity       int,
recent_rating           double,
is_popular              int,
has_activity            int,
packing_fee             double,
sale_day                int,
total_comment_cnt       bigint,
favor_comment_cnt       bigint,
delivery_price_lack_rate    double,
single_rate             double,
unit_price_lack_rate    double,
quantity_1d             int,
quantity_7d             int,
total_7d                double,
quantity_30d            int,
total_30d               double,
is_xianshi              int,
city_id                 int,
total_work_day          int,
is_controlled_by_eleme  int,
total_score             double,
like_score              double,
overall_score           double,
order_score             double,
subsidy_score           double,
exposure_score          double,
dinner_exposure_score   double,
tea_exposure_score      double,
night_exposure_score    double,
negative_complaints_score   double,
negative_bad_rating_score   double,
tea_order_exposure_score    double,
night_order_exposure_score  double,
is_new_restaurant_carousel  int,
is_qualitativus         int,
price_level             int,
back_user_rate          double,
like_rate_score         double,
order_cnt_month         int,
negative_user_withdraw_order_score  double,
avg_cost                double,
gmv_score               double,
label                   int
)
partitioned  by (dt string);

insert overwrite table analyst.persistent_every_day_special_price_feature_train_shell partition (dt='${day}') 
select price, activity_price, discount_price, discount_price_rate, is_gum, is_spicy, is_featured, recent_popularity, recent_rating, is_popular, has_activity, packing_fee, 
sale_day, total_comment_cnt, favor_comment_cnt, delivery_price_lack_rate, single_rate, unit_price_lack_rate, quantity_1d, quantity_7d, total_7d, quantity_30d, total_30d, is_xianshi, 
city_id, total_work_day, is_controlled_by_eleme, total_score, like_score, overall_score, order_score, subsidy_score, exposure_score, dinner_exposure_score, tea_exposure_score, night_exposure_score, 
negative_complaints_score, negative_bad_rating_score, tea_order_exposure_score, night_order_exposure_score, is_new_restaurant_carousel, is_qualitativus, price_level, back_user_rate, like_rate_score, 
order_cnt_month, negative_user_withdraw_order_score, avg_cost, gmv_score, label 
 from temp.every_day_special_price_feature_09;














#**************欠采样**************
drop table if exists temp.every_day_special_price_feature_10;
create table temp.every_day_special_price_feature_10 as 
select price,activity_price,discount_price,discount_price/price discount_price_rate,is_gum,is_spicy,is_featured,recent_popularity,recent_rating,is_popular,has_activity,    packing_fee,sale_day,total_comment_cnt,favor_comment_cnt,    delivery_price_lack_rate,single_rate,unit_price_lack_rate,    quantity_1d,quantity_7d,total_7d,quantity_30d,    total_30d,is_xianshi,city_id,total_work_day,is_controlled_by_eleme,total_score,like_score,overall_score,order_score,subsidy_score,exposure_score,dinner_exposure_score,tea_exposure_score,night_exposure_score,negative_complaints_score,negative_bad_rating_score,avg_cost,gmv_score ,service_rating_avg_180_90,service_rating_avg_90_60,service_rating_avg_60_30,service_rating_avg_30_7,service_rating_avg_7_3,service_rating_avg_3,buy_180_90,buy_90_60,buy_60_30,buy_30_7,buy_7_3,buy_3,time_type,label
from  temp.every_day_special_price_feature_09
where label=1


drop table if exists temp.every_day_special_price_feature_11;
create table temp.every_day_special_price_feature_11 as 
select price,activity_price,discount_price,discount_price/price discount_price_rate,is_gum,is_spicy,is_featured,recent_popularity,recent_rating,is_popular,has_activity,    packing_fee,sale_day,total_comment_cnt,favor_comment_cnt,    delivery_price_lack_rate,single_rate,unit_price_lack_rate,    quantity_1d,quantity_7d,total_7d,quantity_30d,    total_30d,is_xianshi,city_id,total_work_day,is_controlled_by_eleme,total_score,like_score,overall_score,order_score,subsidy_score,exposure_score,dinner_exposure_score,tea_exposure_score,night_exposure_score,negative_complaints_score,negative_bad_rating_score,avg_cost,gmv_score ,service_rating_avg_180_90,service_rating_avg_90_60,service_rating_avg_60_30,service_rating_avg_30_7,service_rating_avg_7_3,service_rating_avg_3,buy_180_90,buy_90_60,buy_60_30,buy_30_7,buy_7_3,buy_3,time_type,label
from  temp.every_day_special_price_feature_09
where label=0 limit 100000;

hive -e "set hive.cli.print,.header=True;select * from temp.every_day_special_price_feature_10;">./trainData_1.txt;
hive -e "set hive.cli.print,.header=True;select * from temp.every_day_special_price_feature_11;">./testData_0.txt;

