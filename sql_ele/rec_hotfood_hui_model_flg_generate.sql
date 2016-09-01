set mapred.max.split.size=4096000000;

drop table if exists temp.rec_hotfood_hui_step_tmp_1;
CREATE TABLE temp.rec_hotfood_hui_step_tmp_1 AS
select
a.log_time,
a.session_id,
b.user_id,
cast(split(parse_json_object(a.activity_param,'restaurants'),',')[0] as bigint) as restaurants1,
cast(split(parse_json_object(a.activity_param,'restaurants'),',')[1] as bigint) as restaurants2,
cast(split(parse_json_object(a.activity_param,'restaurants'),',')[2] as bigint) as restaurants3,
cast(split(parse_json_object(a.activity_param,'restaurants'),',')[3] as bigint) as restaurants4,
cast(split(parse_json_object(a.activity_param,'restaurants'),',')[4] as bigint) as restaurants5,
a.dt
from dw.dw_log_app_pv_day_inc a, dw.dw_log_app_visit_day_inc b
where a.dt >= get_date('2016-08-22',-10) and b.dt >= get_date('2016-08-22',-10) and a.session_id = b.session_id
and a.activity_id = 2333 and b.user_id is not null;

drop table if exists temp.rec_hotfood_hui_step_tmp_2;
CREATE TABLE temp.rec_hotfood_hui_step_tmp_2 AS
select
log_time,user_id,restaurants1 as restaurant,dt,session_id
from temp.rec_hotfood_hui_step_tmp_1
union all
select
log_time,user_id,restaurants2 as restaurant,dt,session_id
from temp.rec_hotfood_hui_step_tmp_1
union all
select
log_time,user_id,restaurants3 as restaurant,dt,session_id
from temp.rec_hotfood_hui_step_tmp_1
union all
select
log_time,user_id,restaurants4 as restaurant,dt,session_id
from temp.rec_hotfood_hui_step_tmp_1
union all
select
log_time,user_id,restaurants5 as restaurant,dt,session_id
from temp.rec_hotfood_hui_step_tmp_1;

drop table if exists temp.rec_hotfood_hui_step_tmp_3;
CREATE TABLE temp.rec_hotfood_hui_step_tmp_3 AS
select log_time,user_id,restaurant,dt,session_id,
row_number() OVER(PARTITION BY concat(session_id,cast(restaurant as string)) ORDER BY log_time desc) as flg
from temp.rec_hotfood_hui_step_tmp_2;

drop table if exists temp.rec_hotfood_hui_step_tmp_4;
CREATE TABLE temp.rec_hotfood_hui_step_tmp_4 AS
select log_time,user_id,restaurant,dt,session_id
from temp.rec_hotfood_hui_step_tmp_3 where flg = 1;

drop table if exists temp.rec_hotfood_hui_step_tmp_5;
CREATE table temp.rec_hotfood_hui_step_tmp_5 as
select session_id,cast(parse_json_object(activity_param,'restaurant_id') as bigint) as restaurant 
from dw.dw_log_app_pv_day_inc a
where a.dt >= get_date('2016-08-22',-10) and activity_id = 1399 and cast(parse_json_object(activity_param,'restaurant_id') as bigint) is not null;

drop table if exists temp.rec_hotfood_hui_step_tmp_6;
CREATE table temp.rec_hotfood_hui_step_tmp_6 as
select distinct session_id,restaurant
from temp.rec_hotfood_hui_step_tmp_5;

drop table if exists temp.rec_hotfood_hui_step_tmp_7;
CREATE table temp.rec_hotfood_hui_step_tmp_7 as
select a.log_time,a.user_id,a.session_id,a.restaurant,dt,
case when b.restaurant is null then 0 else 1 end as click_flg
from temp.rec_hotfood_hui_step_tmp_4 a left join temp.rec_hotfood_hui_step_tmp_6 b 
on a.session_id = b.session_id and a.restaurant = b.restaurant;

drop table if exists temp.rec_hotfood_hui_step_tmp_8;
CREATE table temp.rec_hotfood_hui_step_tmp_8 as
select dt,user_id,restaurant_id as restaurant
FROM dw.dw_trd_order_wide_day
where dt >= get_date('2016-08-22',-10) and order_status=1 and bu_flag!='SIZ';

drop table if exists temp.rec_hotfood_hui_step_tmp_9;
CREATE table temp.rec_hotfood_hui_step_tmp_9 as
select a.log_time,a.user_id,a.restaurant,a.dt,
a.click_flg,case when b.restaurant is null then 0 else 1 end as order_flg
from temp.rec_hotfood_hui_step_tmp_7 a left join temp.rec_hotfood_hui_step_tmp_8 b
on a.dt=b.dt and a.restaurant = b.restaurant and a.user_id = b.user_id;


drop table if exists temp.rec_hotfood_GBDT_sort_hui_step_tmp_0;
CREATE TABLE temp.rec_hotfood_GBDT_sort_hui_step_tmp_0 AS
select log_time,hour(log_time)*60+minute(log_time) as minutes,user_id,b.primary_category as category,c.id as category_id,restaurant as restaurant_id,a.dt,click_flg,order_flg
from temp.rec_hotfood_hui_step_tmp_9 a,
rec.rec_cls_restaurant_category_relation b,
rec.rec_cls_restaurant_category c
where a.dt = '2016-08-18' and b.dt = '2016-08-17' and b.primary_category = c.category and c.is_secondary = 1 and a.restaurant = b.restaurant_id
union all
select log_time,hour(log_time)*60+minute(log_time) as minutes,user_id,b.primary_category as category,c.id as category_id,restaurant as restaurant_id,a.dt,click_flg,order_flg
from temp.rec_hotfood_hui_step_tmp_9 a,
rec.rec_cls_restaurant_category_relation b,
rec.rec_cls_restaurant_category c
where a.dt = '2016-08-19' and b.dt = '2016-08-18' and b.primary_category = c.category and c.is_secondary = 1 and a.restaurant = b.restaurant_id
union all
select log_time,hour(log_time)*60+minute(log_time) as minutes,user_id,b.primary_category as category,c.id as category_id,restaurant as restaurant_id,a.dt,click_flg,order_flg
from temp.rec_hotfood_hui_step_tmp_9 a,
rec.rec_cls_restaurant_category_relation b,
rec.rec_cls_restaurant_category c
where a.dt = '2016-08-20' and b.dt = '2016-08-19' and b.primary_category = c.category and c.is_secondary = 1 and a.restaurant = b.restaurant_id
union all
select log_time,hour(log_time)*60+minute(log_time) as minutes,user_id,b.primary_category as category,c.id as category_id,restaurant as restaurant_id,a.dt,click_flg,order_flg
from temp.rec_hotfood_hui_step_tmp_9 a,
rec.rec_cls_restaurant_category_relation b,
rec.rec_cls_restaurant_category c
where a.dt = '2016-08-21' and b.dt = '2016-08-20' and b.primary_category = c.category and c.is_secondary = 1 and a.restaurant = b.restaurant_id;

drop table if exists temp.rec_hotfood_GBDT_sort_hui_step_tmp_1;
create table if not exists temp.rec_hotfood_GBDT_sort_hui_step_tmp_1 as 
select a.minute, concat('{', 
                            concat_ws(',', collect_set(concat(b.id, ':', cast(round(a.heat,4) as string)))), 
                            '}') as cat_prefer
from 
(select * from rec.rec_hotfood_time_cat_rel where dt='3000-12-31') a
join
(select * from rec.rec_cls_restaurant_category where is_secondary=1) b
on a.category=b.category
group by minute;


drop table if exists temp.rec_hotfood_GBDT_sort_hui_step_tmp_2;
CREATE table temp.rec_hotfood_GBDT_sort_hui_step_tmp_2 AS
select
a.dt, 
a.user_id,
a.restaurant_id,
pmod(datediff(a.dt, '2016-07-04'), 7)+1 as weekdays,
parse_json_object(b.cat_prefer,a.category_id) as time_cat_prefer,
f.total_score,
f.order_score,
f.collections_score,
f.user_order_score,
f.like_score,
f.picture_score,
f.negative_complaints_score,
f.negative_restaurant_withdraw_order_score,
f.negative_user_withdraw_order_score,
f.negative_bad_rating_score,
f.reminder_score,
f.customer_order_price,
f.exposure_score,
c.bu_flag,
d.avg_score as user_avg_score,  
e.avg_percent as restaurant_avg_percent,
1.0/(1.0+pow(2.7182818284590,-(d.avg_score - e.avg_percent))) as user_restaurant_ratio,
1.0/(1.0+pow(2.7182818284590,-(d.avg_score - e.avg_percent))) as abs_user_restaurant_ratio,
e.avg_price as restaurant_avg_price,
g.score as user_category_score,
parse_json_object(parse_json_object(h.attr_value,cast(a.restaurant_id as string)),"pv_prefer") as shop_pv_prefer,
parse_json_object(parse_json_object(h.attr_value,cast(a.restaurant_id as string)),"order_prefer") as shop_order_prefer,
click_flg,
order_flg
from (select * from temp.rec_hotfood_GBDT_sort_hui_step_tmp_0 where dt = '2016-08-18') a
left join temp.rec_hotfood_GBDT_sort_hui_step_tmp_1 b on a.minutes = b.minute
left join (select * from dm.dm_usr_portrait_order where dt = '2016-08-17') c on a.user_id = c.user_id
left join (select * from rec.rec_hotfood_user_profile where dt = '2016-08-17') d on a.user_id = d.user_id 
left join (select * from rec.rec_hotfood_restaurant_profile where dt = '2016-08-17') e on a.restaurant_id = e.restaurant_id 
left join (select * from rec.rec_prd_restaurant_rank_info where dt = '2016-08-17') f on a.restaurant_id = f.id 
left join (select * from rec.rec_hotfood_user_category_sort_profile where dt = '2016-08-17') g on a.category_id = g.category_id and a.user_id = g.user_id 
left join (select * from rec.rec_hotfood_user_info where dt='2016-08-17' and model='shop_prefer') h on a.user_id=h.user_id
union all
select
a.dt, 
a.user_id,
a.restaurant_id,
pmod(datediff(a.dt, '2016-07-04'), 7)+1 as weekdays,
parse_json_object(b.cat_prefer,a.category_id) as time_cat_prefer,
f.total_score,
f.order_score,
f.collections_score,
f.user_order_score,
f.like_score,
f.picture_score,
f.negative_complaints_score,
f.negative_restaurant_withdraw_order_score,
f.negative_user_withdraw_order_score,
f.negative_bad_rating_score,
f.reminder_score,
f.customer_order_price,
f.exposure_score,
c.bu_flag,
d.avg_score as user_avg_score,  
e.avg_percent as restaurant_avg_percent,
1.0/(1.0+pow(2.7182818284590,-(d.avg_score - e.avg_percent))) as user_restaurant_ratio,
1.0/(1.0+pow(2.7182818284590,-(d.avg_score - e.avg_percent))) as abs_user_restaurant_ratio,
e.avg_price as restaurant_avg_price,
g.score as user_category_score,
parse_json_object(parse_json_object(h.attr_value,cast(a.restaurant_id as string)),"pv_prefer") as shop_pv_prefer,
parse_json_object(parse_json_object(h.attr_value,cast(a.restaurant_id as string)),"order_prefer") as shop_order_prefer,
click_flg,
order_flg
from (select * from temp.rec_hotfood_GBDT_sort_hui_step_tmp_0 where dt = '2016-08-19') a
left join temp.rec_hotfood_GBDT_sort_hui_step_tmp_1 b on a.minutes = b.minute
left join (select * from dm.dm_usr_portrait_order where dt = '2016-08-18') c on a.user_id = c.user_id
left join (select * from rec.rec_hotfood_user_profile where dt = '2016-08-18') d on a.user_id = d.user_id 
left join (select * from rec.rec_hotfood_restaurant_profile where dt = '2016-08-18') e on a.restaurant_id = e.restaurant_id 
left join (select * from rec.rec_prd_restaurant_rank_info where dt = '2016-08-18') f on a.restaurant_id = f.id 
left join (select * from rec.rec_hotfood_user_category_sort_profile where dt = '2016-08-18') g on a.category_id = g.category_id and a.user_id = g.user_id 
left join (select * from rec.rec_hotfood_user_info where dt='2016-08-18' and model='shop_prefer') h on a.user_id=h.user_id
union all
select
a.dt, 
a.user_id,
a.restaurant_id,
pmod(datediff(a.dt, '2016-07-04'), 7)+1 as weekdays,
parse_json_object(b.cat_prefer,a.category_id) as time_cat_prefer,
f.total_score,
f.order_score,
f.collections_score,
f.user_order_score,
f.like_score,
f.picture_score,
f.negative_complaints_score,
f.negative_restaurant_withdraw_order_score,
f.negative_user_withdraw_order_score,
f.negative_bad_rating_score,
f.reminder_score,
f.customer_order_price,
f.exposure_score,
c.bu_flag,
d.avg_score as user_avg_score,  
e.avg_percent as restaurant_avg_percent,
1.0/(1.0+pow(2.7182818284590,-(d.avg_score - e.avg_percent))) as user_restaurant_ratio,
1.0/(1.0+pow(2.7182818284590,-(d.avg_score - e.avg_percent))) as abs_user_restaurant_ratio,
e.avg_price as restaurant_avg_price,
g.score as user_category_score,
parse_json_object(parse_json_object(h.attr_value,cast(a.restaurant_id as string)),"pv_prefer") as shop_pv_prefer,
parse_json_object(parse_json_object(h.attr_value,cast(a.restaurant_id as string)),"order_prefer") as shop_order_prefer,
click_flg,
order_flg
from (select * from temp.rec_hotfood_GBDT_sort_hui_step_tmp_0 where dt = '2016-08-20') a
left join temp.rec_hotfood_GBDT_sort_hui_step_tmp_1 b on a.minutes = b.minute
left join (select * from dm.dm_usr_portrait_order where dt = '2016-08-19') c on a.user_id = c.user_id
left join (select * from rec.rec_hotfood_user_profile where dt = '2016-08-19') d on a.user_id = d.user_id 
left join (select * from rec.rec_hotfood_restaurant_profile where dt = '2016-08-19') e on a.restaurant_id = e.restaurant_id 
left join (select * from rec.rec_prd_restaurant_rank_info where dt = '2016-08-19') f on a.restaurant_id = f.id 
left join (select * from rec.rec_hotfood_user_category_sort_profile where dt = '2016-08-19') g on a.category_id = g.category_id and a.user_id = g.user_id 
left join (select * from rec.rec_hotfood_user_info where dt='2016-08-19' and model='shop_prefer') h on a.user_id=h.user_id
union all
select
a.dt, 
a.user_id,
a.restaurant_id,
pmod(datediff(a.dt, '2016-07-04'), 7)+1 as weekdays,
parse_json_object(b.cat_prefer,a.category_id) as time_cat_prefer,
f.total_score,
f.order_score,
f.collections_score,
f.user_order_score,
f.like_score,
f.picture_score,
f.negative_complaints_score,
f.negative_restaurant_withdraw_order_score,
f.negative_user_withdraw_order_score,
f.negative_bad_rating_score,
f.reminder_score,
f.customer_order_price,
f.exposure_score,
c.bu_flag,
d.avg_score as user_avg_score,  
e.avg_percent as restaurant_avg_percent,
1.0/(1.0+pow(2.7182818284590,-(d.avg_score - e.avg_percent))) as user_restaurant_ratio,
1.0/(1.0+pow(2.7182818284590,-(d.avg_score - e.avg_percent))) as abs_user_restaurant_ratio,
e.avg_price as restaurant_avg_price,
g.score as user_category_score,
parse_json_object(parse_json_object(h.attr_value,cast(a.restaurant_id as string)),"pv_prefer") as shop_pv_prefer,
parse_json_object(parse_json_object(h.attr_value,cast(a.restaurant_id as string)),"order_prefer") as shop_order_prefer,
click_flg,
order_flg
from (select * from temp.rec_hotfood_GBDT_sort_hui_step_tmp_0 where dt = '2016-08-21') a
left join temp.rec_hotfood_GBDT_sort_hui_step_tmp_1 b on a.minutes = b.minute
left join (select * from dm.dm_usr_portrait_order where dt = '2016-08-20') c on a.user_id = c.user_id
left join (select * from rec.rec_hotfood_user_profile where dt = '2016-08-20') d on a.user_id = d.user_id 
left join (select * from rec.rec_hotfood_restaurant_profile where dt = '2016-08-20') e on a.restaurant_id = e.restaurant_id 
left join (select * from rec.rec_prd_restaurant_rank_info where dt = '2016-08-20') f on a.restaurant_id = f.id 
left join (select * from rec.rec_hotfood_user_category_sort_profile where dt = '2016-08-20') g on a.category_id = g.category_id and a.user_id = g.user_id 
left join (select * from rec.rec_hotfood_user_info where dt='2016-08-20' and model='shop_prefer') h on a.user_id=h.user_id

drop table if exists temp.rec_hotfood_restaurant_food_category_temp_1;
create table temp.rec_hotfood_restaurant_food_category_temp_1 as 
select dt,food_id,category from dm.dm_mdl_food_name_normalize_day where dt in ('2016-08-17','2016-08-18','2016-08-19','2016-08-20')

drop table if exists temp.rec_hotfood_restaurant_food_category_temp_2;
create table temp.rec_hotfood_restaurant_food_category_temp_2 as 
select a.dt,a.restaurant_id,concat('[\"',concat_ws('\",\"',collect_set(b.category)),'\"]') as restaurant_food_name
from dw.dw_prd_food a,temp.rec_hotfood_restaurant_food_category_temp_1 b
where a.dt in ('2016-08-17','2016-08-18','2016-08-19','2016-08-20') and a.dt=b.dt and a.id = b.food_id
group by a.restaurant_id,a.dt;

drop table if exists temp.rec_hotfood_restaurant_food_category_temp_3;
create table temp.rec_hotfood_restaurant_food_category_temp_3 as 
select dt,user_id,attr_value as user_favour_food from rec.rec_hotfood_user_info
where dt in ('2016-08-18','2016-08-19','2016-08-20') and model = 'food_prefer';


drop table if exists temp.rec_hotfood_restaurant_food_category_temp_4;
create table temp.rec_hotfood_restaurant_food_category_temp_4 as
select a.dt,a.user_id,a.restaurant_id,a.weekdays,
a.time_cat_prefer,a.total_score,a.order_score,
a.collections_score,a.user_order_score,a.like_score,
a.picture_score,a.negative_complaints_score,a.negative_restaurant_withdraw_order_score,
a.negative_user_withdraw_order_score,a.negative_bad_rating_score,
a.reminder_score,a.customer_order_price,a.exposure_score,a.bu_flag,
a.user_avg_score,a.restaurant_avg_percent,a.user_restaurant_ratio,
a.abs_user_restaurant_ratio,a.restaurant_avg_price,a.user_category_score,
a.shop_pv_prefer,a.shop_order_prefer,b.restaurant_food_name,c.user_favour_food,a.click_flg,a.order_flg 
from (select * from temp.rec_hotfood_GBDT_sort_hui_step_tmp_2 where dt = '2016-08-19') a
left join (select * from temp.rec_hotfood_restaurant_food_category_temp_2 where dt = '2016-08-18') b on a.restaurant_id = b.restaurant_id
left join (select * from temp.rec_hotfood_restaurant_food_category_temp_3 where dt = '2016-08-18') c on a.user_id = c.user_id
union all
select a.dt,a.user_id,a.restaurant_id,a.weekdays,
a.time_cat_prefer,a.total_score,a.order_score,
a.collections_score,a.user_order_score,a.like_score,
a.picture_score,a.negative_complaints_score,a.negative_restaurant_withdraw_order_score,
a.negative_user_withdraw_order_score,a.negative_bad_rating_score,
a.reminder_score,a.customer_order_price,a.exposure_score,a.bu_flag,
a.user_avg_score,a.restaurant_avg_percent,a.user_restaurant_ratio,
a.abs_user_restaurant_ratio,a.restaurant_avg_price,a.user_category_score,
a.shop_pv_prefer,a.shop_order_prefer,b.restaurant_food_name,c.user_favour_food,a.click_flg,a.order_flg 
from (select * from temp.rec_hotfood_GBDT_sort_hui_step_tmp_2 where dt = '2016-08-20') a
left join (select * from temp.rec_hotfood_restaurant_food_category_temp_2 where dt = '2016-08-19') b on a.restaurant_id = b.restaurant_id
left join (select * from temp.rec_hotfood_restaurant_food_category_temp_3 where dt = '2016-08-19') c on a.user_id = c.user_id
union all
select a.dt,a.user_id,a.restaurant_id,a.weekdays,
a.time_cat_prefer,a.total_score,a.order_score,
a.collections_score,a.user_order_score,a.like_score,
a.picture_score,a.negative_complaints_score,a.negative_restaurant_withdraw_order_score,
a.negative_user_withdraw_order_score,a.negative_bad_rating_score,
a.reminder_score,a.customer_order_price,a.exposure_score,a.bu_flag,
a.user_avg_score,a.restaurant_avg_percent,a.user_restaurant_ratio,
a.abs_user_restaurant_ratio,a.restaurant_avg_price,a.user_category_score,
a.shop_pv_prefer,a.shop_order_prefer,b.restaurant_food_name,c.user_favour_food,a.click_flg,a.order_flg 
from (select * from temp.rec_hotfood_GBDT_sort_hui_step_tmp_2 where dt = '2016-08-21') a
left join (select * from temp.rec_hotfood_restaurant_food_category_temp_2 where dt = '2016-08-20') b on a.restaurant_id = b.restaurant_id
left join (select * from temp.rec_hotfood_restaurant_food_category_temp_3 where dt = '2016-08-20') c on a.user_id = c.user_id;


