drop table temp.temp_rplc_user_food_rec_${dt};
create table temp.temp_rplc_user_food_rec_${dt} (
user_id bigint, 
rec_method string,
food_name string,
related_order_num bigint,
rec_score double
) partitioned by (step string);

insert overwrite table temp.temp_rplc_user_food_rec_${dt} partition(step='temp_rplc_buy')
select t1.user_id, t2.rec_method, t1.food_name, t1.order_num, t1.order_num from 
(select * from dm.dm_mdl_user_food_sample_day where dt='${day}' and  length(food_name)>1 and user_month_order_num<60) t1 join 
(
 select * from dm.dm_mdl_hotfood_rec_user_model_day where dt='${day}' and model='hotfood' and rec_app='hotfood_food_rec' 
    and rec_method in ('assoc_rule_level1', 'item_based', 'only_buy') 
) t2 on (t1.user_id=t2.user_id);

insert overwrite table temp.temp_rplc_user_food_rec_${dt} partition(step='part_buy')
select t.user_id, t.rec_method, t.food_name,  t.related_order_num, t.related_order_num from 
(
  select user_id, rec_method, food_name, related_order_num, row_number() over(partition by user_id order by related_order_num desc) rno 
  from temp.temp_rplc_user_food_rec_${dt} where step='temp_rplc_buy'
) t 
where t.rno<20 and (t.rno<15 or t.rec_method='only_buy');

insert overwrite table  temp.temp_rplc_user_food_rec_${dt} partition(step='temp_rplc_rec')
select t1.user_id, t1.rec_method, t2.rec_food_name, t1.related_order_num, 
t1.related_order_num* t2.probability *bound_data(t2.rec_food_price, 3, 50)/20.0 score from 
(
  select user_id, rec_method, related_order_num, food_name, case when rec_method='assoc_rule_level1' then 'current' else 'corr_01' end method
  from temp.temp_rplc_user_food_rec_${dt} where step='temp_rplc_buy' and rec_method in ('assoc_rule_level1', 'item_based')
) t1 join 
(select * from dm.dm_mdl_food_item_rec_day where dt='3000-12-31') t2 
on (t1.food_name=t2.prefix_food_name and t1.method=t2.model);

insert overwrite table  temp.temp_rplc_user_food_rec_${dt} partition(step='temp_rplc_rec_first')
select t.user_id,t.rec_method, t.food_name, t.related_order_num, t.rec_score from
(
  select user_id, rec_method, food_name, related_order_num, rec_score, row_number() over (partition by user_id, food_name order by rec_score desc) rno 
  from temp.temp_rplc_user_food_rec_${dt} where step='temp_rplc_rec'
) t
where t.rno=1; 


insert overwrite table  temp.temp_rplc_user_food_rec_${dt} partition(step='part_rec') 
select t1.user_id, t1.rec_method, t1.food_name, t1.related_order_num, t1.rec_score from 
(select * from temp.temp_rplc_user_food_rec_${dt} where step='temp_rplc_rec_first') t1 left outer join 
(select * from temp.temp_rplc_user_food_rec_${dt} where step='temp_rplc_buy') t2 
on (t1.user_id=t2.user_id and t1.food_name=t2.food_name) 
where t2.food_name is null;

drop table temp.temp_rplc_rec_user_food_item_${dt};
create table temp.temp_rplc_rec_user_food_item_${dt} as 
select user_id, rec_method, food_name, rec_score, is_fresh from 
(
select user_id, rec_method, food_name, round(rec_score, 2) rec_score, case when step='part_rec' then '1' else '0' end is_fresh, 
row_number() over (partition by user_id order by rec_score desc) rno    
from temp.temp_rplc_user_food_rec_${dt} where step like 'part_%'
) t 
where t.rno<20;

insert overwrite table rec.rec_hotfood_user_food_rec partition(dt='${day}', model='food_prefer_item_based') 
select user_id,'food_prefer', concat('{', concat_ws(',', collect_set(
concat('\"', food_name,'\":\{\"score\":', cast(rec_score as string), ', \"is_fresh\":', is_fresh, '\}'))), '}') info,
from_unixtime(unix_timestamp()) from temp.temp_rplc_rec_user_food_item_${dt} 
group by user_id;


