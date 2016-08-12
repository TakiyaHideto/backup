#***************************************************************************************************
# ** 文件名称： dm.dm_ups_user_item_info_food_prefer.sql
# ** 功能描述： 生成用户食品偏好，并插入到用户画像中
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-11
#***************************************************************************************************

drop table temp.temp_rec_user_food_score;
create table temp.temp_rec_user_food_score as
select user_id, food_name, 
bound_data((order_num/user_month_order_num)*total_price/datediff(dt,last_order_time),1,10) as score
from dm.dm_mdl_user_food_sample_day
where dt='${day}';

drop table temp.temp_rec_user_food_score_max_min;
create table temp.temp_rec_user_food_score_max_min as 
select user_id, max(score) as score_max, min(score) as score_min
from temp.temp_rec_user_food_score
group by user_id;

drop table temp.temp_rec_user_food_score_normalized;
create table temp.temp_rec_user_food_score_normalized as
select t.user_id, concat('{', concat_ws(',',collect_set(concat('\"',t.food_name,'\":','\"',t.score_normalized,'\"'))), '}') food_info
from(
select t1.user_id, t1.food_name, round((t1.score-t2.score_min)/(t2.score_max-t2.score_min),2) as score_normalized
from temp.temp_rec_user_food_score t1
join temp.temp_rec_user_food_score_max_min t2
on t1.user_id=t2.user_id) t
group by t.user_id;

drop table temp.temp_ups_user_food_prefer_partition;
create table temp.temp_ups_user_food_prefer_partition as 
select 
case when t1.user_id is not null then t1.user_id else t2.user_id end as user_id,
case when t2.food_info is not null then t2.food_info else t1.attr_value end as attr_value 
from
(select user_id, attr_value
from dm.dm_ups_user_item_info
where flag='rec_food' and attr_key='food_prefer' ) t1
full outer join 
(select user_id, food_info
from temp.temp_rec_user_food_score_normalized) t2
on t1.user_id=t2.user_id;
select * from temp_ups_user_food_prefer_partition limit 100;

insert overwrite table dm.dm_ups_user_item_info partition(dt='${day}', flag='food_prefer')
select user_id, 'food_prefer' as attr_key, attr_value, '1' as is_json, from_unixtime(unix_timestamp()) as update_time
from temp.temp_ups_user_food_prefer_partition
