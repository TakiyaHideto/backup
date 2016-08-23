#***************************************************************************************************
# ** 文件名称： dm_ups_user_item_info_category_prefer.sql
# ** 功能描述： 生成用户大类偏好，并插入到用户画像中
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-22
#***************************************************************************************************

DROP TABLE temp.temp_mdl_user_food_sample_cate_day;
CREATE TABLE temp.temp_mdl_user_food_sample_cate_day AS
SELECT t1.tag as cate, t2.user_id, sum(t2.order_num) as order_num, sum(t2.total_price) as total_price,
max(t2.last_order_time) as last_order_time, sum(t2.user_month_order_num) as user_month_order_num
FROM(
SELECT key, tag
FROM dim.dim_mdl_food_tag_classification 
WHERE part='class1_3') t1
JOIN(
SELECT *
FROM dm.dm_mdl_user_food_sample_day
WHERE dt='${day}'
) t2
ON t1.key=t2.food_name
GROUP BY t1.tag, t2.user_id;

DROP TABLE temp.temp_rec_user_cate_score;
CREATE TABLE temp.temp_rec_user_cate_score as
select user_id, cate, 
(order_num/user_month_order_num)*bound_data(total_price,2,50)/datediff('${day}',last_order_time) as score
from temp.temp_mdl_user_food_sample_cate_day;

drop table temp.temp_rec_user_cate_score_normalized;
create table temp.temp_rec_user_cate_score_normalized as
select t.user_id, concat('{', concat_ws(',',collect_set(concat('\"',t.cate,'\":','\"',t.score_normalized,'\"'))), '}') food_info
from(
select user_id, cate, round(1/(1+exp(-score)),2) as score_normalized
from temp.temp_rec_user_cate_score) t
group by t.user_id; 

drop table temp.temp_ups_user_food_prefer_partition;
create table temp.temp_ups_user_food_prefer_partition as 
select t.user_id, t.attr_value
from(
select 
case when t1.user_id is not null then t1.user_id else t2.user_id end as user_id,
case when t2.food_info is not null then t2.food_info else t1.attr_value end as attr_value 
from
(select user_id, attr_value
from dm.dm_ups_user_item_info
where flag='rec_category' and attr_key='category_prefer' and datediff('${day}',dt)=1) t1
full outer join 
(select user_id, food_info
from temp.temp_rec_user_cate_score_normalized) t2
on t1.user_id=t2.user_id) t
group by t.user_id, t.attr_value;

insert overwrite table dm.dm_ups_user_item_info partition(dt='${day}', flag='rec_category')
select user_id, 'rec' as top_category,'category_prefer' as attr_key, attr_value, '1' as is_json, from_unixtime(unix_timestamp()) as update_time
from temp.temp_ups_user_food_prefer_partition;

insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='rec_category')
select user_id, 'rec' as top_category,'category_prefer' as attr_key, attr_value, '1' as is_json, from_unixtime(unix_timestamp()) as update_time
from temp.temp_ups_user_food_prefer_partition;