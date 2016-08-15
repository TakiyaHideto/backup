#***************************************************************************************************
# ** 文件名称： dm_ups_user_item_info_flavor_prefer.sql
# ** 功能描述： 生成用户口味偏好，并插入到用户画像中
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-15
#***************************************************************************************************

DROP TABLE temp.temp_food_tag_classification_flavor_mapping;
CREATE TABLE temp.temp_food_tag_classification_flavor_mapping AS
SELECT t4.key, t4.priority, t3.tag, t3.flavor, t3.method
FROM(
SELECT t2.key, t2.priority, t1.tag, t1.flavor, t1.method
FROM (
SELECT *
FROM temp.temp_food_tag_classification
WHERE part='class3'
) t1
JOIN (
SELECT * 
FROM temp.temp_food_tag_classification
WHERE part='class2'
) t2
ON t1.key=t2.tag
) t3
JOIN (
SELECT *
FROM temp.temp_food_tag_classification
WHERE part='class1'
) t4
ON t3.key=t4.tag;

DROP TABLE temp.temp_mdl_user_food_flavor_day;
CREATE TABLE temp.temp_mdl_user_food_flavor_day AS
SELECT t2.flavor, t1.user_id, sum(order_num) AS order_num_total, sum(total_price) AS total_price_total, 
max(last_order_time) AS last_order_time_max, sum(user_month_order_num) AS user_month_order_num_total, '${day}' as record_day
FROM (SELECT * FROM dm.dm_mdl_user_food_sample_day WHERE dt='${day}') t1
JOIN (SELECT * FROM temp.temp_food_tag_classification_flavor_mapping) t2
ON t2.key=t1.food_name
WHERE t2.flavor!='无口味' and t2.flavor!='暂无'
GROUP BY t1.user_id, t2.flavor;

drop table temp.temp_rec_user_flavor_score;
create table temp.temp_rec_user_flavor_score as
select user_id, flavor, 
(order_num_total/user_month_order_num_total)*bound_data(total_price_total,2,50)/datediff(record_day,last_order_time_max) as score
from temp.temp_mdl_user_food_flavor_day;

drop table temp.temp_rec_user_flavor_score_normalized;
create table temp.temp_rec_user_flavor_score_normalized as
select t.user_id, concat('{', concat_ws(',',collect_set(concat('\"',t.flavor,'\":','\"',t.score_normalized,'\"'))), '}') flavor_info
from(
select user_id, flavor, round(1/(1+exp(-score)),2) as score_normalized
from temp.temp_rec_user_flavor_score) t
group by t.user_id; 

drop table temp.temp_ups_user_flavor_prefer_partition;
create table temp.temp_ups_user_flavor_prefer_partition as 
select t.user_id, t.attr_value
from(
select 
case when t1.user_id is not null then t1.user_id else t2.user_id end as user_id,
case when t2.flavor_info is not null then t2.flavor_info else t1.attr_value end as attr_value 
from
(select user_id, attr_value
from dm.dm_ups_user_item_info
where flag='rec_flavor' and attr_key='flavor_prefer' ) t1
full outer join 
(select user_id, flavor_info
from temp.temp_rec_user_flavor_score_normalized) t2
on t1.user_id=t2.user_id) t
group by t.user_id, t.attr_value;

insert overwrite table dm.dm_ups_user_item_info partition(dt='${day}', flag='rec_flavor')
select user_id, 'rec' as top_category,'flavor_prefer' as attr_key, attr_value, '1' as is_json, from_unixtime(unix_timestamp()) as update_time
from temp.temp_ups_user_food_prefer_partition;

insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='rec_flavor')
select user_id, 'rec' as top_category,'flavor_prefer' as attr_key, attr_value, '1' as is_json, from_unixtime(unix_timestamp()) as update_time
from temp.temp_ups_user_food_prefer_partition;





