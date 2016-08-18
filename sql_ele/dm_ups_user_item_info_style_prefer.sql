
#***************************************************************************************************
# ** 文件名称： dm_ups_user_item_info_style_prefer.sql
# ** 功能描述： 生成用户口味风格，并插入到用户画像中
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-18
#***************************************************************************************************

DROP TABLE temp.temp_food_tag_classification_mapping;
CREATE TABLE temp.temp_food_tag_classification_mapping AS
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

DROP TABLE temp.temp_food_tag_classification_style_mapping;
CREATE TABLE temp.temp_food_tag_classification_style_mapping AS
SELECT t2.key, t1.single_tag as style
FROM(
SELECT key, single_tag
FROM (SELECT key, tag FROM temp.temp_food_tag_classification WHERE part='food_tag_class4_func') t
LATERAL VIEW EXPLODE(split(tag,'#')) tmp AS single_tag
WHERE single_tag is not null and single_tag!='') t1
JOIN (
SELECT key, single_tag
FROM(SELECT key, concat_ws('#', tag, flavor, method) as food_tag FROM temp.temp_food_tag_classification_mapping) t
LATERAL VIEW EXPLODE(split(food_tag,'#')) tmp AS single_tag
) t2
ON t2.single_tag=t1.key;


DROP TABLE temp.temp_mdl_user_food_style_day;
CREATE TABLE temp.temp_mdl_user_food_style_day AS
SELECT t2.style, t1.user_id, sum(order_num) AS order_num_total, sum(total_price) AS total_price_total, 
max(last_order_time) AS last_order_time_max, sum(user_month_order_num) AS user_month_order_num_total, '${day}' as record_day
FROM (SELECT * FROM dm.dm_mdl_user_food_sample_day WHERE dt='${day}') t1
JOIN (SELECT * FROM temp.temp_food_tag_classification_style_mapping) t2
ON t2.key=t1.food_name
WHERE t2.style!='无口味' and t2.style!='暂无'
GROUP BY t1.user_id, t2.style;

drop table temp.temp_rec_user_style_score;
create table temp.temp_rec_user_style_score as
select user_id, style, 
(order_num_total/user_month_order_num_total)*bound_data(total_price_total,2,50)/(datediff(record_day,last_order_time_max)+1) as score
from temp.temp_mdl_user_food_style_day;

drop table temp.temp_rec_user_style_score_normalized;
create table temp.temp_rec_user_style_score_normalized as
select t.user_id, concat('{', concat_ws(',',collect_set(concat('\"',t.style,'\":','\"',t.score_normalized,'\"'))), '}') style_info
from(
select user_id, style, round(1/(1+exp(-score)),2) as score_normalized
from temp.temp_rec_user_style_score) t
group by t.user_id; 

drop table temp.temp_ups_user_style_prefer_partition;
create table temp.temp_ups_user_style_prefer_partition as 
select t.user_id, t.attr_value
from(
select 
case when t1.user_id is not null then t1.user_id else t2.user_id end as user_id,
case when t2.style_info is not null then t2.style_info else t1.attr_value end as attr_value 
from
(select user_id, attr_value
from dm.dm_ups_user_item_info
where flag='rec_style' and attr_key='style_prefer' and datediff('${day}',dt)=1) t1
full outer join 
(select user_id, style_info
from temp.temp_rec_user_style_score_normalized) t2
on t1.user_id=t2.user_id) t
group by t.user_id, t.attr_value;

insert overwrite table dm.dm_ups_user_item_info partition(dt='${day}', flag='rec_style')
select user_id, 'rec' as top_category,'style_prefer' as attr_key, attr_value, '1' as is_json, from_unixtime(unix_timestamp()) as update_time
from temp.temp_ups_user_style_prefer_partition;

insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='rec_style')
select user_id, 'rec' as top_category,'style_prefer' as attr_key, attr_value, '1' as is_json, from_unixtime(unix_timestamp()) as update_time
from temp.temp_ups_user_style_prefer_partition;





