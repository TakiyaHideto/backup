#***************************************************************************************************
# ** 文件名称： dm_log_hotfood_lost_user_day_inc.sql
# ** 功能描述： 热卖美食流失流量分析表
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-17
#***************************************************************************************************

DROP TABLE temp.temp_log_hotfood_exposure_order_buy;
CREATE TABLE temp.temp_log_hotfood_exposure_order_buy AS
SELECT t1.session_id, t2.user_id, t2.restaurant_id, t2.is_click, t2.restaurant_index
FROM(
SELECT session_id, max(is_buy) as buy_flag
FROM dw.dw_log_hotfood_exposure_click_buy_day_inc
WHERE dt='${day}'
GROUP BY session_id
HAVING buy_flag<1) t1
JOIN(
SELECT session_id, user_id, restaurant_id, is_click, restaurant_index
FROM dw.dw_log_hotfood_exposure_click_buy_day_inc
WHERE dt='${day}'
) t2
ON t1.session_id=t2.session_id
GROUP BY t1.session_id, t2.user_id, t2.restaurant_id, t2.is_click, t2.restaurant_index;


DROP TABLE temp.temp_log_order_session;
CREATE TABLE temp.temp_log_order_session AS
SELECT t1.session_id, max(t1.user_id) as user_id, t2.order_id, 
t2.restaurant_id, t2.channel_source
FROM temp.temp_log_hotfood_exposure_order_buy t1
JOIN(
SELECT session_id, order_id, restaurant_id, channel_source
FROM dw.dw_log_order_session_day_inc
WHERE dt='${day}'
) t2
ON t1.session_id=t2.session_id
GROUP BY t1.session_id, t2.order_id, t2.restaurant_id, t2.channel_source;


DROP TABLE temp.temp_log_app_order_session_source;
CREATE TABLE temp.temp_log_app_order_session_source AS
SELECT t1.session_id, t1.user_id, t2.eleme_device_id, t1.order_id, t1.channel_source, 
t2.activity_id, t2.title, t1.restaurant_id, t2.restaurant_type,
t2.total as total_price
FROM temp.temp_log_order_session t1
JOIN(
SELECT *
FROM dm.dm_log_app_order_source_day_inc
WHERE dt='${day}' and title not like '%热卖%'
) t2
ON t1.order_id=t2.order_id;


DROP TABLE temp.temp_user_order_activity_list;
CREATE TABLE temp.temp_user_order_activity_list AS
SELECT t1.session_id, t1.user_id, t1.eleme_device_id, t1.order_id, t1.activity_id, 
t1.restaurant_id, t1.restaurant_type, t1.total_price,
t1.channel_source, t1.title, t2.module_name, t2.name_cn
FROM temp.temp_log_app_order_session_source t1
JOIN(
SELECT id, module_name, name_cn
FROM dim.dim_log_activity
) t2
ON t1.activity_id=t2.id;


DROP TABLE temp.temp_session_without_order;
CREATE TABLE temp.temp_session_without_order AS
SELECT t.session_id, max(t.order_id) as order_id
FROM(
SELECT session_id, null as order_id
FROM temp.temp_log_hotfood_exposure_order_buy
UNION ALL
SELECT session_id, order_id
FROM dw.dw_log_order_session_day_inc
WHERE dt='${day}'
) t
GROUP BY t.session_id
HAVING order_id is null;


DROP TABLE temp.temp_mdl_traffic_nonhotfood_session_order;
CREATE TABLE temp.temp_mdl_traffic_nonhotfood_session_order AS
SELECT t2.session_id, t2.user_id, t2.eleme_device_id, t2.order_id, t2.activity_id,
t2.restaurant_id, t1.is_exposed_hotfood, t1.is_click_hotfood,
case when t1.restaurant_index=-1 then null else t1.restaurant_index end as restaurant_index,
t2.total_price, t2.channel_source, t2.title, t2.module_name, t2.name_cn
FROM(
SELECT session_id, user_id, restaurant_id, max(is_exposed_hotfood) as is_exposed_hotfood, 
max(is_click_hotfood) as is_click_hotfood, max(restaurant_index) as restaurant_index
FROM (
SELECT session_id, user_id, restaurant_id, 1 as is_exposed_hotfood, is_click as is_click_hotfood, restaurant_index
FROM temp.temp_log_hotfood_exposure_order_buy
UNION ALL
SELECT session_id, user_id, restaurant_id, 0 as is_exposed_hotfood, 0 as is_click_hotfood, -1 as restaurant_index
FROM temp.temp_user_order_activity_list
) t
GROUP BY session_id, user_id, restaurant_id
) t1
JOIN temp.temp_user_order_activity_list t2
ON t1.session_id=t2.session_id and t1.user_id=t2.user_id and t1.restaurant_id=t2.restaurant_id
UNION ALL
SELECT session_id, null as user_id, null as eleme_device_id, null as order_id, 
null as activity_id, null as restaurant_id, null as is_exposed_hotfood, null as is_click_hotfood, 
null as restaurant_index, null as total_price, null as channel_source, null as title, null as module_name, null as name_cn
FROM temp.temp_session_without_order
;


INSERT OVERWRITE TABLE dm.dm_log_hotfood_lost_user_day_inc PARTITION(dt='${day}')
SELECT eleme_device_id, session_id, user_id, order_id, activity_id, title, module_name, restaurant_id,
is_exposed_hotfood, is_click_hotfood, restaurant_index, channel_source, name_cn
FROM temp.temp_mdl_traffic_nonhotfood_session_order;


