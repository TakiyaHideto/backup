#***************************************************************************************************
# ** 文件名称： dim_mdl_extract_rec_food_json_key.sql
# ** 功能描述： 
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-09-01
#***************************************************************************************************

DROP TABLE temp.temp_ups_user_json_key_extraction;
CREATE TABLE temp.temp_ups_user_json_key_extraction(
top_category STRING, 
key STRING,
value STRING
) PARTITIONED BY (part STRING); 

INSERT OVERWRITE TABLE temp.temp_ups_user_json_key_extraction PARTITION(part='rec_style')
SELECT t.top_category, t.attr_key AS key, value
FROM(
SELECT top_category, attr_key, get_json_keys(attr_value) AS json_keys
FROM dm.dm_ups_user_item_info
WHERE dt='3000-12-31' and flag='rec_style' and attr_value!='\\{\\}'
) t 
LATERAL VIEW EXPLODE(t.json_keys) tmp AS value
GROUP BY t.top_category, t.attr_key, value;

INSERT OVERWRITE TABLE temp.temp_ups_user_json_key_extraction PARTITION(part='rec_food')
SELECT t.top_category, t.attr_key AS key, value
FROM(
SELECT top_category, attr_key, get_json_keys(attr_value) AS json_keys
FROM dm.dm_ups_user_item_info
WHERE dt='3000-12-31' and flag='rec_food' and attr_value!='{}'
) t 
LATERAL VIEW EXPLODE(t.json_keys) tmp AS value
GROUP BY t.top_category, t.attr_key, value;

-- INSERT OVERWRITE TABLE temp.temp_ups_user_json_key_extraction PARTITION(part='rec_cooking')
-- SELECT t.top_category, t.attr_key AS key, value
-- FROM(
-- SELECT top_category, attr_key, get_json_keys(attr_value) AS json_keys
-- FROM dm.dm_ups_user_item_info
-- WHERE dt='3000-12-31' and flag='rec_cooking' and attr_value!='\\{\\}'
-- ) t 
-- LATERAL VIEW EXPLODE(t.json_keys) tmp AS value
-- GROUP BY t.top_category, t.attr_key, value;

INSERT OVERWRITE TABLE temp.temp_ups_user_json_key_extraction PARTITION(part='rec_flavor')
SELECT t.top_category, t.attr_key AS key, value
FROM(
SELECT top_category, attr_key, get_json_keys(attr_value) AS json_keys
FROM dm.dm_ups_user_item_info
WHERE dt='3000-12-31' and flag='rec_flavor' and attr_value!='{}'
) t 
LATERAL VIEW EXPLODE(t.json_keys) tmp AS value
GROUP BY t.top_category, t.attr_key, value;

INSERT OVERWRITE TABLE temp.temp_ups_user_json_key_extraction PARTITION(part='rec_category')
SELECT t.top_category, t.attr_key AS key, value
FROM(
SELECT top_category, attr_key, get_json_keys(attr_value) AS json_keys
FROM dm.dm_ups_user_item_info
WHERE dt='3000-12-31' and flag='rec_category' and attr_value!='{}'
) t 
LATERAL VIEW EXPLODE(t.json_keys) tmp AS value
GROUP BY t.top_category, t.attr_key, value;


INSERT OVERWRITE TABLE dim.dim_ups_key_value_info
SELECT *
FROM temp.temp_ups_user_json_key_extraction
WHERE part is not null
GROUP BY top_category, key, value;


