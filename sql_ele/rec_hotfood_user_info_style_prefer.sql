#***************************************************************************************************
# ** 文件名称： rec_hotfood_user_info_style_prefer.sql
# ** 功能描述： 生成用户功能标签偏好，并导入user_info中
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-30
#***************************************************************************************************


DROP TABLE temp.temp_hotfood_user_info_style_prefer_from_ups;
CREATE TABLE temp.temp_hotfood_user_info_style_prefer_from_ups AS
SELECT user_id, split(single_style_info,':')[0] AS style_name, split(single_style_info,':')[1] AS style_score
FROM(
SELECT user_id, regexp_replace(attr_value,"\\{|\\}","") AS style_info
FROM dm.dm_ups_user_item_info 
WHERE dt='3000-12-31' and flag='rec_style'
) t
LATERAL VIEW EXPLODE(split(t.style_info,',')) tmp AS single_style_info;

INSERT OVERWRITE TABLE rec.rec_hotfood_user_food_rec PARTITION(dt='${day}', model='food_prefer_style_info')
SELECT user_id,'tag_prefer', concat('{', concat_ws(',', collect_set(
concat(style_name,':\{\"score\":', cast(style_score AS string), ', \"is_fresh\":', '0', '\}'))), '}') info, from_unixtime(unix_timestamp()) 
FROM temp.temp_hotfood_user_info_style_prefer_from_ups 
GROUP BY user_id;
