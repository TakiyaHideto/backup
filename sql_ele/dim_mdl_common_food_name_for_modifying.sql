#***************************************************************************************************
# ** 文件名称： dim_mdl_common_food_name_for_modifying.sql
# ** 功能描述： 
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-22
#***************************************************************************************************

INSERT OVERWRITE TABLE dim.dim_mdl_common_food_name PARTITION(part='base')
SELECT t1.food_name_pattern, t1.normal_food_name, t1.priority, t1.last_update_time, t2.tag_function, t1.tag_scene, t1.category, t1.flavor
FROM(
SELECT food_name_pattern, normal_food_name, priority, last_update_time, tag_scene, category, flavor
FROM dim.dim_mdl_common_food_name_history
WHERE part='base' and dt='${day}'
) t1
LEFT OUTER JOIN(
SELECT key, collect_set(func) as tag_function
FROM dim.dim_mdl_food_tag_classification
WHERE part='class1_func'
GROUP BY key
) t2
ON t1.normal_food_name=t2.key;