#***************************************************************************************************
# ** 文件名称： dim_mdl_common_food_name_history.sql
# ** 功能描述： 备份dim.dim_mdl_common_food_name
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-18
#***************************************************************************************************

INSERT OVERWRITE TABLE dim.dim_mdl_common_food_name_history PARTITION(dt='${day}', part='base')
SELECT * 
FROM dim.dim_mdl_common_food_name
WHERE part='base'


