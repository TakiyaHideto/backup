-- 创建
DROP TABLE temp.temp_food_tag_mapping_info;
CREATE TABLE temp.temp_food_tag_mapping_info (
key STRING, 
value STRING
) PARTITIONED BY (part STRING) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ;

LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/food_attr.txt' OVERWRITE INTO TABLE  temp.temp_food_tag_mapping_info PARTITION (part='food_attr_origin');
LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/scene_tag.txt' OVERWRITE INTO TABLE  temp.temp_food_tag_mapping_info PARTITION (part='scene_tag_origin');
LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/function_tag.txt' OVERWRITE INTO TABLE  temp.temp_food_tag_mapping_info PARTITION (part='function_tag_origin');

-- INSERT OVERWRITE TABLE temp.temp_food_tag_mapping_info PARTITION(part='normal_food_tag')
-- SELECT t1.name, t2.value
-- FROM ( 
-- SELECT t.name, single_seg_food_name
-- FROM(SELECT name, segmenter(name,true,'food_name.dic','stop.dic') AS seg_food_name FROM dw.dw_prd_food WHERE dt>'2016-07-20' AND name!='未知') t
-- LATERAL VIEW EXPLODE(seg_food_name) myTable AS single_seg_food_name
-- ) t1
-- JOIN (SELECT key, value FROM temp.temp_food_tag_mapping_info WHERE part='food_attr_origin') t2
-- ON t1.single_seg_food_name=t2.key
-- GROUP BY t1.name, t2.value;

INSERT OVERWRITE TABLE temp.temp_food_tag_mapping_info PARTITION(part='normal_food_tag')
SELECT t1.normalize_food_name, t2.value
FROM ( 
SELECT t.normalize_food_name, single_seg_food_name
FROM(SELECT normalize_food_name, segmenter(normalize_food_name,true,'food_name.dic','stop.dic') AS seg_food_name FROM dm.dm_mdl_food_name_normalize_day WHERE dt='2016-08-08') t
LATERAL VIEW EXPLODE(seg_food_name) myTable AS single_seg_food_name
) t1
JOIN (SELECT key, value FROM temp.temp_food_tag_mapping_info WHERE part='food_attr_origin') t2
ON t1.single_seg_food_name=t2.key
GROUP BY t1.normalize_food_name, t2.value;

INSERT OVERWRITE TABLE temp.temp_food_tag_mapping_info PARTITION(part='food_function')
SELECT t2.key, t1.key
FROM(
SELECT key, single_tag
FROM (SELECT key, value FROM temp.temp_food_tag_mapping_info WHERE (part='function_tag_origin')) t3
LATERAL VIEW EXPLODE(split(value,'#')) myTable AS single_tag 
) t1
JOIN(
SELECT key, single_tag
FROM (SELECT key, value FROM temp.temp_food_tag_mapping_info WHERE (part='normal_food_tag')) t4
LATERAL VIEW EXPLODE(split(value,'#')) myTable AS single_tag 
) t2
ON t1.single_tag=t2.single_tag
GROUP BY t2.key, t1.key;

INSERT OVERWRITE TABLE temp.temp_food_tag_mapping_info PARTITION(part='food_scene')
SELECT t2.key, t1.key
FROM(
SELECT key, single_tag
FROM (SELECT key, value FROM temp.temp_food_tag_mapping_info WHERE (part='scene_tag_origin')) t3
LATERAL VIEW EXPLODE(split(value,'#')) myTable AS single_tag 
) t1
JOIN(
SELECT key, single_tag
FROM (SELECT key, value FROM temp.temp_food_tag_mapping_info WHERE (part='normal_food_tag')) t4
LATERAL VIEW EXPLODE(split(value,'#')) myTable AS single_tag 
) t2
ON t1.single_tag=t2.single_tag
GROUP BY t2.key, t1.key;




