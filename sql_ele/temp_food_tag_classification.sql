DROP TABLE temp.temp_food_tag_classification;
CREATE TABLE temp.temp_food_tag_classification(
key STRING, 
priority INT,
tag STRING,
flavor STRING,
method STRING
) PARTITIONED BY (part STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ;

-- INSERT OVERWRITE TABLE temp.temp_food_tag_classification PARTITION(part='class1')
-- SELECT food_name_pattern, priority, normal_food_name
-- FROM dim.dim_mdl_common_food_name
-- WHERE part='base';

-- DROP TABLE temp.temp_food_tag_classification_temp;
-- CREATE TABLE temp.temp_food_tag_classification_temp(
-- key STRING, 
-- priority INT,
-- tag STRING,
-- flavor STRING,
-- method STRING
-- ) PARTITIONED BY (part STRING)
-- ROW FORMAT DELIMITED 
-- FIELDS TERMINATED BY '\t'
-- STORED AS TEXTFILE;

LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/food_tag_class1.txt' OVERWRITE INTO TABLE  temp.temp_food_tag_classification PARTITION (part='class1');
LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/food_tag_class2.txt' OVERWRITE INTO TABLE  temp.temp_food_tag_classification PARTITION (part='class2');
LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/food_tag_class3.txt' OVERWRITE INTO TABLE  temp.temp_food_tag_classification PARTITION (part='class3');
LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/food_tag_class4.txt' OVERWRITE INTO TABLE  temp.temp_food_tag_classification PARTITION (part='class4');

INSERT OVERWRITE TABLE temp.temp_food_tag_classification PARTITION(part='class1_3')
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

-- INSERT OVERWRITE TABLE temp.temp_food_tag_classification PARTITION(part='class2')
-- SELECT concat_ws('','%',key,'%'), priority, value
-- FROM temp.temp_food_tag_classification_temp
-- WHERE part='class2';

-- INSERT OVERWRITE TABLE temp.temp_food_tag_classification PARTITION(part='class3')
-- SELECT concat_ws('','%',key,'%'), priority, value
-- FROM temp.temp_food_tag_classification_temp
-- WHERE part='class3';

-- INSERT OVERWRITE TABLE temp.temp_food_tag_classification PARTITION(part='class4')
-- SELECT concat_ws('','%',key,'%'), priority, value
-- FROM temp.temp_food_tag_classification_temp
-- WHERE part='class4';

-- LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/food_func_tag' OVERWRITE INTO TABLE  temp.temp_food_tag_classification PARTITION (part='food_func');
-- LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/food_scene_tag' OVERWRITE INTO TABLE  temp.temp_food_tag_classification PARTITION (part='food_scene');
