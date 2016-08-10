DROP TABLE temp.temp_food_tag_classification;
CREATE TABLE temp.temp_food_tag_classification(
key STRING, 
priority INT,
value STRING
) PARTITIONED BY (part STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ;

INSERT OVERWRITE TABLE temp.temp_food_tag_classification PARTITION(part='class1')
SELECT food_name_pattern, priority, normal_food_name
FROM dim.dim_mdl_common_food_name
WHERE part='base';



DROP TABLE temp.temp_food_tag_classification_temp;
CREATE TABLE temp.temp_food_tag_classification_temp(
key STRING, 
priority INT,
value STRING
) PARTITIONED BY (part STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/food_tag_c2' OVERWRITE INTO TABLE  temp.temp_food_tag_classification_temp PARTITION (part='class2');
LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/food_tag_c3' OVERWRITE INTO TABLE  temp.temp_food_tag_classification_temp PARTITION (part='class3');
LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/food_tag_c4' OVERWRITE INTO TABLE  temp.temp_food_tag_classification_temp PARTITION (part='class4');


INSERT OVERWRITE TABLE temp.temp_food_tag_classification PARTITION(part='class2')
SELECT concat_ws('','%',key,'%'), priority, value
FROM temp.temp_food_tag_classification_temp
WHERE part='class2';

INSERT OVERWRITE TABLE temp.temp_food_tag_classification PARTITION(part='class3')
SELECT concat_ws('','%',key,'%'), priority, value
FROM temp.temp_food_tag_classification_temp
WHERE part='class3';

INSERT OVERWRITE TABLE temp.temp_food_tag_classification PARTITION(part='class4')
SELECT concat_ws('','%',key,'%'), priority, value
FROM temp.temp_food_tag_classification_temp
WHERE part='class4';

LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/food_func_tag' OVERWRITE INTO TABLE  temp.temp_food_tag_classification_temp PARTITION (part='food_func');
LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/food_scene_tag' OVERWRITE INTO TABLE  temp.temp_food_tag_classification_temp PARTITION (part='food_scene');