 CREATE DATABASE IF NOT EXISTS JD_db;
 USE JD_db;
 CREATE TABLE IF NOT EXISTS action(reqId string, eventTime timestamp, ingestionTime timestamp, actionValue int);
 CREATE TABLE IF NOT EXISTS flattenRequest(reqId string, eventTime timestamp, main_id string, pair_id string, user_id string, sku_id string, time bigint, split_id int, time1 string);
 CREATE TABLE IF NOT EXISTS bo_user(ingestionTime timestamp, user_id string, age string, sex string, user_lv_cd string, user_reg_tm bigint);
 CREATE TABLE IF NOT EXISTS bo_action(ingestionTime timestamp, pair_id string, time bigint, model_id string, type string, cate string, br string);
 CREATE TABLE IF NOT EXISTS bo_product(ingestionTime timestamp, sku_id string, a1 string, a2 string, a3 string, cate string, br string);
 CREATE TABLE IF NOT EXISTS bo_comment(ingestionTime timestamp, dt bigint, sku_id string, comment_num int, has_bad_comment string, bad_comment_rate float);
