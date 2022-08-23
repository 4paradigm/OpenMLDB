import os,sys
import glob
import pandas as pd
import xxhash
import numpy as np

dataset = sys.argv[1]
dstdir = sys.argv[2]

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
#export to csv

combined_csv.rename(columns={'reqId_1': 'C1','flattenRequest_eventTime_original_0': 'C2','flattenRequest_reqId_original_1': 'C3','flattenRequest_pair_id_original_24': 'C4','flattenRequest_sku_id_original_25': 'C5','flattenRequest_user_id_original_26':	'C6','flattenRequest_pair_id_window_unique_count_27':	'I1','flattenRequest_pair_id_window_top1_ratio_28':	'I2','flattenRequest_pair_id_window_top1_ratio_29':	'I3','flattenRequest_pair_id_window_unique_count_32':	'I4','flattenRequest_pair_id_window_count_35': 'I5','flattenRequest_eventTime_dayofweek_41': 'C7','flattenRequest_eventTime_isweekday_43': 'C8','reqId_3': 'C9','action_actionValue_multi_direct_2': 'Label','bo_product_a1_multi_direct_3': 'C10','bo_product_a2_multi_direct_4': 'C11','bo_product_a3_multi_direct_5': 'C12','bo_product_br_multi_direct_6': 'C13','bo_product_cate_multi_direct_7':'C14','bo_product_ingestionTime_multi_direct_8':'C15','bo_user_age_multi_direct_9': 'C16','bo_user_ingestionTime_multi_direct_10':'C17','bo_user_sex_multi_direct_11':'C18','bo_user_user_lv_cd_multi_direct_12':'C19','reqId_14':'C20','bo_comment_bad_comment_rate_multi_max_13':	'I6','bo_comment_bad_comment_rate_multi_min_14':	'I7','bo_comment_bad_comment_rate_multi_min_15':	'I8','bo_comment_comment_num_multi_unique_count_22':	'I9','bo_comment_has_bad_comment_multi_unique_count_23': 'I10','bo_comment_has_bad_comment_multi_top3frequency_30': 'C21','bo_comment_comment_num_multi_top3frequency_33': 'C22','reqId_17': 'C23','bo_action_br_multi_top3frequency_16': 'C24','bo_action_cate_multi_top3frequency_17': 'C25','bo_action_model_id_multi_top3frequency_18': 'C26','bo_action_model_id_multi_unique_count_19': 'I11','bo_action_model_id_multi_unique_count_20': 'I12','bo_action_type_multi_unique_count_21': 'I13','bo_action_type_multi_top3frequency_40': 'C27','bo_action_type_multi_top3frequency_42': 'C28'}, inplace=True)

#combined_csv.to_csv( "combined_csv.csv", index=False)

def generate_hash(val):
    res = []
    if val.name == 'Label':
        return val
    for i in val:
        test = xxhash.xxh64(str(i), seed = 10)
        res.append(test.intdigest())
    return res


cols = ['Label',
        'I1', 'I2', 'I3', 'I4', 'I5', 'I6', 'I7', 'I8', 'I9', 'I10', 'I11', 'I12', 'I13', 
        'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7','C8', 'C9', 'C10', 'C11', 'C12', 'C13', 'C14', 
        'C15', 'C16', 'C17', 'C18', 'C19', 'C20', 'C21', 'C22','C23', 'C24', 'C25', 'C26', 'C27', 'C28']


df = combined_csv[cols]
df= df.apply(lambda x: generate_hash(x), axis = 0)


for col in df.columns:
    if col == 'Label':
        df[col] = df[col].astype('float32') 
    else:
        df[col] = df[col].astype('int64')
df.to_parquet(dstdir+dataset+'.parquet', engine='pyarrow', index=False)
    
sample_size = df['Label'].size
print(dataset + " samples = " + str(sample_size))

