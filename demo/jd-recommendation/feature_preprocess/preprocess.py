import glob
import os
import sys
import pandas as pd
import xxhash
import shutil

# feature csv
feature_path = sys.argv[1]
save_path = os.path.dirname(__file__) + '/out'

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
# feature = spark.read.csv(feature_path)
# feature.show()

# or pandas
features = pd.concat([pd.read_csv(f)
                     for f in [i for i in glob.glob(feature_path + '/*.csv')]])
total_rows = features.shape[0]
print('feature total count:', total_rows)

# rename for deepfm train
features.rename(columns={'action_actionValue_multi_direct_2': 'Label',  # notice: this is label
                         'reqId_1': 'C1',
                         'flattenRequest_eventTime_original_0': 'C2',
                         'flattenRequest_reqId_original_1': 'C3',
                         'flattenRequest_pair_id_original_24': 'C4',
                         'flattenRequest_sku_id_original_25': 'C5',
                         'flattenRequest_user_id_original_26':	'C6',
                         'flattenRequest_pair_id_window_unique_count_27':	'I1',
                         'flattenRequest_pair_id_window_top1_ratio_28':	'I2',
                         'flattenRequest_pair_id_window_top1_ratio_29':	'I3',
                         'flattenRequest_pair_id_window_unique_count_32':	'I4',
                         'flattenRequest_pair_id_window_count_35': 'I5',
                         'flattenRequest_eventTime_dayofweek_41': 'C7',
                         'flattenRequest_eventTime_isweekday_43': 'C8',
                         'reqId_3': 'C9',
                         'bo_product_a1_multi_direct_3': 'C10',
                         'bo_product_a2_multi_direct_4': 'C11',
                         'bo_product_a3_multi_direct_5': 'C12',
                         'bo_product_br_multi_direct_6': 'C13',
                         'bo_product_cate_multi_direct_7': 'C14',
                         'bo_product_ingestionTime_multi_direct_8': 'C15',
                         'bo_user_age_multi_direct_9': 'C16',
                         'bo_user_ingestionTime_multi_direct_10': 'C17',
                         'bo_user_sex_multi_direct_11': 'C18',
                         'bo_user_user_lv_cd_multi_direct_12': 'C19',
                         'reqId_14': 'C20',
                         'bo_comment_bad_comment_rate_multi_max_13':	'I6',
                         'bo_comment_bad_comment_rate_multi_min_14':	'I7',
                         'bo_comment_bad_comment_rate_multi_min_15':	'I8',
                         'bo_comment_comment_num_multi_unique_count_22':	'I9',
                         'bo_comment_has_bad_comment_multi_unique_count_23': 'I10',
                         'bo_comment_has_bad_comment_multi_top3frequency_30': 'C21',
                         'bo_comment_comment_num_multi_top3frequency_33': 'C22',
                         'reqId_17': 'C23',
                         'bo_action_br_multi_top3frequency_16': 'C24',
                         'bo_action_cate_multi_top3frequency_17': 'C25',
                         'bo_action_model_id_multi_top3frequency_18': 'C26',
                         'bo_action_model_id_multi_unique_count_19': 'I11',
                         'bo_action_model_id_multi_unique_count_20': 'I12',
                         'bo_action_type_multi_unique_count_21': 'I13',
                         'bo_action_type_multi_top3frequency_40': 'C27',
                         'bo_action_type_multi_top3frequency_42': 'C28'},
                inplace=True)

cols = ['Label',
        'I1', 'I2', 'I3', 'I4', 'I5', 'I6', 'I7', 'I8', 'I9', 'I10', 'I11', 'I12', 'I13',
        'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10', 'C11', 'C12', 'C13', 'C14',
        'C15', 'C16', 'C17', 'C18', 'C19', 'C20', 'C21', 'C22', 'C23', 'C24', 'C25', 'C26', 'C27', 'C28']
# reorder columns
features = features[cols]


def generate_hash(val):
    res = []
    if val.name == 'Label':
        return val
    for i in val:
        test = xxhash.xxh64(str(i), seed=10)
        res.append(test.intdigest())
    return res


def convert_type(df):
    for col in df.columns:
        if col == 'Label':
            df[col] = df[col].astype('float32')
        else:
            df[col] = df[col].astype('int64')

# onehot encoding
features = features.apply(lambda x: generate_hash(x), axis=0)
convert_type(features)

# split to train/test/val ~ 0.8/0.1/0.1
train_rows = int(total_rows * 0.8)
test_rows = int(total_rows * 0.1)
# rest is val
val_rows = total_rows - train_rows - test_rows
# use dict to manage feature set
feature_set = {}
feature_set['train'] = features.iloc[0:train_rows]
feature_set['test'] = features.iloc[train_rows:train_rows+test_rows]
feature_set['val'] = features.iloc[train_rows+test_rows:]


for name, df in feature_set.items():
    print(f'{name} count:', df.shape[0])

    # re-mkdir first
    dir = f'{save_path}/{name}'
    shutil.rmtree(dir, ignore_errors=True)
    os.makedirs(dir)
    df.to_parquet(f'{dir}/{name}.parquet', engine='pyarrow', index=False)
    print(f'saved to {dir}')


del features['Label']
table_size = features.apply(lambda x: x.nunique(), axis=0)

print('table size array:\n', ','.join(map(str, table_size.array)))

print(f'saved to {save_path}/data_info.txt')
with open(f'{save_path}/data_info.txt', 'w') as text_file:
    text_file.write(str(train_rows) + '\n')
    text_file.write(str(test_rows) + '\n')
    text_file.write(str(val_rows) + '\n')
    text_file.write(','.join(map(str, table_size.array)))
