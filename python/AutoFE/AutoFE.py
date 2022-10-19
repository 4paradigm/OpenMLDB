from autox.autox import AutoX
from autox.autox_competition.util import log
from autox.autox_competition.process_data.feature_type_recognition import Feature_type_recognition
import re
import requests
import os
import sys
import time
import shutil
import pandas as pd
from sklearn.model_selection import train_test_split


class openMLDB_sql_generator():
    def __init__(self, target, train_name, test_name, path, time_series=False, ts_unit=None, time_col=None,
                 metric='rmse', feature_type={}, relations=[], id=[], task_type='regression',
                 Debug=False, image_info={}, target_map={},output_file_path="/tmp/",table_info_dict={},table_list=[]):
        self.Debug = Debug
        self.info_ = {}
        self.info_['id'] = id
        self.info_['path'] = path
        self.info_['task_type'] = task_type
        self.info_['target'] = target
        self.info_['feature_type'] = feature_type
        self.info_['relations'] = relations
        self.info_['train_name'] = train_name
        self.info_['test_name'] = test_name
        self.info_['metric'] = metric
        self.info_['time_series'] = time_series
        self.info_['ts_unit'] = ts_unit
        self.info_['time_col'] = time_col
        self.info_['image_info'] = image_info
        self.info_['target_map'] = target_map
        self.info_['output_file_path'] = output_file_path
        self.original_column_name_list=[]
        #self.table_list=table_list
        self.info_['table_list']=table_list
        self.info_['union_table_list'] = table_list
        self.info_['table_info_dict'] = table_info_dict
        for k in self.info_['feature_type'][self.info_['train_name']]:
            self.original_column_name_list.append(k)

        if Debug:
            log("Debug mode, sample data")
            self.dfs_[train_name] = self.dfs_[train_name].sample(5000)
        if feature_type == {}:
            for table_name in self.dfs_.keys():
                df = self.dfs_[table_name]
                feature_type_recognition = Feature_type_recognition()
                feature_type = feature_type_recognition.fit(df)
                self.info_['feature_type'][table_name] = feature_type

    def add_feature_column(self, processed_column_name_list, new_csv_filename):
        print("")

        feature_type = {}  # self.info_['feature_type']
        feature_type[new_csv_filename + "_train.csv"] = {}
        feature_type[new_csv_filename + "_test.csv"] = {}
        for i in processed_column_name_list:
            # for csv_list in feature_type:
            #    feature_type[csv_list][i]="num"
            feature_type[new_csv_filename + "_train.csv"][i] = "num"
            if not i == self.info_['target']:
                feature_type[new_csv_filename + "_test.csv"][i] = "num"
            # TODO:check whether all "num"

        # print(feature_type['train2.csv'])
        return feature_type
    def print_windows(self):
        sql=""
        for p, window_now in enumerate(self.window_list):
            sql += window_now["name"]
            sql += " AS ("
            if(not window_now["UNION"]==""):
                sql += " UNION " + window_now["UNION"]
            sql += " PARTITION BY " + window_now["PARTITION BY"]
            sql += " ORDER BY " + window_now["ORDER BY"]
            sql += " " + window_now["ROWS"]
            sql += " BETWEEN " + window_now["BETWEEN"]
            sql += " " + window_now["PRE"]
            if p == len(self.window_list) - 1:
                sql += ")\n"
            else:
                sql += "),\n"
        return sql

    def time_series_feature_sql(self):
        # recipe is as follows
        '''
        SELECT col1,
        sum(col1) over w1 as col1_w1
        col2 over w2 as col2_w2
        ...
        from table1
        WINDOW
        w1 AS ..rows_range/rows BETWEEN []
        w2 AS ..
        INTO OUTFILE ..
        '''
        shift_dict = {}
        shift_dict['year'] = [1, 2, 3, 4, 5, 10, 20]
        shift_dict['month'] = [1, 2, 3, 4, 8, 12, 24, 60, 120]
        shift_dict['day'] = [1, 2, 3, 7, 14, 21, 30, 60 , 90, 182, 365]
        shift_dict['minute'] = [1, 2, 3, 5, 10, 15, 30, 45, 60, 120, 240, 720, 1440]

        col_name_dict = {}
        col_name_dict['datetime'] = []
        col_name_dict['num'] = []
        col_name_dict['cat'] = []
        for k, v in self.info_['feature_type'][self.info_['train_name']].items():
            if (k) not in col_name_dict[v]:
                col_name_dict[v].append(k)
        # col_name_dict['datetime']=['pickup_datetime', 'dropoff_datetime']
        # col_name_dict['num']= ['pickup_latitude', 'dropoff_latitude', 'pickup_longitude', 'dropoff_longitude', 'passenger_count']#, 'trip_duration']
        # col_name_dict['cat']=['vendor_id', 'id', 'store_and_fwd_flag']

        function_list = ['sum', 'avg', 'min', 'max', 'count', 'log', 'lag0']  # ,'log','lag0']
        lag_num_list = shift_dict['day']
        toUseLag = True
        if toUseLag:
            for l, lag_num in enumerate(lag_num_list):
                function_list.append('lag' + str(lag_num))
                function_list.append('lag' + str(lag_num) + '-0')

        self.info_['table_list'] = [self.info_['table_list'][0]]

        '''
        w AS (PARTITION BY vendor_id ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW),
        w2 AS (PARTITION BY passenger_count ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW);
        '''

        shift_dict['window_range'] = [1, 2, 3, 7, 14, 21, 30, 60 , 90, 182, 365]
        self.window_list = [{"name": "w1",
                             "UNION": "",
                              "PARTITION BY": "vendor_id",
                              "ORDER BY": "pickup_datetime",
                              "ROWS": "ROWS_RANGE",
                              "BETWEEN": "1d",
                              "PRE": "PRECEDING AND CURRENT ROW"
                              },
                            {"name": "w2",
                             "UNION": "",
                             "PARTITION BY": "passenger_count",
                             "ORDER BY": "pickup_datetime",
                             "ROWS": "ROWS_RANGE",
                             "BETWEEN": "1d",
                             "PRE": "PRECEDING AND CURRENT ROW"
                             }
                            ]
        #TODO: to automatic generate window dict :
        # (PARTITION BY all categoric columns,ORDER BY all time columns, BETWEEN more minutes/hours/days/months/years)

        sql = "SELECT "
        multi_operator_func_list = ['lag']
        # current_window_name="w1"
        #self.original_column_name_list = pd.read_csv(
        #    self.info_['path'] + self.info_['train_name']).columns.values.tolist()
        self.processed_column_name_list =  pd.read_csv(
            self.info_['path'] + self.info_['train_name']).columns.values.tolist()
        self.column_name2sql_dict={}

        ##union window definition
        to_union=False
        if to_union:
            for union_table_i,union_table in enumerate(self.info_['union_table_list']):
                if union_table_i>0:
                    window_number_original=len(self.window_list)
                    for w in range(window_number_original):
                        self.window_list.append(self.window_list[w].copy())
                        self.window_list[len(self.window_list)-1]["UNION"]=union_table
                        self.window_list[len(self.window_list) - 1]["name"] = self.window_list[len(self.window_list) - 1]["name"]+"_union_"+union_table

        for w, window in enumerate(self.window_list):
            for data_type_in_col_name_list,v_in_col_name_dict in col_name_dict.items():
                for col_name_i, col_name in enumerate(col_name_dict[data_type_in_col_name_list]):
                    if w == 0 :#and not self.info_['table_info_dict'][self.info_['table_list'][0]]['column_type'][col_name]=='string': #string type will not counted as feature
                        sql += col_name
                        sql += ","
            '''for col_name_i, col_name in enumerate(col_name_dict['datetime']):
                if w == 0:
                    sql += col_name
                    sql += ","
            for col_name_i, col_name in enumerate(col_name_dict['num']):
                if w == 0:
                    sql += col_name
                    sql += ","'''
            for col_name_i, col_name in enumerate(col_name_dict['num']):
                if not col_name == self.info_['target']:
                    for func_i, func in enumerate(function_list):
                        have_multi_op = False
                        multi_op_index = 0
                        func_processed_name = func.replace("-", "minus").replace("+", "add").replace("*",
                                                                                                     "multiply").replace(
                                                                                                    "/", "divide")
                        column_sql=""
                        for op_i, op in enumerate(multi_operator_func_list):
                            if func.startswith(op) and len(func) > len(op):
                                #sql += op
                                column_sql+=op
                                have_multi_op = True
                                multi_op_index = op_i
                                break
                        '''
                        if have_multi_op:
                            sql+=multi_operator_func_list[multi_op_index]
                        '''
                        if not have_multi_op:
                            #sql += func
                            column_sql += func
                        #sql += '(' + col_name
                        column_sql += '(' + col_name
                        op_list = ['-', '+', '*', '/']
                        if have_multi_op:  # and any(op2 in func[len(multi_operator_func_list[multi_op_index])-1:] for op2 in op_list):
                            #sql += ","
                            column_sql+=","
                            func_splited = re.split("([-|\+|\*|\/])", func)
                            # func_splited=func.split("\-|\+|\*|\/")
                            #sql += func_splited[0][len(multi_operator_func_list[multi_op_index]):]
                            column_sql += func_splited[0][len(multi_operator_func_list[multi_op_index]):]
                            # print("DEBUG:func_splited")
                            # print(func_splited)
                            if len(func_splited) > 1 and func_splited[1] in op_list:
                                #sql += ')'
                                #sql += func_splited[1]
                                column_sql += ')'
                                column_sql += func_splited[1]
                                if len(func_splited) > 2:
                                    #sql += multi_operator_func_list[multi_op_index]  # func_splited[0][len(multi_operator_func_list[multi_op_index]):]
                                    #sql += '(' + col_name + ','
                                    #sql += func_splited[2]
                                    column_sql += multi_operator_func_list[multi_op_index]  # func_splited[0][len(multi_operator_func_list[multi_op_index]):]
                                    column_sql += '(' + col_name + ','
                                    column_sql += func_splited[2]
                                else:
                                    #sql += '0'
                                    column_sql+='0'
                                pass
                        #sql += ') OVER '+window["name"]+" AS "
                        column_sql+= ') OVER '+window["name"]+" AS "
                        new_column_name=(self.info_['table_list'][0]+"__"+func_processed_name + "__" + col_name + "__" + window["name"])
                        #sql += new_column_name
                        column_sql+=new_column_name
                        sql += column_sql
                        self.column_name2sql_dict[new_column_name]=column_sql
                        self.processed_column_name_list.append(new_column_name)
                        sql += ","
                        sql += "\n "
                '''
                if i< len(col_name_dict['num'])-1:
                    sql+=","   
                '''

        sql += " FROM "
        for k, table_name in enumerate(self.info_['table_list']):
            sql += table_name
            if k < len(self.info_['table_list']) - 1:
                sql += ","
        sql += "\n "

        sql += " WINDOW "
        sql += self.print_windows()

        file_num = 2
        file_name = "feature_data_test_auto_sql_generator-22-10-11-demo" + str(file_num)

        save_sql="INTO OUTFILE '%s';" % (self.info_['output_file_path']+file_name)
        sql += save_sql

        print("*" * 50)
        print(sql)
        print("*" * 50)
        processed_feature_type = self.add_feature_column(self.processed_column_name_list, "output_" + file_name)
        return sql,save_sql, processed_feature_type, file_name

    def decode_time_series_feature_sql_column(self, topk_feature_list):
        sql = ""
        pd.read_csv(self.info_['path'] + self.info_['train_name']).columns.values.tolist()  # []
        #sql_selected_column_name_list = []
        sql += "SELECT "
        sql += self.info_['target']+","
        topk_features_in_original_columns = [x for idx,x in enumerate(topk_feature_list) if (x in self.processed_column_name_list)][:]
        print("DEBUG: topk_features_in_original_columns")
        print(len(topk_features_in_original_columns))
        print(topk_features_in_original_columns)
        for i, feature_column_name in enumerate(topk_features_in_original_columns):
            if feature_column_name in self.original_column_name_list:
                sql+=feature_column_name+",\n"
            else:
                sql+=self.column_name2sql_dict[feature_column_name]+",\n"

        sql += " FROM "
        for k, table_name in enumerate(self.info_['table_list']):
            sql += table_name
            if k < len(self.info_['table_list']) - 1:
                sql += ","
        sql += "\n "
        sql += " WINDOW "
        sql += self.print_windows()
        file_num = 2
        file_name ="final_"+ "feature_data_test_auto_sql_generator-22-8-2-demo" + str(file_num)

        #sql += "INTO OUTFILE '/tmp/%s';" % file_name #may be commented when deploy#2022-8-19
        sql += ";"
        return sql


# put all file's pathname into list "listcsv"
def list_dir(file_dir):
    list_csv = []
    dir_list = os.listdir(file_dir)
    for cur_file in dir_list:
        path = os.path.join(file_dir, cur_file)
        # 判断是文件夹还是文件
        if os.path.isfile(path):
            # print("{0} : is file!".format(cur_file))
            dir_files = os.path.join(file_dir, cur_file)
        # 判断是否存在.csv文件，如果存在则获取路径信息写入到list_csv列表中
        if os.path.splitext(path)[1] == '.csv':
            csv_file = os.path.join(file_dir, cur_file)
            # print(os.path.join(file_dir, cur_file))
            # print(csv_file)
            list_csv.append(csv_file)
        if os.path.isdir(path):
            # print("{0} : is dir".format(cur_file))
            # print(os.path.join(file_dir, cur_file))
            list_dir(path)
    return list_csv, dir_files


if __name__ == '__main__':
    # demo dataset can be downloaded in the following website:
    # https://www.kaggle.com/c/nyc-taxi-trip-duration/overview
    db_name="demo_db"
    path = './nyc-taxi-trip-duration/'  # '../../data/{data_name}'
    target_column_name = 'trip_duration'
    output_file_path='/tmp/'
    # id	vendor_id	pickup_datetime	dropoff_datetime	passenger_count	pickup_longitude	pickup_latitude	dropoff_longitude	dropoff_latitude	store_and_fwd_flag	trip_duration
    # id2875421	2	2016/3/14 17:24	2016/3/14 17:32	1	-73.98215485	40.76793671	-73.96463013	40.76560211	N	455
    # c1,c2,c3,c4,c5,c6,date
    # aaa,11,22,1.2,11.3,1.6361E+12,2021/7/20
    relation_dict=[    # 表关系(可以包含为1-1, 1-M, M-1, M-M四种)
        {
            "related_to_main_table": "true",
            "left_entity": "overdue",
            "left_on": ["new_user_id"],
            "left_time_col": "flag1",
            "right_entity": "bank",
            "right_on": ["new_user_id"],
            "right_time_col": "flag1",
            "type": "1-M"
        },
        {
            "related_to_main_table": "true",  # 是否为和主表的关系
            "left_entity": "overdue",  # 左表名字
            "left_on": ["new_user_id"],  # 左表拼表键
            "right_entity": "userinfo",  # 右表名字
            "right_on": ["new_user_id"],  # 右表拼表键
            "type": "1-1"  # 左表与右表的连接关系
        },
        {
            "related_to_main_table": "true",
            "left_entity": "overdue",
            "left_on": ["new_user_id"],
            "left_time_col": "flag1",
            "right_entity": "browse",
            "right_on": ["new_user_id"],
            "right_time_col": "flag1",
            "type": "1-M"
        },
        {
            "related_to_main_table": "true",
            "left_entity": "overdue",
            "left_on": ["new_user_id"],
            "left_time_col": "flag1",
            "right_entity": "bill",
            "right_on": ["new_user_id"],
            "right_time_col": "flag1",
            "type": "1-M"
        }
        
    ]

    #"(id string, vendor_id int, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count int, pickup_longitude double, pickup_latitude double, dropoff_longitude double, dropoff_latitude double, store_and_fwd_flag string, trip_duration int);"

    table_info_dict={
        't1':{'filename':r"/work/taxi-trip/data/taxi_tour_table_train_simple.snappy.parquet",
            'column_type':{'id':'string',
                             'vendor_id': 'int',
                             'pickup_datetime':'timestamp',
                             'dropoff_datetime':'timestamp',
                             'passenger_count': 'int',
                             'pickup_longitude': 'double',
                             'pickup_latitude':'double',
                             'dropoff_longitude': 'double',
                             'dropoff_latitude': 'double',
                             'store_and_fwd_flag': 'string',
                             'trip_duration': 'int'
                            }
        },
        't3':{'filename': r"/work/taxi-trip/data/new-taxi-trip-pay-data-generated-by-xpc.csv",
            'column_type': {'id':'string',
                             'vendor_id': 'int',
                             'pickup_datetime':'timestamp',
                             'dropoff_datetime':'timestamp',
                             'passenger_count': 'int',
                             'pickup_longitude': 'double',
                             'pickup_latitude':'double',
                             'dropoff_longitude': 'double',
                             'dropoff_latitude': 'double',
                             'store_and_fwd_flag': 'string',
                             'trip_duration': 'int',
                              #'pay': 'int'#added
                           }
       }}

    table_list = list(table_info_dict.keys())
    feature_type = {
        'test2.csv': {
            'id': 'cat',
            'vendor_id': 'cat',
            'pickup_datetime': 'datetime',
            'dropoff_datetime': 'datetime',
            'passenger_count': 'num',
            'pickup_longitude': 'num',
            'pickup_latitude': 'num',
            'dropoff_longitude': 'num',
            'dropoff_latitude': 'num',
            'store_and_fwd_flag': 'cat'  # ,
            # 'trip_duration':'num'
        },
        'train2.csv': {
            'id': 'cat',
            'vendor_id': 'cat',
            'pickup_datetime': 'datetime',
            'dropoff_datetime': 'datetime',
            'passenger_count': 'num',
            'pickup_longitude': 'num',
            'pickup_latitude': 'num',
            'dropoff_longitude': 'num',
            'dropoff_latitude': 'num',
            'store_and_fwd_flag': 'cat',
            'trip_duration': 'num'
        }
    }

    myOpenMLDB_sql_generator = openMLDB_sql_generator(target=target_column_name, train_name='train2.csv',
                                                      test_name='test2.csv',
                                                      id=['id', 'vendor_id'], path=path, time_series=True,
                                                      ts_unit='min', time_col=['pickup_datetime', 'dropoff_datetime'],
                                                      feature_type=feature_type,output_file_path=output_file_path,table_info_dict=table_info_dict,table_list=table_list)

    output_sql, output_save_sql,processsed_feature_type, file_name = myOpenMLDB_sql_generator.time_series_feature_sql()
    print("*" * 25 + "processed_feature_type" + "*" * 25)
    print(processsed_feature_type)
    if os.path.exists(output_file_path + file_name):
        print(output_file_path + file_name+" already exists, now delete this directory")
        shutil.rmtree(output_file_path + file_name)
        print(output_file_path + file_name + " deleted")
        #os.remove(output_file_path + file_name)
    ########################
    print(
        "*" * 40 + "following is the 1st generated sql to send query to OpenMLDB and get processed feature data csv file" + "*" * 40)
    #send query to OpenMLDB and get processed feature data csv file
    url ='http://127.0.0.1:9080/dbs/'+db_name #：9080#demo_db/'#'http://127.0.0.1:8080/dbs/{db} '
    init_sql =  []
    init_sql.append('CREATE DATABASE IF NOT EXISTS '+db_name+';')
    init_sql.append('USE '+db_name+';')
    init_sql.append('SET @@execute_mode="offline";')
    init_sql.append('SET @@sync_job = "true";')

    for table_name,table_info in table_info_dict.items():
        print(table_name)
        print(table_info['filename'])
        create_table_argument="("
        count=0
        for column_name, column_type in table_info_dict[table_name]['column_type'].items():
            count+=1
            create_table_argument+=(column_name+" "+column_type)
            if count==len(table_info_dict[table_name]['column_type']):
                create_table_argument += ");"
            else:
                create_table_argument += ", "
        sql_create_table="CREATE TABLE IF NOT EXISTS "+table_name+create_table_argument
        print(sql_create_table)
        init_sql.append(sql_create_table)
        #TODO: match the table name list here and in the class OpenMLDB_sql_generator()
        sql_load_data="LOAD DATA INFILE '"+table_info['filename']+"' INTO TABLE "+table_name+" options(format='"+table_info['filename'].split(".")[-1]+"', header=true, mode='append');"
        init_sql.append(sql_load_data)

    for init_sql_i in init_sql:
        print(init_sql_i)
        data = {"sql": init_sql_i, "mode": "offsync"}  # offsync #original "online"
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        r = requests.post(url, json=data, headers=headers)
        print("request text:")
        print(r.text)
        #print("request content")
        #print(r.content)

    data={"sql":(output_sql) , "mode":"offsync"}   #offsync #original "online"
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    r = requests.post(url, json=data, headers=headers)
    print("request text:")
    print(r.text)
    #print("request content")
    #print(r.content)
    ########################


    path_output = output_file_path + file_name
    csv_list, dir_files = list_dir(file_dir=path_output)
    print(csv_list)
    for csv in csv_list:
        single_data_frame = pd.read_csv(csv)
        #     print(single_data_frame.info())
        if csv == csv_list[0]:
            all_data_frame = single_data_frame
            print(all_data_frame)
        else:  # concatenate all csv to a single dataframe, ingore index
            all_data_frame = pd.concat([all_data_frame, single_data_frame], axis=0)
            print(all_data_frame)

    print(all_data_frame)

    train_set, test_set_with_y = train_test_split(all_data_frame, train_size=0.8)

    print(train_set)
    print(test_set_with_y)
    train_name = 'output_' + file_name + '_train.csv'
    train_set.to_csv(path + train_name, index=False, sep=',')
    test_set = test_set_with_y.drop(columns=target_column_name)  # pd.concat([x_train, y_train], axis=1)
    test_name = 'output_' + file_name + '_test.csv'
    test_set.to_csv(path + test_name, index=False, sep=',')
    ########feature selecting##############
    autox = AutoX(target=target_column_name, train_name=train_name, test_name=test_name,
                  id=['id', 'vendor_id'], path=path, time_series=True, ts_unit='min',
                  time_col=['pickup_datetime', 'dropoff_datetime'],
                  feature_type=processsed_feature_type)  # train_name = 'train2.csv'#feature_type=feature_type

    feature_importance,top_features, train_fe, test_fe = autox.get_top_features()
    print("feature_importance")
    print(feature_importance)
    feature_importance.to_csv(path + file_name + 'feature_importance.csv')
    print(top_features)

    topk = len(list(feature_importance['feature'])) #200  # the number of selected features
    topk_in_bound=min(topk,len(list(feature_importance['feature'])))

    topk_features = [x for x in list(feature_importance['feature']) if x in train_set.columns][:topk_in_bound]
    print("topk_features")
    print(topk_features)
    topk_features_df = pd.DataFrame(data=topk_features)
    topk_features_df.to_csv(path + file_name + 'topk_features.csv')
    train_fe.head()
    test_fe.head()

    top_features_df = pd.DataFrame(data=top_features)
    top_features_df.to_csv(path + file_name+'top_features.csv')
    train_fe.to_csv(path + file_name + 'train_fe.csv')
    test_fe.to_csv(path + file_name + 'test_fe.csv')
    ###########################################
    #######decode feature to final deploy sql###################
    # top_features=test_set.columns.values.tolist()#should be removed if AutoX is in the pipeline

    final_sql = myOpenMLDB_sql_generator.decode_time_series_feature_sql_column(list(feature_importance['feature'][:topk_in_bound]))
    print("*" * 25 + "final_sql" + "*" * 25)
    print(final_sql)

    ############################################################
    print(
        "*" * 40 + "following is the 2st final generated DEPLOY sql with SELECTED FEATURE to send query to OpenMLDB and get processed feature data csv file" + "*" * 40)


    ############deployment#####################################
    #url = 'http://127.0.0.1:9080/dbs/demo_db/'#deployments/demo_data_service'  # 'http://127.0.0.1:8080/dbs/{db} '
    file_num = 1
    time_list=[]
    time_list.append(time.strftime('%Y_%m_%d__%H_%M_%S', time.localtime()))
    deploy_service_name = "feature_data_test_auto_sql_generator" + str(file_num)+time_list[len(time_list)-1]
    #deploy_sql=[]#"DEPLOY " + deploy_service_name + " " +final_sql
    #deploy_sql.append('USE demo_db;')
    #deploy_sql.append('SET @@execute_mode="offline";')
    #deploy_sql.append('SET @@sync_job = "true";')
    deploy_sql="DEPLOY " + deploy_service_name + " " +final_sql
    data = {"sql":deploy_sql, "mode":"offsync"}
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    r = requests.post(url, json=data, headers=headers)
    print("deploy_sql")
    print(deploy_sql)
    print("request text")
    print(r.text)
    # TODO: To add request data for defined deployment
    ################train##########################################
    #feature_path,model path

    #train_py_path="/work/taxi-trip/"

    #sys.path.append(train_py_path)
    #import train
    print("Now you can run \npython3 /work/taxi-trip/train.py "+output_file_path+file_name+ " /tmp/model.txt")
    #train(output_file_path+file_name, "/tmp/model.txt")
    ##############################################################