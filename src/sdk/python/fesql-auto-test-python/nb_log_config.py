# -*- coding: utf-8 -*-
"""
此文件nb_log_config.py是自动生成到python项目的根目录的。
在这里面写的变量会覆盖此文件nb_log_config_default中的值。对nb_log包进行默认的配置。

由于不同的logger天然就是多个实例，所以可以通过get_logger_and_handlers传参针对每个logger精确的做不同的配置。
最终配置方式是由get_logger_and_add_handlers方法的各种传参决定，如果方法相应的传参为None则使用这里面的配置。
最终配置方式是由get_logger_and_add_handlers方法的各种传参决定，如果方法相应的传参为None则使用这里面的配置。
最终配置方式是由get_logger_and_add_handlers方法的各种传参决定，如果方法相应的传参为None则使用这里面的配置。
重要的重复三遍。

"""
import logging
# 
# DING_TALK_TOKEN = '3dd0eexxxxxadab014bd604XXXXXXXXXXXX'  # 数据组报警机器人
# 
# EMAIL_HOST = ('smtp.sohu.com', 465)
# EMAIL_FROMADDR = 'aaa0509@sohu.com'  # 'matafyhotel-techl@matafy.com',
# EMAIL_TOADDRS = ('cccc.cheng@silknets.com', 'yan@dingtalk.com',)
# EMAIL_CREDENTIALS = ('aaa0509@sohu.com', 'abcdefg')
# 
# ELASTIC_HOST = '127.0.0.1'
# ELASTIC_PORT = 9200
# 
# KAFKA_BOOTSTRAP_SERVERS = ['192.168.199.202:9092']
# ALWAYS_ADD_KAFKA_HANDLER_IN_TEST_ENVIRONENT = False
# 
# MONGO_URL = 'mongodb://myUserAdmin:mimamiama@127.0.0.1:27016/admin'
# 
# DEFAULUT_USE_COLOR_HANDLER = True  # 是否默认使用有彩的日志。有的人讨厌彩色可以关掉（主要是不按提示的说明配置pycahrm的conose）。
# DISPLAY_BACKGROUD_COLOR_IN_CONSOLE = True     # 在控制台是否显示彩色块状的日志。为False则不使用大块的背景颜色。
# AUTO_PATCH_PRINT = True  # 是否自动打print的猴子补丁，如果打了后指不定，print自动变色和可点击跳转。
from util import tools

WARNING_PYCHARM_COLOR_SETINGS = False
# 
DEFAULT_ADD_MULTIPROCESSING_SAFE_ROATING_FILE_HANDLER = True  # 是否默认同时将日志记录到记log文件记事本中。
# LOG_FILE_SIZE = 100  # 单位是M,每个文件的切片大小，超过多少后就自动切割
# LOG_FILE_BACKUP_COUNT = 3
# 
# LOG_LEVEL_FILTER = logging.DEBUG  # 默认日志级别，低于此级别的日志不记录了。例如设置为INFO，那么logger.debug的不会记录，只会记录logger.info以上级别的。
# RUN_ENV = 'test'
# 
# FORMATTER_DICT = {
#     1: logging.Formatter(
#         '日志时间【%(asctime)s】 - 日志名称【%(name)s】 - 文件【%(filename)s】 - 第【%(lineno)d】行 - 日志等级【%(levelname)s】 - 日志信息【%(message)s】',
#         "%Y-%m-%d %H:%M:%S"),
#     2: logging.Formatter(
#         '%(asctime)s - %(name)s - %(filename)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s',
#         "%Y-%m-%d %H:%M:%S"),
#     3: logging.Formatter(
#         '%(asctime)s - %(name)s - 【 File "%(pathname)s", line %(lineno)d, in %(funcName)s 】 - %(levelname)s - %(message)s',
#         "%Y-%m-%d %H:%M:%S"),  # 一个模仿traceback异常的可跳转到打印日志地方的模板
#     4: logging.Formatter(
#         '%(asctime)s - %(name)s - "%(filename)s" - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s -               File "%(pathname)s", line %(lineno)d ',
#         "%Y-%m-%d %H:%M:%S"),  # 这个也支持日志跳转
#     5: logging.Formatter(
#         '%(asctime)s - %(name)s - "%(pathname)s:%(lineno)d" - %(funcName)s - %(levelname)s - %(message)s',
#         "%Y-%m-%d %H:%M:%S"),  # 我认为的最好的模板,推荐
#     6: logging.Formatter('%(name)s - %(asctime)-15s - %(filename)s - %(lineno)d - %(levelname)s: %(message)s',
#                          "%Y-%m-%d %H:%M:%S"),
#     7: logging.Formatter('%(asctime)s - %(name)s - "%(filename)s:%(lineno)d" - %(levelname)s - %(message)s',"%Y-%m-%d %H:%M:%S"),  # 一个只显示简短文件名和所处行数的日志模板
# }
# 
# FORMATTER_KIND = 5  # 默认选择第几个模板

LOG_PATH=tools.getRootPath()+"/logs"