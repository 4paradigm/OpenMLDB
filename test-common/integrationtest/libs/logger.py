# coding=utf-8
import logging
import datetime
import os
import libs.conf as conf

log_level = conf.log_level
log_path = os.getenv("testlogpath")

log_format = '%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s'
curDate = datetime.date.today() - datetime.timedelta(days=0)
infoLogName = r'%s/info_%s.log' %(log_path, curDate)
errorLogName = r'%s/error_%s.log' %(log_path, curDate)

formatter = logging.Formatter(log_format)

infoLogger = logging.getLogger("infoLog")
errorLogger = logging.getLogger("errorLog")

infoLogger.setLevel(eval('logging.' + log_level.upper()))
errorLogger.setLevel(logging.ERROR)

infoHandler = logging.FileHandler(infoLogName, 'a')
infoHandler.setFormatter(formatter)

errorHandler = logging.FileHandler(errorLogName, 'a')
errorHandler.setFormatter(formatter)

testHandler = logging.StreamHandler()
testHandler.setLevel(eval('logging.' + log_level.upper()))
testHandler.setFormatter(formatter)

infoLogger.addHandler(infoHandler)
infoLogger.addHandler(testHandler)
errorLogger.addHandler(errorHandler)
