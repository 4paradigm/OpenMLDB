import ConfigParser
import string
import os
import sys


cf = ConfigParser.ConfigParser()
cf.read(os.getenv("testconfpath"))

failfast = cf.getboolean("test_opt", "failfast")

multidimension = cf.getboolean("dimension", "multidimension")
multidimension_vk = eval(cf.get("dimension", "multidimension_vk"))
multidimension_scan_vk = eval(cf.get("dimension", "multidimension_scan_vk"))

log_level = cf.get("log", "log_level")
