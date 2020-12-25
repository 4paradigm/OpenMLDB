import os
import sys

from auto_cases import gen_case_yaml

if __name__ == "__main__":
    currentPath = os.getcwd()
    index = currentPath.rfind('fesql')
    prePath = currentPath[0:index]
    casePath = prePath+"fesql/cases/auto_gen_cases"
    gen_case_yaml(casePath)