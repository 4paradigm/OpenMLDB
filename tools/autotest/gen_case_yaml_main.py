import os
import sys

from auto_cases import gen_case_yaml

if __name__ == "__main__":
    '''
    生成yaml的入口
    '''
    currentPath = os.getcwd()
    index = currentPath.rfind('fesql')
    if index == -1:
        prePath = currentPath+"/"
    else:
        prePath = currentPath[0:index]
    print("prePath:"+prePath)
    casePath = prePath+"fesql/cases/auto_gen_cases"
    print("casePath:"+casePath)
    gen_case_yaml(casePath)