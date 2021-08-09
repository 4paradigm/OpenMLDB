# Benchmark Report Generator

## 1. 准备Benchmark数据

mkdir data
cd data
vi branch_name_timestamp.txt
拷贝性能测试结果到文本中

cd ..

mkdir render
python gen_report.py

