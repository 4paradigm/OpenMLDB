# TalkingData 广告欺诈检测（OpenMLDB + XGboost）

我们将演示如何使用 [OpenMLDB](https://github.com/4paradigm/OpenMLDB) 与其他开源软件一起开发一个完整的机器学习应用程序，完成 TalkingData 广告欺诈检测挑战（有关此挑战的更多信息请参阅 [Kaggle](https://www.kaggle.com/c/talkingdata-adtracking-fraud-detection/overview)）。




## 1 准备工作

### 1.1 下载并安装OpenMLDB

#### 1.1.1 在 Docker 中运行

我们建议您使用docker来运行此 Demo。OpenMLDB 和依赖项都已安装完毕。

**启动 Docker**

```
docker run -it 4pdosc/openmldb:0.8.4 bash
```

#### 1.1.2 在本地运行

下载 OpenMLDB 服务器 pkg，版本>=0.5.0。

安装所有依赖项：

```
pip install pandas xgboost==1.4.2 sklearn tornado "openmldb>=0.5.0" requests
```

### 1.2 准备数据

我们只使用 `train.csv` 的前10000行作为示例数据，请参见[train\_sample.csv](https://github.com/4paradigm/OpenMLDB/tree/main/demo/talkingdata-adtracking-fraud-detection)。

如果你想要测试完整数据，请通过以下方式下载

```
kaggle competitions download -c talkingdata-adtracking-fraud-detection
```

并将数据解压缩到 `demo/talkingdata-adtracking-fraud-detection/data` 。然后调用 [train\_and\_serve.py](https://github.com/4paradigm/OpenMLDB/blob/main/demo/talkingdata-adtracking-fraud-detection/train_and_serve.py)中的 `cut_data()` 方法，制作新的csv样本用于训练。


### 1.3 启动 OpenMLDB 集群

```
/work/init.sh
```

### 1.4 启动预测服务器

即使您还没有部署预测服务器，您也可以启动它，使用选项 `--no-init`。

```
python3 /work/talkingdata/predict_server.py --no-init > predict.log 2>&1 &
```


```{tip}
- 训练完毕后，您可以发送 post 请求至 `<ip>:<port>/update` 更新预测服务器。
- 您可以运行 `pkill -9 python3` 命令，关闭后台预测服务器。
```


## 2 训练并应用

```
cd /work/talkingdata
python3 train_and_serve.py
```

我们使用 OpenMLDB 提取特征，并通过 xgboost 进行训练，请参见[train\_and\_serve.py](https://github.com/4paradigm/OpenMLDB/blob/main/demo/talkingdata-adtracking-fraud-detection/train_and_serve.py)。

1. 将数据加载到离线存储
2. 离线特征提取；
   * ip-day-hour 组合的点击次数 -> 窗口期 1h
   * ip-app 组合的点击次数 -> 无限窗口期
   * ip-app-os 组合的点击次数 -> 无限窗口期 
3. 训练并保存模型
4. 部署sql
5. 加载数据到在线存储
6. 更新预测服务器上的模型

## 3 预测

向预测服务器发送post请求 `<ip>:<port>/predict` 即可进行一次预测。或者您也可以运行下面的python脚本。

```
python3 predict.py
```

## 4 提示

预构建的 xgboost python wheel 可能与您计算机中的 openmldb python sdk 不兼容，可能会出现该报错：
`train\_and\_serve.py core dump at SetGPUAttribute...`

通过源代码构建xgboost可解决该问题：进入 xgboost 源代码所在的目录，并执行
`cd python-package && python setup.py install`

或者构建 wheel ：
`python setup.py bdist_wheel`
