# TalkingData 广告欺诈检测（OpenMLDB + XGboost）

本文将演示如何使用 OpenMLDB 与开源软件 XGboost 联合开发一个完整的机器学习应用，完成 [TalkingData 广告欺诈检测挑战](https://www.kaggle.com/c/talkingdata-adtracking-fraud-detection/overview)。

## 准备

本文基于 OpenMLDB CLI 进行开发和部署，首先需要下载样例数据并且启动 OpenMLDB CLI。推荐使用 Docker 镜像来快速体验。

- Docker 版本：>= 18.03

### 拉取镜像

在命令行执行以下命令拉取 OpenMLDB 镜像，并启动 Docker 容器，OpenMLDB 和依赖项都已安装：

```bash
docker run -it 4pdosc/openmldb:0.7.1 bash
```

### 准备数据

本例使用 `train.csv` 的前 10000 行作为示例数据，详情请参见代码 [train\_sample.csv](https://github.com/4paradigm/OpenMLDB/tree/main/demo/talkingdata-adtracking-fraud-detection)。

如果你想要测试完整数据，请通过以下方式下载：

```
kaggle competitions download -c talkingdata-adtracking-fraud-detection
```

并将数据解压缩到 `demo/talkingdata-adtracking-fraud-detection/data`。然后调用 [train\_and\_serve.py](https://github.com/4paradigm/OpenMLDB/blob/main/demo/talkingdata-adtracking-fraud-detection/train_and_serve.py) 中的 `cut_data()` 方法，制作新的 CSV 数据样本用于训练。

### 启动 OpenMLDB 集群

镜像内提供了 init.sh 脚本帮助用户快速启动集群。

```
/work/init.sh
```

### 启动预测服务器

即使还没有部署预测服务器，你也可以使用选项 `--no-init` 启动。

```
python3 /work/talkingdata/predict_server.py --no-init > predict.log 2>&1 &
```

```{tip}
- 训练完毕后，可以发送 post 请求至 `<ip>:<port>/update` 更新预测服务器。
- 可以运行 `pkill -9 python3` 命令，关闭后台预测服务器。
```

## 训练并应用

```
cd /work/talkingdata
python3 train_and_serve.py
```

使用 OpenMLDB 提取特征，并通过 XGboost 训练机器学习模型，请参见 [train\_and\_serve.py](https://github.com/4paradigm/OpenMLDB/blob/main/demo/talkingdata-adtracking-fraud-detection/train_and_serve.py)。程序具体内容有如下步骤：

1. 将数据加载到离线存储
2. 离线特征提取
   * ip-day-hour 组合的点击次数 -> 窗口期 1h
   * ip-app 组合的点击次数 -> 无限窗口期
   * ip-app-os 组合的点击次数 -> 无限窗口期 
3. 训练并保存模型
4. 部署sql
5. 加载数据到在线存储
6. 更新预测服务器上的模型

## 预测

向预测服务器发送 POST 请求 `<ip>:<port>/predict` 即可进行一次预测。或者也可以运行下面的 Python 脚本。

```
python3 predict.py
```

## 提示

预构建的 XGboost python wheel 可能与您计算机中的 openmldb python sdk 不兼容，可能会出现该报错：`train\_and\_serve.py core dump at SetGPUAttribute...`

通过源代码构建 XGboost 可解决该问题：进入 XGhboost 源代码所在的目录，并执行：

```bash
cd python-package && python setup.py install
```

或者构建 wheel：

```bash
python setup.py bdist_wheel
``` 
