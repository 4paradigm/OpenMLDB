# OpenMLDB + XGBoost: TalkingData Ad Tracking Fraud Detection

We will demonstrate how to use [OpenMLDB](https://github.com/4paradigm/OpenMLDB) together with other open source software to develop a machine learning application in order to complete the TalkingData AD Fraud Detection challenge (see more information on this challenge in [Kaggle](https://www.kaggle.com/c/talkingdata-adtracking-fraud-detection/overview)).

## 1 Preparations

### 1.1 Download OpenMLDB

#### 1.1.1 Run in Docker

It is recommended to run this demo in Docker. Please make sure that OpenMLDB and its dependencies are installed.

**Start the OpenMLDB Docker Image**

```
docker run -it 4pdosc/openmldb:0.8.3 bash
```

#### 1.1.2 Run Locally

If you want to run this demo locally, please run following command to install all dependencies after downloading OpenMLDB Server pkg and make sure your version is higher than 0.5.0:
```
pip install pandas xgboost==1.4.2 sklearn tornado "openmldb>=0.5.0" requests
```


### 1.2 Data Preparation

In this example, only the first 10,000 rows of the training data set provided by Kaggle will be used, see the data in [train\_sample.csv](https://github.com/4paradigm/OpenMLDB/tree/main/demo/talkingdata-adtracking-fraud-detection).
To download the complete dataset, please run the command:
```
kaggle competitions download -c talkingdata-adtracking-fraud-detection
```
After download, please extract the data to `demo/talkingdata-adtracking-fraud-detection/data` and run the `cut_data()` function in the [train\_and\_serve.py](https://github.com/4paradigm/OpenMLDB/blob/main/demo/talkingdata-adtracking-fraud-detection/train_and_serve.py) to make new dataset for training.


### 1.3 Start the OpenMLDB Cluster

```
/work/init.sh
```

### 1.4 Start the Prediction Server

If you have not deployed the prediction server, you can start it with the option `--no-init`.

```
python3 /work/talkingdata/predict_server.py --no-init > predict.log 2>&1 &
```

```{tip}
- To stop the prediction server, please use `pkill -9 python3` command.
- You can send the post request `<ip>:<port>/update` to update the prediction server after training.
```


## 2 Training and Serving

```
cd /work/talkingdata
python3 train_and_serve.py
```
The python script [train\_and\_serve.py](https://github.com/4paradigm/OpenMLDB/blob/main/demo/talkingdata-adtracking-fraud-detection/train_and_serve.py) uses the OpenMLDB for feature extraction and the XGBoost model for training. 
The script completes the following tasks:

1. Loading the data into the offline database.
2. Extracting the features offline:
   * Number of clicks of the 'ip-day-hour' in 1h window.
   * Number of clicks of the 'ip-app' in unlimited window.
   * Number of clicks of the 'ip-app-os' in unlimited window.
3. Training and saving the model.
4. Deploying the SQL.
5. Loading the data into the online storage.
6. Updating the model on the prediction server.

## 3 Prediction

For prediction, send the post request  `<ip>:<port>/predict` to the prediction server, or you can run the Python script through the command below:
```
python3 predict.py
```

## 4 Note

The pre-installed XGBoost Python wheel may be incompatible with the OpenMLDB Python SDK on your computer, which may lead to the following error:
`train\_and\_serve.py core dump at SetGPUAttribute...`

Installing the XGBoost by the source code may resolve the problem. Please switch to the path of the XGBoost source code and execute:
`cd python-package && python setup.py install`

There is another solution to construct the wheel by executing:
`python setup.py bdist_wheel`
