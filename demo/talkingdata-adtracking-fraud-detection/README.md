# TalkingData AdTracking Fraud Detection

We demonstrate how to use [OpenMLDB](https://github.com/4paradigm/OpenMLDB) together with other opensource software to develop a complete machine learning application for TalkingData AdTracking Fraud Detection (read more about this application on [Kaggle](https://www.kaggle.com/c/talkingdata-adtracking-fraud-detection/overview)).

## Prepare

### OpenMLDB
#### Run in Docker

We recommend you to use docker to run the demo. OpenMLDB and dependencies have been installed.

**Start docker**
```
docker run -it 4pdosc/openmldb:0.5.0 bash
```
In the container, 
```
cd /work/talkingdata
```

#### Run locally

Download OpenMLDB server pkg, version >= 0.5.0 .

Install all dependencies:
```
pip install pandas xgboost==1.4.2 tornado "openmldb>=0.5.0"
```

### Data Prepare

We just use the head 10000 rows of `train.csv` to be the sample data, see in `demo/talkingdata-adtracking-fraud-detection/train_sample.csv`.

If you want to test full data, download it by
```
kaggle competitions download -c talkingdata-adtracking-fraud-detection
```
And unzip the data to `demo/talkingdata-adtracking-fraud-detection/data`. Then, call `cut_data()` in `train_and_serve.py` to produce new sample csv for training.

## Process

### Start OpenMLDB cluster
```
./init.sh
```

### Train and Serve
```
python3 train_and_serve.py
```
We use OpenMLDB to do feature extraction, and train by xgboost, see `train_and_serve.py`.
1. load data to offline storage
2. offline feature extraction: 
    * clicks for each ip-day-hour combination -> window 1h
    * clicks for each ip-app combination -> unbounded window
    * clicks for each ip-app-os combination -> unbounded window
3. train and save model
4. deploy sql online
5. load data to online storage

### Predict
And then, we start a predict server to do online request.
1. start the preidct server, using the model we trained previously
```
python3 predict_server.py 127.0.0.1:9080 model.json > predict.log 2>&1 &
```
2. predict once
```
python3 predict.py
```

You can run `pkill python3` to kill the predict server in the background.

## Q&A
Q: train_and_serve.py core dump at SetGPUAttribute...
A: The pre-built xgboost python wheel may be imcompatible with openmldb python sdk in your machine. You should xgboost from source.

Checkout the xgboost source code, and `cd python-package && python setup.py install`, or build the wheel `python setup.py bdist_wheel`.
