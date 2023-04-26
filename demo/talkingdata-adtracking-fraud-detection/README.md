# TalkingData AdTracking Fraud Detection

We demonstrate how to use [OpenMLDB](https://github.com/4paradigm/OpenMLDB) together with other opensource software to
develop a complete machine learning application for TalkingData AdTracking Fraud Detection (read more about this
application on [Kaggle](https://www.kaggle.com/c/talkingdata-adtracking-fraud-detection/overview)).

## Prepare

### OpenMLDB

#### Run in Docker

We recommend you to use docker to run the demo. OpenMLDB and dependencies have been installed.

**Start docker**

```
docker run -it 4pdosc/openmldb:0.7.3 bash
```

#### Run locally

Download OpenMLDB server pkg, version >= 0.5.0 .

Install all dependencies:

```
pip install pandas xgboost==1.4.2 sklearn tornado "openmldb>=0.5.0" requests
```

### Data Prepare

We just use the head 10000 rows of `train.csv` to be the sample data, see
in `demo/talkingdata-adtracking-fraud-detection/train_sample.csv`.

If you want to test full data, download it by

```
kaggle competitions download -c talkingdata-adtracking-fraud-detection
```

And unzip the data to `demo/talkingdata-adtracking-fraud-detection/data`. Then, call `cut_data()`
in `train_and_serve.py` to produce new sample csv for training.

## Process

### Start OpenMLDB cluster

```
/work/init.sh
```

### Start predict server

You can start predict server even if you haven't deployed, with option `--no-init`.

```
python3 /work/talkingdata/predict_server.py --no-init > predict.log 2>&1 &
```

After trained, you can make predict server to update, by sending a post request to `<ip>:<port>/update`.

You can run `pkill -9 python3` to kill predict server in the background.

### Train and Serve

```
cd /work/talkingdata
python3 train_and_serve.py
```

We use OpenMLDB to do feature extraction, and train by xgboost,
see [train_and_serve.py](https://github.com/4paradigm/OpenMLDB/blob/main/demo/talkingdata-adtracking-fraud-detection/train_and_serve.py)
.

1. load data to offline storage
2. offline feature extraction:
    * clicks for each ip-day-hour combination -> window 1h
    * clicks for each ip-app combination -> unbounded window
    * clicks for each ip-app-os combination -> unbounded window
3. train and save model
4. deploy sql online
5. load data to online storage
6. update model to predict server

#### The Jupyter Way

You can use the jupyter nodebook `train_and_serve.ipynb`, the same with `train_and_serve.py`

Steps:
1. `docker ... -p 8888:8888 ...`, 8888 is the jupyter server default port.
1. start openmldb and predict server
1. `pip3 install notebook`
1. run jupyter, `jupyter --ip 0.0.0.0 --allow-root`. You can set the password before running, `jupyter notebook password`.
1. run `train_and_serve.ipynb` in jupyter notebook web.


```{tip}
Use `jupyter nbconvert --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --ClearMetadataPreprocessor.preserve_cell_metadata_mask tags --to=notebook --log-level=ERROR --inplace train_and_serve.ipynb` to make notebook clean.
```

### Predict

Predict once, send a post request to predict server `<ip>:<port>/predict`. Or you can run the python script below.

```
python3 predict.py
```

## Q&A

Q: train_and_serve.py core dump at SetGPUAttribute... A: The pre-built xgboost python wheel may be imcompatible with
openmldb python sdk in your machine. You should xgboost from source.

Checkout the xgboost source code, and `cd python-package && python setup.py install`, or build the
wheel `python setup.py bdist_wheel`.
