# Prepare
## Run in Docker

We recommend you to use docker to run the demo. OpenMLDB and dependencies have been installed.

**Start docker**
```
docker run -it 4pdosc/openmldb:0.5.0 bash
```
In the container, 
```
cd /work/talkingdata
```

## Run locally

Download OpenMLDB server pkg, version >= 0.5.0 .

Install all dependencies:
```
pip install pandas xgboost tornado openmldb
```

## Data Prepare
We use [kaggle talkingdata](https://www.kaggle.com/c/talkingdata-adtracking-fraud-detection/overview).

We just use the head 10000 rows of train data to be the sample data, see in `demo/talkingdata-adtracking-fraud-detection/train_sample.csv`.

If you want to test full data, download it by
```
kaggle competitions download -c talkingdata-adtracking-fraud-detection
```
And unzip the data to `demo/talkingdata-adtracking-fraud-detection/data`. Then, call `cut_data()` in `train_and_serve.py` to produce new sample csv for training.

# Process

## Start OpenMLDB cluster
```
./init.sh
```

## Feature Extraction

We use OpenMLDB to do feature extraction, see `train_and_serve.py`.
1. load datai(4,000,000 rows) to offline(about 2min)
2. offline feature extraction(about 7min): 
    * clicks for each ip-day-hour combination -> window 1h
    * clicks for each ip-app combination -> unbounded window
    * clicks for each ip-app-os combination -> unbounded window
3. train and save model
4. deploy online
5. load data to online(about 13min)

And then, we use predict server to do online request.
1. start preidct server, using model
2. run predict

# Q&A
Q: train_and_serve.py core dump at SetGPUAttribute...
A: You may need install the xgboost without use-cuda. Build it from source.
