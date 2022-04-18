# Prepare
```
pip install pandas xgboost tornado
```

## OpenMLDB
use the main branch version(server must have commit a4f4617)

1. build python sdk from source, then install it.
```
pip install openmldb
```

2. download OpenMLDB main version(>= a4f4617) release package, to start OpenMLDB server
e.g. https://github.com/4paradigm/OpenMLDB/suites/5904829335/artifacts/201250777
or build it from branch main.

3. start OpenMLDB cluster(cluster in local)

## Data Prepare
https://www.kaggle.com/c/talkingdata-adtracking-fraud-detection/overview
```
kaggle competitions download -c talkingdata-adtracking-fraud-detection
unzip to ./demo/talkingdata-adtracking-fraud-detection/data 
```

# Process
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
2. 
