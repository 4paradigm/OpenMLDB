# Prepare
```
pip install matplotlib pandas sklearn lightgbm
```

## OpenMLDB
use the main branch version(must have commit 90147ca4)
build python sdk from source, then install it.
```
pip install openmldb
```

build server from source, or download OpenMLDB main version release package, to start OpenMLDB server
e.g. https://github.com/4paradigm/OpenMLDB/suites/5749300609/artifacts/190974184

start OpenMLDB server

## Data Prepare
https://www.kaggle.com/c/talkingdata-adtracking-fraud-detection/overview
```
kaggle competitions download -c talkingdata-adtracking-fraud-detection
unzip to ./demo/talkingdata-adtracking-fraud-detection/data 
```

# Process
We use OpenMLDB to do feature extraction, see `train_and_serve.py`.

1. data prepare: 
    * clicks for each ip-day-hour combination -> window 1h
    * clicks for each ip-app combination -> unbounded window
    * clicks for each ip-app-os combination -> unbounded window
2. 

