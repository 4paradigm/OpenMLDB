#!/bin/bash

if [ "$1" == "-h" ]; then
  echo "Usage: `basename $0` DATA_DIR(abs)
        We'll read required args in \$DATA_DIR/data_info.txt, and save results in path $(dirname $0)/"
  exit 0
fi

DATA_DIR=$1
SAVE_DIR=$(cd $(dirname $0);pwd)
echo "use data dir ${DATA_DIR}, data info:"
#cat  ${DATA_DIR}/data_info.txt
ls $DATA_DIR/data_info.txt || exit -1
IFS=$'\n' read -ra arr -d '' < $DATA_DIR/data_info.txt

NUM_TRAIN=${arr[0]}
NUM_TEST=${arr[1]}
NUM_VAL=${arr[2]}
TABLE_SIZE_ARRAY=${arr[3]}

DEVICE_NUM_PER_NODE=1

PERSISTENT_PATH=$SAVE_DIR/persistent
MODEL_SAVE_DIR=$SAVE_DIR/model_out
MODEL_SERVING_PATH=$SAVE_DIR/model/embedding/1/model

mkdir -p $PERSISTENT_PATH
mkdir -p $MODEL_SAVE_DIR
mkdir -p $MODEL_SERVING_PATH 

echo "num_train $NUM_TRAIN, num_test $NUM_TEST, num_val $NUM_VAL, table_size_array $TABLE_SIZE_ARRAY"
echo "result save path: persistent $PERSISTENT_PATH, model $MODEL_SAVE_DIR, model serving $MODEL_SERVING_PATH"

python3 -m oneflow.distributed.launch \
    --nproc_per_node $DEVICE_NUM_PER_NODE \
    --nnodes 1 \
    --node_rank 0 \
    --master_addr 127.0.0.1 \
    deepfm_train_eval_JD.py \
      --disable_fusedmlp \
      --data_dir $DATA_DIR \
      --persistent_path $PERSISTENT_PATH \
      --table_size_array "$TABLE_SIZE_ARRAY" \
      --store_type 'cached_host_mem' \
      --cache_memory_budget_mb 1024 \
      --batch_size 1000 \
      --train_batches 75000 \
      --loss_print_interval 100 \
      --dnn "1000,1000,1000,1000,1000" \
      --net_dropout 0.2 \
      --learning_rate 0.001 \
      --embedding_vec_size 16 \
      --num_train_samples $NUM_TRAIN \
      --num_val_samples $NUM_VAL \
      --num_test_samples $NUM_TEST \
      --model_save_dir $MODEL_SAVE_DIR \
      --save_best_model \
      --save_graph_for_serving \
      --model_serving_path $MODEL_SERVING_PATH \
      --save_model_after_each_eval
