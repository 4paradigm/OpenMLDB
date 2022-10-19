#!/bin/bash
DEVICE_NUM_PER_NODE=1
DEMODIR="$1"
DATA_DIR="$DEMODIR"/openmldb_process/out
PERSISTENT_PATH="$DEMODIR"/oneflow_process/persistent
MODEL_SAVE_DIR="$DEMODIR"/oneflow_process/model_out
MODEL_SERVING_PATH="$DEMODIR"/oneflow_process/model/embedding/1/model

python3 -m oneflow.distributed.launch \
    --nproc_per_node $DEVICE_NUM_PER_NODE \
    --nnodes 1 \
    --node_rank 0 \
    --master_addr 127.0.0.1 \
    deepfm_train_eval_JD.py \
      --disable_fusedmlp \
      --data_dir "$DATA_DIR" \
      --persistent_path "$PERSISTENT_PATH" \
      --table_size_array "11,42,1105,200,11,1295,1,1,5,3,23,23,7,5042381,3127923,5042381,3649642,28350,105180,7,2,5042381,5,4,4,41,2,2,8,3456,4,5,5042381,10,60,5042381,843,17,1276,101,100" \
      --store_type 'cached_host_mem' \
      --cache_memory_budget_mb 1024 \
      --batch_size 10000 \
      --train_batches 75000 \
      --loss_print_interval 100 \
      --dnn "1000,1000,1000,1000,1000" \
      --net_dropout 0.2 \
      --learning_rate 0.001 \
      --embedding_vec_size 16 \
      --num_train_samples 4007924 \
      --num_val_samples 504398 \
      --num_test_samples 530059 \
      --model_save_dir "$MODEL_SAVE_DIR" \
      --save_best_model \
      --save_graph_for_serving \
      --model_serving_path "$MODEL_SERVING_PATH" \
      --save_model_after_each_eval 
