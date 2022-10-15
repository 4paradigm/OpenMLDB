import argparse
import os
import sys
import glob
import time
import math
import numpy as np
import psutil
import oneflow as flow
import oneflow.nn as nn
from petastorm.reader import make_batch_reader

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))


def get_args(print_args=True):
    def int_list(x):
        return list(map(int, x.split(",")))

    def str_list(x):
        return list(map(str, x.split(",")))

    parser = argparse.ArgumentParser()

    parser.add_argument("--data_dir", type=str, required=True)
    parser.add_argument(
        "--num_train_samples", type=int, required=True, help="the number of train samples",
    )
    parser.add_argument(
        "--num_val_samples", type=int, required=True, help="the number of validation samples",
    )
    parser.add_argument(
        "--num_test_samples", type=int, required=True, help="the number of test samples"
    )

    parser.add_argument("--model_load_dir", type=str, default=None, help="model loading directory")
    parser.add_argument("--model_save_dir", type=str, default=None, help="model saving directory")
    parser.add_argument(
        "--save_initial_model", action="store_true", help="save initial model parameters or not",
    )
    parser.add_argument(
        "--save_model_after_each_eval",
        action="store_true",
        help="save model after each eval or not",
    )

    parser.add_argument("--embedding_vec_size", type=int, default=16, help="embedding vector size")
    parser.add_argument(
        "--dnn", type=int_list, default="1000,1000,1000,1000,1000", help="dnn hidden units number",
    )
    parser.add_argument("--net_dropout", type=float, default=0.2, help="net dropout rate")
    parser.add_argument("--disable_fusedmlp", action="store_true", help="disable fused MLP or not")

    parser.add_argument("--lr_factor", type=float, default=0.1)
    parser.add_argument("--min_lr", type=float, default=1.0e-6)
    parser.add_argument("--learning_rate", type=float, default=0.001, help="learning rate")

    parser.add_argument(
        "--batch_size", type=int, default=10000, help="training/evaluation batch size"
    )
    parser.add_argument(
        "--train_batches", type=int, default=75000, help="the maximum number of training batches",
    )
    parser.add_argument("--loss_print_interval", type=int, default=100, help="")

    parser.add_argument(
        "--patience",
        type=int,
        default=2,
        help="number of epochs with no improvement after which learning rate will be reduced",
    )
    parser.add_argument(
        "--min_delta",
        type=float,
        default=1.0e-6,
        help="threshold for measuring the new optimum, to only focus on significant changes",
    )

    parser.add_argument(
        "--table_size_array",
        type=int_list,
        help="embedding table size array for sparse fields",
        required=True,
    )
    parser.add_argument(
        "--persistent_path", type=str, required=True, help="path for persistent kv store",
    )
    parser.add_argument(
        "--store_type",
        type=str,
        default="cached_host_mem",
        help="OneEmbeddig persistent kv store type: device_mem, cached_host_mem, cached_ssd",
    )
    parser.add_argument(
        "--cache_memory_budget_mb",
        type=int,
        default=1024,
        help="size of cache memory budget on each device in megabytes when store_type is cached_host_mem or cached_ssd",
    )

    parser.add_argument(
        "--amp", action="store_true", help="enable Automatic Mixed Precision(AMP) training or not",
    )
    parser.add_argument("--loss_scale_policy", type=str, default="static", help="static or dynamic")

    parser.add_argument(
        "--disable_early_stop", action="store_true", help="enable early stop or not"
    )
    parser.add_argument("--save_best_model", action="store_true", help="save best model or not")
    parser.add_argument(
        "--save_graph_for_serving",
        action="store_true",
        help="Save Graph and OneEmbedding for serving. ",
    )
    parser.add_argument(
        "--model_serving_path", type=str, required=True, help="Graph object path for model serving",
    )
    args = parser.parse_args()

    if print_args and flow.env.get_rank() == 0:
        _print_args(args)
    return args


def _print_args(args):
    """Print arguments."""
    print("------------------------ arguments ------------------------", flush=True)
    str_list = []
    for arg in vars(args):
        dots = "." * (48 - len(arg))
        str_list.append("  {} {} {}".format(arg, dots, getattr(args, arg)))
    for arg in sorted(str_list, key=lambda x: x.lower()):
        print(arg, flush=True)
    print("-------------------- end of arguments ---------------------", flush=True)


num_dense_fields = 13
num_sparse_fields = 28


class DeepFMDataReader(object):
    """A context manager that manages the creation and termination of a
    :class:`petastorm.Reader`.
    """

    def __init__(
        self,
        parquet_file_url_list,
        batch_size,
        num_epochs=1,
        shuffle_row_groups=True,
        shard_seed=2019,
        shard_count=1,
        cur_shard=0,
    ):
        self.parquet_file_url_list = parquet_file_url_list
        self.batch_size = batch_size
        self.num_epochs = num_epochs
        self.shuffle_row_groups = shuffle_row_groups
        self.shard_seed = shard_seed
        self.shard_count = shard_count
        self.cur_shard = cur_shard

        fields = ["Label"]
        fields += [f"I{i+1}" for i in range(num_dense_fields)]
        fields += [f"C{i+1}" for i in range(num_sparse_fields)]
        self.fields = fields
        self.num_fields = len(fields)

    def __enter__(self):
        self.reader = make_batch_reader(
            self.parquet_file_url_list,
            workers_count=2,
            shuffle_row_groups=self.shuffle_row_groups,
            num_epochs=self.num_epochs,
            shard_seed=self.shard_seed,
            shard_count=self.shard_count,
            cur_shard=self.cur_shard,
        )
        self.loader = self.get_batches(self.reader)
        return self.loader

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.reader.stop()
        self.reader.join()

    def get_batches(self, reader, batch_size=None):
        if batch_size is None:
            batch_size = self.batch_size

        tail = None

        for rg in reader:
            rgdict = rg._asdict()
            rglist = [rgdict[field] for field in self.fields]
            pos = 0
            if tail is not None:
                pos = batch_size - len(tail[0])
                tail = list(
                    [
                        np.concatenate((tail[i], rglist[i][0 : (batch_size - len(tail[i]))]))
                        for i in range(self.num_fields)
                    ]
                )
                if len(tail[0]) == batch_size:
                    label = tail[0]
                    features = tail[1 : self.num_fields]
                    tail = None
                    yield label, np.stack(features, axis=-1)
                else:
                    pos = 0
                    continue
            while (pos + batch_size) <= len(rglist[0]):
                label = rglist[0][pos : pos + batch_size]
                features = [rglist[j][pos : pos + batch_size] for j in range(1, self.num_fields)]
                pos += batch_size
                yield label, np.stack(features, axis=-1)
            if pos != len(rglist[0]):
                tail = [rglist[i][pos:] for i in range(self.num_fields)]


def make_criteo_dataloader(data_path, batch_size, shuffle=True):
    """Make a Criteo Parquet DataLoader.
    :return: a context manager when exit the returned context manager, the reader will be closed.
    """
    files = ["file://" + name for name in glob.glob(f"{data_path}/*.parquet")]
    files.sort()

    world_size = flow.env.get_world_size()
    batch_size_per_proc = batch_size // world_size

    return DeepFMDataReader(
        files,
        batch_size_per_proc,
        None,  # TODO: iterate over all eval dataset
        shuffle_row_groups=shuffle,
        shard_seed=2019,
        shard_count=world_size,
        cur_shard=flow.env.get_rank(),
    )


class OneEmbedding(nn.Module):
    def __init__(
        self,
        table_name,
        embedding_vec_size,
        persistent_path,
        table_size_array,
        store_type,
        cache_memory_budget_mb,
        size_factor,
    ):
        assert table_size_array is not None
        vocab_size = sum(table_size_array)

        tables = [
            flow.one_embedding.make_table_options(
                [
                    flow.one_embedding.make_column_options(
                        flow.one_embedding.make_normal_initializer(mean=0, std=1e-4)
                    ),
                    flow.one_embedding.make_column_options(
                        flow.one_embedding.make_normal_initializer(mean=0, std=1e-4)
                    ),
                ]
            )
            for _ in range(len(table_size_array))
        ]

        if store_type == "device_mem":
            store_options = flow.one_embedding.make_device_mem_store_options(
                persistent_path=persistent_path, capacity=vocab_size, size_factor=size_factor,
            )
        elif store_type == "cached_host_mem":
            assert cache_memory_budget_mb > 0
            store_options = flow.one_embedding.make_cached_host_mem_store_options(
                cache_budget_mb=cache_memory_budget_mb,
                persistent_path=persistent_path,
                capacity=vocab_size,
                size_factor=size_factor,
            )
        elif store_type == "cached_ssd":
            assert cache_memory_budget_mb > 0
            store_options = flow.one_embedding.make_cached_ssd_store_options(
                cache_budget_mb=cache_memory_budget_mb,
                persistent_path=persistent_path,
                capacity=vocab_size,
                size_factor=size_factor,
            )
        else:
            raise NotImplementedError("not support", store_type)

        super(OneEmbedding, self).__init__()
        self.one_embedding = flow.one_embedding.MultiTableMultiColumnEmbedding(
            name=table_name,
            embedding_dim=embedding_vec_size,
            dtype=flow.float,
            key_type=flow.int64,
            tables=tables,
            store_options=store_options,
        )

    def forward(self, ids):
        return self.one_embedding.forward(ids)


class DNN(nn.Module):
    def __init__(
        self,
        in_features,
        hidden_units,
        out_features,
        skip_final_activation=False,
        dropout=0.0,
        fused=True,
    ) -> None:
        super(DNN, self).__init__()
        if fused:
            self.dropout_rates = [dropout] * len(hidden_units)
            self.linear_layers = nn.FusedMLP(
                in_features,
                hidden_units,
                out_features,
                self.dropout_rates,
                0.0,
                skip_final_activation,
            )
        else:
            denses = []
            dropout_rates = [dropout] * len(hidden_units) + [0.0]
            use_relu = [True] * len(hidden_units) + [not skip_final_activation]
            hidden_units = [in_features] + hidden_units + [out_features]
            for idx in range(len(hidden_units) - 1):
                denses.append(nn.Linear(hidden_units[idx], hidden_units[idx + 1], bias=True))
                if use_relu[idx]:
                    denses.append(nn.ReLU())
                if dropout_rates[idx] > 0:
                    denses.append(nn.Dropout(p=dropout_rates[idx]))
            self.linear_layers = nn.Sequential(*denses)

        for name, param in self.linear_layers.named_parameters():
            if "weight" in name:
                nn.init.xavier_normal_(param)
            elif "bias" in name:
                param.data.fill_(0.0)

    def forward(self, x: flow.Tensor) -> flow.Tensor:
        return self.linear_layers(x)


def interaction(embedded_x: flow.Tensor) -> flow.Tensor:
    return flow._C.fused_dot_feature_interaction([embedded_x], pooling="sum")


class DeepFMModule(nn.Module):
    def __init__(
        self,
        embedding_vec_size=128,
        dnn=[1024, 1024, 512, 256],
        use_fusedmlp=True,
        persistent_path=None,
        table_size_array=None,
        one_embedding_store_type="cached_host_mem",
        cache_memory_budget_mb=8192,
        dropout=0.2,
    ):
        super(DeepFMModule, self).__init__()

        self.embedding_vec_size = embedding_vec_size

        self.embedding_layer = OneEmbedding(
            table_name="sparse_embedding",
            embedding_vec_size=[embedding_vec_size, 1],
            persistent_path=persistent_path,
            table_size_array=table_size_array,
            store_type=one_embedding_store_type,
            cache_memory_budget_mb=cache_memory_budget_mb,
            size_factor=3,
        )

        self.dnn_layer = DNN(
            in_features=embedding_vec_size * (num_dense_fields + num_sparse_fields),
            hidden_units=dnn,
            out_features=1,
            skip_final_activation=True,
            dropout=dropout,
            fused=use_fusedmlp,
        )

    def forward(self, inputs) -> flow.Tensor:
        multi_embedded_x = self.embedding_layer(inputs)
        embedded_x = multi_embedded_x[:, :, 0 : self.embedding_vec_size]
        lr_embedded_x = multi_embedded_x[:, :, -1]

        # FM
        lr_out = flow.sum(lr_embedded_x, dim=1, keepdim=True)
        dot_sum = interaction(embedded_x)
        fm_pred = lr_out + dot_sum

        # DNN
        dnn_pred = self.dnn_layer(embedded_x.flatten(start_dim=1))

        return fm_pred + dnn_pred


def make_deepfm_module(args):
    model = DeepFMModule(
        embedding_vec_size=args.embedding_vec_size,
        dnn=args.dnn,
        use_fusedmlp=not args.disable_fusedmlp,
        persistent_path=args.persistent_path,
        table_size_array=args.table_size_array,
        one_embedding_store_type=args.store_type,
        cache_memory_budget_mb=args.cache_memory_budget_mb,
        dropout=args.net_dropout,
    )
    return model


class DeepFMValGraph(flow.nn.Graph):
    def __init__(self, deepfm_module, amp=False):
        super(DeepFMValGraph, self).__init__()
        self.module = deepfm_module
        if amp:
            self.config.enable_amp(True)

    def build(self, features):
        predicts = self.module(features.to("cuda"))
        return predicts.sigmoid()


class DeepFMTrainGraph(flow.nn.Graph):
    def __init__(
        self, deepfm_module, loss, optimizer, grad_scaler=None, amp=False, lr_scheduler=None,
    ):
        super(DeepFMTrainGraph, self).__init__()
        self.module = deepfm_module
        self.loss = loss
        self.add_optimizer(optimizer, lr_sch=lr_scheduler)
        self.config.allow_fuse_model_update_ops(True)
        self.config.allow_fuse_add_to_output(True)
        self.config.allow_fuse_cast_scale(True)
        if amp:
            self.config.enable_amp(True)
            self.set_grad_scaler(grad_scaler)

    def build(self, labels, features):
        logits = self.module(features.to("cuda"))
        loss = self.loss(logits, labels.to("cuda"))
        loss.backward()
        return loss.to("cpu")


def make_lr_scheduler(args, optimizer):
    batches_per_epoch = math.ceil(args.num_train_samples / args.batch_size)
    milestones = [
        batches_per_epoch * (i + 1)
        for i in range(math.floor(math.log(args.min_lr / args.learning_rate, args.lr_factor)))
    ]
    multistep_lr = flow.optim.lr_scheduler.MultiStepLR(
        optimizer=optimizer, milestones=milestones, gamma=args.lr_factor,
    )

    return multistep_lr


def get_metrics(logs):
    kv = {"auc": 1, "logloss": -1}
    monitor_value = 0
    for k, v in kv.items():
        monitor_value += logs.get(k, 0) * v
    return monitor_value


def early_stop(epoch, monitor_value, best_metric, stopping_steps, patience=2, min_delta=1e-6):
    rank = flow.env.get_rank()
    stop_training = False
    save_best = False
    if monitor_value < best_metric + min_delta:
        stopping_steps += 1
        if rank == 0:
            print("Monitor(max) STOP: {:.6f}!".format(monitor_value))
    else:
        stopping_steps = 0
        best_metric = monitor_value
        save_best = True
    if stopping_steps >= patience:
        stop_training = True
        if rank == 0:
            print(f"Early stopping at epoch={epoch}!")
    return stop_training, best_metric, stopping_steps, save_best


def train(args):
    rank = flow.env.get_rank()

    deepfm_module = make_deepfm_module(args)
    deepfm_module.to_global(flow.env.all_device_placement("cuda"), flow.sbp.broadcast)

    def load_model(dir):
        if rank == 0:
            print(f"Loading model from {dir}")
        if os.path.exists(dir):
            state_dict = flow.load(dir, global_src_rank=0)
            deepfm_module.load_state_dict(state_dict, strict=False)
        else:
            if rank == 0:
                print(f"Loading model from {dir} failed: invalid path")

    if args.model_load_dir:
        load_model(args.model_load_dir)

    def save_model(subdir):
        if not args.model_save_dir:
            return
        save_path = os.path.join(args.model_save_dir, subdir)
        if rank == 0:
            print(f"Saving model to {save_path}")
        state_dict = deepfm_module.state_dict()
        flow.save(state_dict, save_path, global_dst_rank=0)

    if args.save_initial_model:
        save_model("initial_checkpoint")

    # TODO: clip gradient norm
    opt = flow.optim.Adam(deepfm_module.parameters(), lr=args.learning_rate)
    lr_scheduler = make_lr_scheduler(args, opt)
    loss = flow.nn.BCEWithLogitsLoss(reduction="mean").to("cuda")

    if args.loss_scale_policy == "static":
        grad_scaler = flow.amp.StaticGradScaler(1024)
    else:
        grad_scaler = flow.amp.GradScaler(
            init_scale=1073741824, growth_factor=2.0, backoff_factor=0.5, growth_interval=2000,
        )

    eval_graph = DeepFMValGraph(deepfm_module, args.amp)
    train_graph = DeepFMTrainGraph(
        deepfm_module, loss, opt, grad_scaler, args.amp, lr_scheduler=lr_scheduler
    )

    batches_per_epoch = math.ceil(args.num_train_samples / args.batch_size)

    best_metric = -np.inf
    stopping_steps = 0
    save_best = False
    stop_training = False

    cached_eval_batches = prefetch_eval_batches(
        f"{args.data_dir}/val", args.batch_size, math.ceil(args.num_val_samples / args.batch_size),
    )

    deepfm_module.train()
    epoch = 0
    with make_criteo_dataloader(f"{args.data_dir}/train", args.batch_size) as loader:
        step, last_step, last_time = -1, 0, time.time()
        for step in range(1, args.train_batches + 1):
            labels, features = batch_to_global(*next(loader))
            loss = train_graph(labels, features)
            if step % args.loss_print_interval == 0:
                loss = loss.numpy()
                if rank == 0:
                    latency = (time.time() - last_time) / (step - last_step)
                    throughput = args.batch_size / latency
                    last_step, last_time = step, time.time()
                    strtime = time.strftime("%Y-%m-%d %H:%M:%S")
                    print(
                        f"Rank[{rank}], Step {step}, Loss {loss:0.4f}, "
                        + f"Latency {(latency * 1000):0.3f} ms, Throughput {throughput:0.1f}, {strtime}"
                    )

            if step % batches_per_epoch == 0:
                epoch += 1
                auc, logloss = eval(
                    args,
                    eval_graph,
                    tag="val",
                    cur_step=step,
                    epoch=epoch,
                    cached_eval_batches=cached_eval_batches,
                )
                if args.save_model_after_each_eval:
                    save_model(f"step_{step}_val_auc_{auc:0.5f}")

                monitor_value = get_metrics(logs={"auc": auc, "logloss": logloss})

                stop_training, best_metric, stopping_steps, save_best = early_stop(
                    epoch,
                    monitor_value,
                    best_metric=best_metric,
                    stopping_steps=stopping_steps,
                    patience=args.patience,
                    min_delta=args.min_delta,
                )

                if args.save_best_model and save_best:
                    if rank == 0:
                        print(f"Save best model: monitor(max): {best_metric:.6f}")
                    save_model("best_checkpoint")

                if not args.disable_early_stop and stop_training:
                    break

                deepfm_module.train()
                last_time = time.time()

    if args.save_best_model:
        load_model(f"{args.model_save_dir}/best_checkpoint")
    if rank == 0:
        print("================ Test Evaluation ================")
    eval(args, eval_graph, tag="test", cur_step=step, epoch=epoch)

    if args.save_graph_for_serving:
        del eval_graph
        recompiled_eval_graph = compile_eval_graph(args, deepfm_module, tag="test")
        eval_state_dict = recompiled_eval_graph.state_dict()
        flow.save(recompiled_eval_graph, args.model_serving_path)


def np_to_global(np):
    t = flow.from_numpy(np)
    return t.to_global(placement=flow.env.all_device_placement("cpu"), sbp=flow.sbp.broadcast)


def batch_to_global(np_label, np_features, is_train=True):
    labels = np_to_global(np_label.reshape(-1, 1)) if is_train else np_label.reshape(-1, 1)
    features = np_to_global(np_features)
    return labels, features


def prefetch_eval_batches(data_dir, batch_size, num_batches):
    cached_eval_batches = []
    with make_criteo_dataloader(data_dir, batch_size, shuffle=False) as loader:
        for _ in range(num_batches):
            label, features = batch_to_global(*next(loader), is_train=False)
            cached_eval_batches.append((label, features))
    return cached_eval_batches


def eval(args, eval_graph, tag="val", cur_step=0, epoch=0, cached_eval_batches=None):
    if tag == "val":
        batches_per_epoch = math.ceil(args.num_val_samples / args.batch_size)
    else:
        batches_per_epoch = math.ceil(args.num_test_samples / args.batch_size)

    eval_graph.module.eval()
    labels, preds = [], []
    eval_start_time = time.time()

    if cached_eval_batches == None:
        with make_criteo_dataloader(
            f"{args.data_dir}/{tag}", args.batch_size, shuffle=False
        ) as loader:
            eval_start_time = time.time()
            for i in range(batches_per_epoch):
                label, features = batch_to_global(*next(loader), is_train=False)
                pred = eval_graph(features)
                labels.append(label)
                preds.append(pred.to_local())
    else:
        for i in range(batches_per_epoch):
            label, features = cached_eval_batches[i]
            pred = eval_graph(features)
            labels.append(label)
            preds.append(pred.to_local())

    labels = (
        np_to_global(np.concatenate(labels, axis=0)).to_global(sbp=flow.sbp.broadcast()).to_local()
    )
    preds = (
        flow.cat(preds, dim=0)
        .to_global(placement=flow.env.all_device_placement("cpu"), sbp=flow.sbp.split(0))
        .to_global(sbp=flow.sbp.broadcast())
        .to_local()
    )

    flow.comm.barrier()
    eval_time = time.time() - eval_start_time

    rank = flow.env.get_rank()

    metrics_start_time = time.time()
    auc = flow.roc_auc_score(labels, preds).numpy()[0]
    logloss = flow._C.binary_cross_entropy_loss(preds, labels, weight=None, reduction="mean")
    metrics_time = time.time() - metrics_start_time

    if rank == 0:
        host_mem_mb = psutil.Process().memory_info().rss // (1024 * 1024)
        stream = os.popen("nvidia-smi --query-gpu=memory.used --format=csv")
        device_mem_str = stream.read().split("\n")[rank + 1]

        strtime = time.strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"Rank[{rank}], Epoch {epoch}, Step {cur_step}, AUC {auc:0.6f}, LogLoss {logloss:0.6f}, "
            + f"Eval_time {eval_time:0.2f} s, Metrics_time {metrics_time:0.2f} s, Eval_samples {labels.shape[0]}, "
            + f"GPU_Memory {device_mem_str}, Host_Memory {host_mem_mb} MiB, {strtime}"
        )

    return auc, logloss


def compile_eval_graph(args, deepfm_module, tag="val"):
    eval_graph = DeepFMValGraph(deepfm_module, args.amp)
    eval_graph.module.eval()
    with make_criteo_dataloader(f"{args.data_dir}/{tag}", args.batch_size, shuffle=False) as loader:
        label, features = batch_to_global(*next(loader), is_train=False)
        # Cause we want to infer to GPU, so here set is_train as True to place input Tensor in CUDA Device
        features = features.to("cuda")
        pred = eval_graph(features)
    return eval_graph


if __name__ == "__main__":
    os.system(sys.executable + " -m oneflow --doctor")
    flow.boxing.nccl.enable_all_to_all(True)
    args = get_args()
    train(args)
