# DolphinScheduler

## Guide
The whole process of machine learning from development to deployment, various tasks such as data processing, feature development, and model training demand significant time and effort. To streamline the deveopment and deployment of AI models and simplify the overall machine-learning process, we have introduced the DolphinScheduler OpenMLDB Task. This task seamlessly integrates the capabilities of the feature platform into DolphinScheduler's workflow, effectively bridging feature engineering with scheduling functionalities, resulting in a comprehensive end-to-end MLOps workflow. In this article, we present a concise introduction and practical demonstration of the procedures for using DolphinScheduler OpenMLDB Task.

```{seealso}
For detailed information on the OpenMLDB Task, please refer to the [DolphinScheduler OpenMLDB Task Official Documentation](https://dolphinscheduler.apache.org/zh-cn/docs/3.1.5/guide/task/openmldb).
```

## Scenarios and Functions
### Why Develop DolphinScheduler OpenMLDB Task

![eco](images/ecosystem.png)

As an open-source machine learning database providing a comprehensive solution for production-level data and feature development, the key to enhancing OpenMLDB's usability and reducing usage barriers lies in upstream and downstream connectivity. As depicted in the diagram above, accessing the data source allows seamless data flow from DataOps into OpenMLDB, and the features provided by OpenMLDB need to smoothly integrate with ModelOps for training. To alleviate the significant workload resulting from manual connections by developers and enhance the convenience of using OpenMLDB, we have developed OpenMLDB connection deployment and monitoring functions. The focus of this presentation is to introduce the framework for integrating OpenMLDB into the DolphinScheduler workflow. The DolphinScheduler OpenMLDB Task simplifies the operation of OpenMLDB, and OpenMLDB tasks are efficiently managed by Workflow, enabling greater automation.

### What Can DolphinScheduler OpenMLDB Task Do

OpenMLDB aims to expedite development launch, enabling developers to focus on the essence of their work rather than expending excessive effort on engineering implementation. By writing OpenMLDB Tasks, we can fulfil the offline import, feature extraction, SQL deployment, and online import requirements of OpenMLDB. Furthermore, we can also implement complete training and online processes using OpenMLDB in DolphinScheduler.

![task func](images/task_func.png)

For instance, the most straightforward user operation process we envision, as illustrated in the diagram above, involves steps 1-4: offline import, feature extraction, SQL deployment online, and online import. All of these steps can be achieved by utilizing the DolphinScheduler OpenMLDB Task.

In addition to SQL execution in OpenMLDB, real-time prediction also requires model deployment. Therefore, we will demonstrate how to utilize the DolphinScheduler OpenMLDB Task to coordinate a comprehensive machine learning training and online process, based on the TalkingData advertising fraud detection scenario from the Kaggle competition. Further information about the TalkingData competition can be found at [talkingdata-adtracking-fraud-detection](https://www.kaggle.com/competitions/talkingdata-adtracking-fraud-detection/discussion).

## Practical Demonstration
### Environmental Configuration

**Run OpenMLDB Image**

The test can be executed on macOS or Linux, and we recommend performing the demonstration testing within the provided OpenMLDB image. In this setup, both OpenMLDB and DolphinScheduler will be launched inside the container, with the web port of DolphinScheduler exposed.
```
docker run -it -p 12345:12345 4pdosc/openmldb:0.8.0 bash
```
```{attention}
For proper configuration of DolphinScheduler, the tenant should be set up as a user of the operating system, and this user must have sudo permissions. It is advised to download and initiate DolphinScheduler within the OpenMLDB container. Otherwise, please ensure that the operating system users with sudo permissions are prepared.
```

As our current Docker image does not have sudo installed, and DolphinScheduler requires sudo for running workflows, please install sudo in the container first: [Instructions For Installing Sudo] (provide relevant instructions).
```
apt update && apt install sudo
```

The DolphinScheduler runs the task using sh, but our docker defaults to sh as `dash`. We have modified it to `bash`:
```
dpkg-reconfigure dash
```
Enter `no`.

**Source Data Preparation**

The workflow used in the following text will start from `/tmp/train'_ Sample. csv ` to import data to OpenMLDB, so first download the source data to this address:
```
curl -SLo /tmp/train_sample.csv https://openmldb.ai/download/dolphinschduler-task/train_sample.csv
```

**Run OpenMLDB Cluster and Predict Server**

Run the following command in the container of OpenMLDB cluster：
```
/work/init.sh
```

We will accomplish a workflow that includes data import, offline training, and successful model launch. For the online part of the model, the model address will be sent to the predict server for completion. Let's begin by downloading and running the predict server in the background:
```
cd /work
curl -SLo predict_server.py https://openmldb.ai/download/dolphinschduler-task/predict_server.py
python3 predict_server.py --no-init > predict.log 2>&1 &
```
```{tip}
If an error is returned in the 'online prediction test', please check the log `/work/predict.log`.
```

**Download and Run DolphinScheduler**

Please note that DolphinScheduler supports OpenMLDB Task versions 3.1.3 and above. In this article, we will be using version 3.1.5, which can be downloaded from the [Official Website](https://dolphinscheduler.apache.org/zh-cn/download/3.1.5) or from a mirrored website.

To start the DolphinScheduler standalone, follow the steps outlined in the [Official Documentation](https://dolphinscheduler.apache.org/zh-cn/docs/3.1.5/guide/installation/standalone) for more information.

```
# Official
curl -SLO https://dlcdn.apache.org/dolphinscheduler/3.1.5/apache-dolphinscheduler-3.1.5-bin.tar.gz
# Image curl -SLO http://openmldb.ai/download/dolphinschduler-task/apache-dolphinscheduler-dev-3.1.5-bin.tar.gz
tar -xvzf apache-dolphinscheduler-*-bin.tar.gz
cd apache-dolphinscheduler-*-bin
sed -i s#/opt/soft/python#/usr/bin/python3#g bin/env/dolphinscheduler_env.sh
./bin/dolphinscheduler-daemon.sh start standalone-server
```

```{hint}
In the official release version of DolphinScheduler, there is an issue with OpenMLDB Task in versions older than 3.1.3, making it incompatible for direct use. If you are using an older version, you can contact us to obtain a corresponding version of OpenMLDB Task for fixing. This problem has been resolved in versions 3.1.3 and later, making them suitable for use with the official release version.

In other versions of DolphinScheduler, there may be a change in `bin/env/dolphinscheduler_env.sh`. If `bin/env/dolphinscheduler_env.sh` does not exist in `PYTHON_HOME`, additional configuration is required. You can modify it using the command `echo "export PYTHON_HOME=/usr/bin/python3" >>bin/env/dolphinscheduler_env.sh`.
```

To access the system UI, open your browser and go to the address http://localhost:12345/dolphinscheduler/ui (the default configuration allows cross-host access, but you need to ensure a smooth IP connection). The default username and password are admin/dolphinscheduler123.

```{note}
The DolphinScheduler worker server requires the OpenMLDB Python SDK. For the DolphinScheduler standalone worker, it is native, so you only need to install the OpenMLDB Python SDK locally. We have already installed it in our OpenMLDB image. If you are in a different environment, please install the openmldb SDK using the command `pip3 install openmldb`.
```

**Download Workflow Configuration**

Workflow can be manually created, but for the purpose of simplifying the demonstration, we have provided a JSON workflow file directly, which you can download from the following link: [Click to Download](http://openmldb.ai/download/dolphinschduler-task/workflow_openmldb_demo.json). Later, you can upload this file directly to the DolphinScheduler environment and make simple modifications (as shown in the demonstration below) to complete the entire workflow.

Please note that the download will not be saved within the container but to the browser host you are using. The upload will be done on the web page later.

### Demonstration

#### 1. Initial Configuration

To create a tenant in the DolphinScheduler web, navigate to the tenant management interface, and fill in the required fields, including the **Operating system user with sudo permission**, and use the default settings for the queue. The Docker container can directly use the root user.

![create tenant](images/ds_create_tenant.png)

Bind the tenant to the user again. For simplicity, we directly bind to the admin user. Enter to the User Management page and click Edit Admin User.

![bind tenant](images/ds_bind_tenant.png)

After binding, the user status is similar as shown below.
![bind status](images/ds_bind_status.png)

#### 2. Create Workflow
In DolphinScheduler, you need to create a project first, and then create a workflow within that project.

To begin, create a test project. As shown in the following figure, click on "Create Project" and enter the project name.

![create project](images/ds_create_project.png)

![project](images/ds_project.png)

Once inside the project, you can import the downloaded workflow file. In the workflow definition interface, click on "Import Workflow" as depicted in the following figure.

![import workflow](images/ds_import_workflow.png)

After importing, the workflow table will show as follows.

![workflow list](images/ds_workflow_list.png)

Click on the workflow name to view the detailed content of the workflow, as shown in the following figure.

![workflow detail](images/ds_workflow_detail.png)

**Note**: A minor modification is required here since the task ID will change after importing the workflow. Specifically, the upstream and downstream IDs in the switch task do not exist and need to be manually adjusted.

![switch](images/ds_switch.png)

As depicted in the above figure, there are non-existent IDs in the settings of the switch task. Please modify the "branch flow" and "pre-check conditions" for successful and failed workflows to match the tasks of the current workflow.

The correct results are shown in the following figure:

![right](images/ds_switch_right.png)

Once the modifications are completed, save the workflow directly. The default value for the tenant in the imported workflow is "default," which is also **executable**. If you want to specify your own tenant, please select the tenant when saving the workflow, as shown in the following figure.

![set tenant](images/ds_set_tenant.png)

#### 3. Run Online Workflow

After saving the workflow, it needs to be launched before running. Once it goes online, the run button will be activated. As illustrated in the following figure.

![run](images/ds_run.png)

After clicking "Run," wait for the workflow to complete. You can view the details of the workflow operation in the Workflow Instance interface, as shown in the following figure.
![run status](images/ds_run_status.png)

To demonstrate the process of successful product launch, validation was not actually validated but returned a successful validation and flowed into the deploy branch. After running the deploy branch and successfully deploying SQL and subsequent tasks, the predict server receives the latest model.

```{note}
If the `Failed` notification appears on the workflow instance, please click on the instance name and go to the detailed page to see which task execution error occurred. Double click on the task and click on "View Log" in the upper right corner to view detailed error information.

`load offline data`, `feature extraction`, and `load online` may display successful task execution in the DolphinScheduler, but actual task execution fails in OpenMLDB. This may lead to errors in the `train` task, where there is no source feature data to concatenate (Traceback `pd.concat`).

When such problems occur, please query the true status of each task in OpenMLDB and run it directly using the command: `echo "show jobs;" | /work/openmldb/bin/openmldb --zk_cluster=127.0.1:2181 --zk_root_path=/openmldb --role=SQL_client`. If the status of a task is `FAILED`, please query the log of that task. The method can be found in [Task Log](../quickstart/beginninger_mustread.md#offline).
```

#### 4. Online Predictive Testing
The predict server also provides online prediction services, requesting through `curl/predict`. We simply construct a real-time request and send it to the predict server.
```
curl -X POST 127.0.0.1:8881/predict -d '{"ip": 114904,
       "app": 11,
       "device": 1,
       "os": 15,
       "channel": 319,
       "click_time": 1509960088000,
       "is_attributed": 0}'
```
The return result is as follow：

![predict](images/ds_predict.png)

#### In Addition

If the workflow is run repeatedly, the `deploy SQL` task may fail because the deployment `demo` already exists. Please delete the deployment in the Docker container before running the workflow again:
```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client --database=demo_db --interactive=false --cmd="drop deployment demo;"
```

You can confirm whether the deployment has been deleted by using the following command:
```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client --database=demo_db --interactive=false --cmd="show deployment demo;"
```

Restart the DolphinScheduler server (Note that restarting this will clear the metadata and require reconfiguring the environment and creating workflows):
```
./bin/dolphinscheduler-daemon.sh stop standalone-server
./bin/dolphinscheduler-daemon.sh start standalone-server
```

If you want to preserve metadata, please refer to [Pseudo Cluster Deployment](https://dolphinscheduler.apache.org/zh-cn/docs/3.1.5/guide/installation/pseudo-cluster) to configure the database.
