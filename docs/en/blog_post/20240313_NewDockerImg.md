# Mastering Distributed Database Development in 10 Minutes with OpenMLDB Developer Docker Image

![](https://cdn-images-1.medium.com/max/7680/1*qiSmedKZYmkNz45_pcms3w.png)

[OpenMLDB](https://github.com/4paradigm/OpenMLDB) is an open-source, distributed in-memory database system designed for time-series data. It focuses on high performance, reliability, and scalability, making it suitable for handling massive time-series data and real-time computation of online features. In the wave of big data and machine learning, OpenMLDB has emerged as a promising player in the open-source database field, thanks to its powerful data processing capabilities and efficient support for machine learning.

The core storage and SQL engine of OpenMLDB consist of over 360,000 lines of C++ code and a massive amount of C header files. To further reduce the project compilation threshold and enhance developers’ efficiency, we have introduced a newly designed OpenMLDB Docker image. This allows developers to quickly compile the database source code from scratch on any operating system platform, including Linux, MacOS, Windows, etc. With just ten minutes, developers can join as contributors to the development of distributed databases.

## Usage

The mirror is currently hosted on the Alibaba Cloud Mirror Repository. The process for using the mirror is as follows:

 1. Start container: Use Docker commands to start the container. This will initiate an environment containing the OpenMLDB source code and all dependencies.
```bash
docker run -it registry.cn-beijing.aliyuncs.com/openmldb/openmldb-build bash
```
2. Compile OpenMLDB: Inside the container, you can directly navigate to the OpenMLDB source code directory and execute the compilation script.
```bash
cd OpenMLDB
make
```
3. Install OpenMLDB, default installation path is ${PROJECT_ROOT}/openmldb
```bash
make install
```
4. Deployment and Testing: After the compilation is complete, you can proceed with deployment and testing accordingly. All necessary tools and dependencies are already prepared and ready to use.

## Concurrent Compilation Time

OpenMLDB disables concurrent compilation by default. However, if the resources on the compilation machine are sufficient, you can enable concurrent compilation using the compilation parameter NPROC. Here we list the time required for concurrent compilation.

### 1. 4-core Compilation
```bash
    make NPROC=4
```
![](https://cdn-images-1.medium.com/max/2000/0*PxiBv02dcLlqT6v4)

### 2. 8-core Compilation
```bash
    make NPROC=8
```
![](https://cdn-images-1.medium.com/max/2000/0*N1-PWLtPlGHTkXBl)

### 3. 16-core Compilation
```bash
    make NPROC=16
```
![](https://cdn-images-1.medium.com/max/2000/0*nj1LbYr7AJQIDUIE)

## Highlights

 1. **Quick Start**: Eliminates complex setup steps, allowing developers to quickly enter development mode on different operating system platforms.

 2. **Unified Environment**: Whether for individual development or team collaboration, the Docker image ensures that each member develops in a consistent environment, effectively avoiding the “it works on my machine” problem.

 3. **Easy Sharing**: The image can be easily shared with other team members or distributed in the community, accelerating the adoption and application of OpenMLDB.

 4. **Complete OpenMLDB Environment**: The image comes pre-installed with the complete source code of OpenMLDB, enabling developers to easily explore and modify the OpenMLDB source code and contribute to the OpenMLDB community.

 5. **Offline Compilation and Deployment Capabilities**: By pre-downloading the third-party libraries required by OpenMLDB, the image can compile and deploy OpenMLDB in a completely offline environment. This greatly improves work efficiency in network-restricted environments, enhancing the flexibility and feasibility of development.

 6. **Compilation Efficiency**: Since all dependencies are already built into the image, this avoids lengthy dependency download and installation processes, making the compilation process much faster.

This custom Docker image tailored for offline building of OpenMLDB not only simplifies the onboarding process for developers but also provides robust support for project compilation, deployment, and testing. We anticipate that this tool will help more developers and enterprises leverage OpenMLDB more efficiently, enabling them to control the compilation and development capabilities of OpenMLDB at the source code level. Moreover, with the enhanced development and application capabilities, we look forward to seeing OpenMLDB further develop and apply in industry ecosystems such as financial risk control, recommendation systems, and quantitative trading.

--------------------------------------------------------------------------------------------------------------

**For more information on OpenMLDB:**
* Official website: [https://openmldb.ai/](https://openmldb.ai/)
* GitHub: [https://github.com/4paradigm/OpenMLDB](https://github.com/4paradigm/OpenMLDB)
* Documentation: [https://openmldb.ai/docs/en/](https://openmldb.ai/docs/en/)
* Join us on [**Slack**](https://join.slack.com/t/openmldb/shared_invite/zt-ozu3llie-K~hn9Ss1GZcFW2~K_L5sMg)!

> _This post is a re-post from [OpenMLDB Blogs](https://openmldb.medium.com/)._