# IP Configuration
## Physical Environment IP
To access the OpenMLDB service across hosts, you need to change the endpoint configuration `127.0.0.1` in the OpenMLDB configuration to `0.0.0.0` or the public IP, and then start the OpenMLDB service. Make sure that the port is not blocked by a firewall.
```{attention}
In the stand-alone version, not only does the endpoint needs to be changed, but also the tablet ip `--tablet=` in the nameserver configuration, which also needs to be modified here.
````

## Docker IP

If you want to access the inside of the container from outside the container (whether on the same host or across hosts), please first
Change the endpoint `127.0.0.1` to `0.0.0.0` (the `tablet` configuration item in the stand-alone version also needs to be changed) to avoid unnecessary trouble.

### External access to the container (same host)
In the same host, if you want to access the OpenMLDB server started in the container, from outside the container (physical machine or other container), you can connect directly using bridge, you can expose the port, or you can use the host network mode directly.

```{caution}
Docker Desktop for Mac does not support accessing containers from physical machines (neither of the following modes), refer to [i-cannot-ping-my-containers](https://docs.docker.com/desktop/mac/networking/#i -cannot-ping-my-containers).


But in macOS, other containers can be accessed from within a container.
```

#### Bridge Connection
The bridge connection does not need to change the docker run command, just query the bridge ip.
```
docker network inspect bridge
```

Looking at the "Containers" field, you can see the ip bound to each container, and the client can access it using this ip.

For example, after starting the container and running OpenMLDB standalone, the inspect result is `172.17.0.2`, then the CLI connection can be used:
```
../openmldb/bin/openmldb --host 172.17.0.2 --port 6527
```

#### Exposed Port
Expose the port through `-p` when starting the container, and the client can access it using the local ip address or the loopback address.

The stand-alone version needs to expose the ports of three components (nameserver, tabletserver, apiserver):
```
docker run -p 6527:6527 -p 9921:9921 -p 8080:8080 -it 4pdosc/openmldb:0.8.4 bash
```

The cluster version needs to expose the zk port and the ports of all components:
```
docker run -p 2181:2181 -p 7527:7527 -p 10921:10921 -p 10922:10922 -p 8080:8080 -p 9902:9902 -it 4pdosc/openmldb:0.8.4 bash
```

```{tip}
`-p` Binding the "physical machine port" and "in-container port" may cause the "container port number" to be used on the physical machine.

If the OpenMLDB service is only in a single container, it only needs to change the exposed physical machine port number, and the client changes the access port accordingly. The configuration items of each service process do not need to be changed.

If the OpenMLDB service process is distributed, the "port number is occupied" appears in multiple containers. We do not recommend the method of "switching the exposed port number". Please change the configured port number and use the same port number when exposing.
```

#### Host Network
Or more conveniently, use host networking without port isolation, for example:
```
docker run --network host -it 4pdosc/openmldb:0.8.4 bash
```
But in this case, it is easy to find that the port is occupied by other processes in the host. If occupancy occurs, change the port number carefully.

### Access native containers across hosts
Since bridge mode cannot achieve cross-host access, the methods of exposing ports and host network can achieve **cross-host** access to native containers.