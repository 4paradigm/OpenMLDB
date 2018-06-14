package status

import (
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/metricbeat/mb"
    "strings"
    "net/http"
    "fmt"
    "io/ioutil"
    "strconv"
    "os"
)

// init registers the MetricSet with the central registry.
// The New method will be called after the setup of the module and before starting to fetch data
func init() {
	if err := mb.Registry.AddMetricSet("brpc", "status", New); err != nil {
		panic(err)
	}
}

// MetricSet type defines all fields of the MetricSet
// As a minimum it must inherit the mb.BaseMetricSet fields, but can be extended with
// additional entries. These variables can be used to persist data or configuration between
// multiple fetch calls.
type MetricSet struct {
	mb.BaseMetricSet
	counter int
    podName string
    portFile string
    port string
}

// New create a new instance of the MetricSet
// Part of new is also setting up the configuration by processing additional
// configuration entries if needed.
func New(base mb.BaseMetricSet) (mb.MetricSet, error) {

	config := struct{
        portFile string `config:"portFile:"`
    }{
        portFile:   "" ,
    }

	logp.Warn("EXPERIMENTAL: The brpc status metricset is experimental")

	if err := base.Module().UnpackConfig(&config); err != nil {
		return nil, err
	}
    pod := os.Getenv("POD_NAME")
    envpath := GetConfFromEnv()
    port := envpath
	return &MetricSet{
		BaseMetricSet: base,
		counter:       1,
        podName: pod,
        portFile: config.portFile,
        port: port,
	}, nil
}
func GetConfFromEnv() (string){
    envpath := os.Getenv("SERVICE_POD_TARGET_PORT")
    fmt.Println("envpath:",envpath)
    return envpath
}
func GetConf(filename string)([]byte,error){
    conf,err := os.Open(filename)
    if err != nil{
        fmt.Println(err)
        return nil,err
    }
    return ioutil.ReadAll(conf)
}
// Fetch methods implements the data gathering and data conversion to the right format
// It returns the event which is then forward to the output. In case of an error, a
func (m *MetricSet) Fetch() ([]common.MapStr, error){
    var port string
    var err error
    var conf []byte
    if m.port == ""{
       filename := m.portFile
       conf,err = GetConf(filename)
       if err != nil{
          fmt.Println(err)
          return nil,err
        }
        port = string(conf)
    }else{
        port = m.port
    }
    portnum := string(port[:])
    portnum = strings.Replace(portnum, "\n", "", -1)
    url := "http://127.0.0.1:" + portnum + "/status"
    client := &http.Client{}
    req, err := http.NewRequest("GET",url,nil)
    if err != nil {
        fmt.Println(err)
        return nil,err
    }

    req.Header.Set("Content-Type", "text/plain")
    req.Header.Set("User-Agent", "curl/7.54.0")

    resp , err := client.Do(req)
    if err != nil{
        fmt.Println(err)
        return nil,err
    }
    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        fmt.Println(err)
        return nil,err
    }
    plain_text := string(body)

    new_plain_text := strings.Replace(plain_text,"\n"," ",-1)
    jsondata := strings.Fields(new_plain_text)
    //fmt.Println(jsondata)
    length := len(jsondata)
    fmt.Println(length)
    var events []common.MapStr
    var maxConcurrency = 0
    var methodName = ""
    var count = 0
    var errnum = 0
    var latency = 0
    var latency_50 = 0
    var latency_90 = 0
    var latency_99 = 0
    var latency_999 = 0
    var latency_9999 = 0
    var maxLatency = 0
    var qps = 0
    var processing = 0
    for i:=0 ; i < length; i++{
        if strings.Contains(jsondata[i],"Request"){
            methodName = jsondata[i - 1]
        }
        if strings.Contains(jsondata[i],"max_concurrency"){
            maxConcurrency ,err = strconv.Atoi(strings.Replace(jsondata[i],"max_concurrency=","",1))
        }
        if jsondata[i] == "count:"{
            count , err = strconv.Atoi(jsondata[i + 1])
            if err != nil{
                fmt.Println(err)
                return nil,err
            }
        }
        if jsondata[i] == "error:"{
            errnum , err = strconv.Atoi(jsondata[i + 1])
            if err != nil{
                fmt.Println(err)
                return nil,err
            }
        }
        if jsondata[i] == "latency:"{
            latency , err = strconv.Atoi(jsondata[i + 1])
            if err != nil{
                fmt.Println(err)
                return nil,err
            }
        }
        if jsondata[i] == "latency_50:"{
            latency_50 , err = strconv.Atoi(jsondata[i + 1])
            if err != nil{
                fmt.Println(err)
                return nil,err
            }
        }
        if jsondata[i] == "latency_90:"{
            latency_90 , err = strconv.Atoi(jsondata[i + 1])
            if err != nil{
                fmt.Println(err)
                return nil,err
            }
        }
        if jsondata[i] == "latency_99:"{
            latency_99 , err = strconv.Atoi(jsondata[i + 1])
            if err != nil{
                fmt.Println(err)
                return nil,err
            }
        }
        if jsondata[i] == "latency_999:"{
            latency_999 , err = strconv.Atoi(jsondata[i + 1])
            if err != nil{
                fmt.Println(err)
                return nil,err
            }
        }
        if jsondata[i] == "latency_9999:"{
            latency_9999 ,err = strconv.Atoi(jsondata[i + 1])
            if err != nil{
                fmt.Println(err)
                return nil,err
            }
        }
        if jsondata[i] == "max_latency:"{
            maxLatency ,err = strconv.Atoi(jsondata[i + 1])
            if err != nil{
                fmt.Println(err)
                return nil,err
            }
        }
        if jsondata[i] == "qps:"{
            qps ,err = strconv.Atoi(jsondata[i + 1])
            if err != nil{
                fmt.Println(err)
                return nil,err
            }
        }
        if jsondata[i] == "processing:"{
            processing , err = strconv.Atoi(jsondata[i + 1])
            if err != nil{
                fmt.Println(err)
                return nil,err
            }
            event := common.MapStr{
                "method_name" : methodName,
                "count" : count,
                "error" : errnum,
                "latency" : latency,
                "latency_50" : latency_50,
                "latency_90" : latency_90,
                "latency_99" : latency_99,
                "latency_999" : latency_999,
                "latency_9999" : latency_9999,
                "max_latency" : maxLatency,
                "qps" : qps,
                "processing" : processing,
                "pod_name" : m.podName,
            }
            if maxConcurrency != 0{
                event["max_concurrency"] = maxConcurrency
                maxConcurrency = 0
            }
           events = append(events, event)
        }
    }
    fmt.Printf("%#v", events)
    return events, err
}

