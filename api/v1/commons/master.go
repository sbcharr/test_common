package commons

import (
	"fmt"
	"bytes"
	"net/http"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/samuel/go-zookeeper/zk"
	mesosproto "github.com/mesos/test_common/api/v1"
	log "github.com/golang/glog"
)

func ConnectToMaster(master string, body []byte) (*http.Request, error) {
	url := fmt.Sprintf("http://%s/api/v1/scheduler", master)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(body)))
	if err != nil {
		log.Error("error creating a new request", err)
		return nil, err
	}
	//req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Type", "application/x-protobuf")
	return req, nil
}

func FindMasterLeader(zks []string) (string, error) {
	masterInfo := new(mesosproto.MasterInfo)
	path := "/mesos"
	conn, _, err := zk.Connect(zks, time.Second)
	if err != nil {
		log.Error("Couldn't connect to zookeeper: ", err)
		return "", err
	}
	defer conn.Close()
	// find mesos master
	children, _, err := conn.Children(path)
	if err != nil {
		log.Error("error connecting to zookeeper children: ", err)
	}
	for _, name := range children {
		data, _, err := conn.Get(path + "/" + name)
		if err != nil {
			log.Error("Error getting master info: ", err)
			return "", err
		}
		err = jsonpb.Unmarshal(bytes.NewBuffer(data), masterInfo)
		if err == nil {
			break
		} else {
			log.Error("error unmarshalling into masterinfo: ", err)
			return "", err
		}

	}
	mesosmaster := fmt.Sprintf("%s:%d", masterInfo.GetHostname(), masterInfo.GetPort())
	return mesosmaster, nil
}
