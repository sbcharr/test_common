package calls

import (
	"strings"
	"bytes"
	"strconv"
	"bufio"
	"sync"

	mesosproto "github.com/mesos/test_common/api/v1"
	"github.com/mesos/test_common/api/v1/client"
	sch "github.com/mesos/test_common/api/v1/scheduler"
	"github.com/golang/protobuf/proto"
	"github.com/mesos/test_common/api/v1/commons"
	"github.com/golang/protobuf/jsonpb"
	log "github.com/golang/glog"
)

func Subscribe(frameworkInfo *mesosproto.FrameworkInfo, zkList string, wg sync.WaitGroup) {
	ev := &sch.Event{}
	//resp := &http.Response{}
	httpCli := client.HTTPClient()
	subscribe := sch.Call_SUBSCRIBE
	reqType := &sch.Call{Type: &subscribe,
		Subscribe: &sch.Call_Subscribe{FrameworkInfo: frameworkInfo},
	}
	body, err := proto.Marshal(reqType)
	if err != nil {
		log.Error("error in converting body to protobuf format", err)
		wg.Done()
		//return
	}
	zks := strings.Split(zkList, ",")
	mesosmaster, err := commons.FindMasterLeader(zks)
	if err != nil {
		log.Error(err)
		wg.Done()
		//return
	}
	//TODO What happens when zookeeper returns a master ip, however, that server goes down before framework can call the server?...Add code to handle.

	req, err := commons.ConnectToMaster(mesosmaster, body)
	if err != nil {
		log.Error("error in building http new request to schedule: ", err)
		wg.Done()
		//return
	}

	resp, err := httpCli.Do(req)
	if err != nil {
		log.Error("error sending request: ", err)
		wg.Done()
		//return
	}

	Redirect:
	switch resp.Status {
	case "HTTP 307 Temporary Redirect":
		url, err := resp.Location()
		if err != nil {
			log.Error("url location error: ", err)
			wg.Done()
			//return
		}
		redirectHost := url.Hostname() + ":" + url.Port()
		//redirectPort := url.Port()
		log.Info("redirecting to host: ", redirectHost)
		req, err := commons.ConnectToMaster(redirectHost, body)
		if err != nil {
			log.Error("error in building http new request to schedule: ", err)
			wg.Done()
		}
		resp, err = httpCli.Do(req)
		if err != nil {
			log.Error("error sending redirect request: ", err)
			wg.Done()
		}
		goto Redirect
	case "200 OK":
		defer resp.Body.Close()
		log.Info("successfully established persistent connection with the master")
		reader := bufio.NewReader(resp.Body)
		m := &jsonpb.Unmarshaler{AllowUnknownFields: false}
		for {
			//A message starts with length of the message + a newline char + actual message
			msgBytes, err := reader.ReadBytes('\n')
			if err != nil {
				log.Error("error reading 200 ok response body: ", err)
				wg.Done()
				//return
			}
			msgBytes = bytes.TrimSpace(msgBytes)
			//log.Println(msgBytes)

			msgLen, err := strconv.Atoi(string(msgBytes))
			//msgLen, err := strconv.ParseUint(string(msgBytes), 10, 64)
			if err != nil {
				//log.Print(string(msgBytes))
				log.Error("error converting length to int:", err)
				wg.Done()
				//return
			}
			//log.Printf("%v:\n", msgLen)

			msg, err := reader.Peek(msgLen)
			if err != nil {
				log.Error("error peeking: ", err)
				wg.Done()
				//return
			}
			log.Info(string(msg))
			r := bytes.NewReader([]byte(msg))
			if err = m.Unmarshal(r, ev); err != nil {
				log.Error("unmarshal error", err)
				wg.Done()
				//return
			}
			//log.Println(ev.GetType())
			reader.Reset(resp.Body)
		}

	//log.Println(resp.Header.Get("Content-Type"))
	//log.Println(resp.Header.Get("Transfer-Encoding"))
	//log.Println(resp.Header.Get("Mesos-Stream-Id"))
	//log.Println(req.Proto)
	//log.Println(resp.Header.Get("Connection"))
	//log.Println(resp.Header.Get("Transfer-Encoding"))
	//}
	default:
		log.Error("failed to establish persistent connection with master:", resp.Status)
		wg.Done()
		//return
	}
}
