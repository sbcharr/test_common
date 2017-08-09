package main

import (
	"sync"
	"flag"

	mesosproto "github.com/mesos/test_common/api/v1"
	sch "github.com/mesos/test_common/api/v1/scheduler"
	"github.com/golang/protobuf/proto"
	//	"github.com/gogo/protobuf/jsonpb"
	"github.com/mesos/test_common/api/v1/calls"
	log "github.com/golang/glog"
)



func init() {
	flag.Parse()
}

func HandleEvent(e *sch.Event) error {
	switch e.GetType().String(){
	case "SUBSCRIBED":

	case "OFFERS":

	case "INVERSE_OFFERS":

	case "RESCIND":

	case "RESCIND_INVERSE_OFFER":

	case "UPDATE":

	case "MESSAGE":

	case "FAILURE":

	case "ERROR":

	case "HEARTBEAT":

	default:

	}
	return nil
}


func main() {
	var wg sync.WaitGroup
	frw := &mesosproto.FrameworkInfo{User: proto.String("foo"), Name: proto.String("An example framework in golang")}
	zkList := "172.28.128.9:2181,172.28.128.8:2181"
	wg.Add(1)
	go calls.Subscribe(frw, zkList, wg)
	wg.Wait()
	log.Info("Program exited")
	log.Flush()
}




