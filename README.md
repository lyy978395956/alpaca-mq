# Alpaca-mq

- High availability based on kafka
- Support flow control, overload protection
- Quickly complete the construction of multiple applications and multiple services
- Support message delay queue
- Point-to-point consumption based on command points

Message queue for service decoupling, You can use it to decouple services, but it is worth noting that you need to ensure that your services are idempotent.

# Installation

Install alpaca-mq using the "go get" command:

- go get github.com/SheepGardener/alpaca-mq

# Puller

The pusher of the message, produces and builds the message, and completes the push of the message,
Puller startup is very simple, you can start it quickly, of course, you need to configure puller first
```
package main

import (
	alpaca "github.com/SheepGardener/alpaca-mq"
)

func init() {
	alpaca.InitLog("./log/puller.log")
}

func main() {
	puller := alpaca.InitPuller("./config/puller.yml", "./config/apps/")
	puller.Pull()
}
```

# Pusher

The consumer of the message, passing the message to the service application,Here is only a simple example, in fact, you can customize a pusher service more flexibly

```

package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	alpaca "github.com/SheepGardener/alpaca-mq"
)

type Response struct {
	Errno  int8   `json:"errno"`
	Errmsg string `json:"errmsg"`
	Logid  string `json:"log_id"`
}

var Pusher *alpaca.Pusher

func init() {
	alpaca.InitLog("./log/pusher.log")
	Pusher = alpaca.InitPusher("./config/pusher.yml")
}

func sendMsg(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	resp := Response{}
	resp.Errno = 0
	resp.Errmsg = "success"

	Logid := r.Form.Get("logid")
	Cmd := r.Form.Get("cmd")
	Hashkey := r.Form.Get("hash_key")
	Data := r.Form.Get("data")

	if Logid == "" {
		Logid = "test" //alpaca.GetLogId()
	}

	if Cmd == "" {
		w.Write([]byte("{\"errno\":-1,\"errmsg\":\"Command cannot be empty\"}"))
		return
	}

	resp.Logid = Logid

	kmsg := &alpaca.Kmessage{
		Cmd:     Cmd,
		Data:    Data,
		LogId:   Logid,
		HashKey: Hashkey,
	}

	err := Pusher.Push(kmsg)

	if err != nil {
		resp.Errno = -1
		resp.Errmsg = fmt.Sprintf("%s", err)
	}

	respJson, err := json.Marshal(resp)

	if err != nil {
		w.Write([]byte("{\"errno\":-1,\"errmsg\":\"ResponData json marchal failed\"}"))
		return
	}
	w.Write(respJson)
}

func main() {

	http.HandleFunc("/sendmsg", sendMsg)
	http.ListenAndServe(":8009", nil)
}
```

# More

If you want to know more how to use it, you can refer to the [examples](https://github.com/SheepGardener/alpaca-mq/tree/master/examples), it can provide you with more help

- [Puller configuration](https://github.com/SheepGardener/alpaca-mq/blob/master/examples/config/puller.yml)
- [Pusher configuration](https://github.com/SheepGardener/alpaca-mq/blob/master/examples/config/pusher.yml)
- [Service application configuration](https://github.com/SheepGardener/alpaca-mq/blob/master/examples/config/apps/test-app.yml)