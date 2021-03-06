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
	//获取请求参数
	r.ParseForm()

	//返回参数初始化
	resp := Response{}
	resp.Errno = 0
	resp.Errmsg = "success"

	Logid := r.Form.Get("logid")
	Cmd := r.Form.Get("cmd")
	Hashkey := r.Form.Get("hash_key")
	Data := r.Form.Get("data")

	if Logid == "" {
		Logid = "111" //alpaca.GetLogId()
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
