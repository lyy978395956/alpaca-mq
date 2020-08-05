/*
* @Author: leiyuya
* @Date:   2020-07-17 18:52:57
* @Last Modified by:   leiyuya
* @Last Modified time: 2020-08-05 10:20:35
 */
package alpaca

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

type Puller struct {
	logger       *Logger
	zk           *Zk
	ce           *Cache
	apps         map[string]App
	pom          *Consumer
	topic        string
	gname        string
	lock         sync.Mutex
	msgMaxRetry  int32
	zkRetryTimes int32
	wnd          chan bool
	mdelay       time.Duration
	tws          map[int32]*TimeWheel
	twsize       int
}

func NewPuller(lg *Logger, cg *GPullerConfig, aplist map[string]App) *Puller {

	zk, err := NewZk(cg.Zookeeper, 5*time.Second)

	if err != nil {
		lg.Fatalf("Init Zookeeper Server failed, err:%s", err)
	}

	pom, err := InitConsumer(cg.Topic, cg.GroupName, NewPullerConfig(cg))

	if err != nil {
		lg.Fatalf("Init Consumer Failed Err:%s", err)
	}

	return &Puller{
		topic:        cg.Topic,
		gname:        cg.GroupName,
		logger:       lg,
		zk:           zk,
		apps:         aplist,
		pom:          pom,
		ce:           InitPool(cg.Redis),
		msgMaxRetry:  cg.MsgMaxRetry,
		zkRetryTimes: cg.ZkRetryTimes,
		wnd:          make(chan bool, cg.Wnd),
		mdelay:       time.Duration(cg.MsgDelay) * time.Second,
		twsize:       cg.TimeWheelSize,
		tws:          make(map[int32]*TimeWheel),
	}
}

func (p *Puller) handleError() {

	for err := range p.pom.Errors() {
		p.logger.Warnf("Consumer Error:%s", err)
	}
}

func (p *Puller) handleRebalanceNotify() {

	for ntf := range p.pom.Notifications() {

		p.logger.Infof("Rebalanced: %+v\n", ntf)

		if p.mdelay <= 0 {
			return
		}

		tp := p.pom.Subscriptions()

		p.resettw()

		p.inittw(tp)
	}
}
func (p *Puller) resettw() {

	if len(p.tws) != 0 {

		for k, v := range p.tws {

			if v.run {
				v.Stop()
			}

			delete(p.tws, k)
		}
	}

}

func (p *Puller) inittw(tp map[string][]int32) {

	if len(tp) != 0 {

		pls, ok := tp[p.topic]

		if !ok {
			return
		}

		for _, v := range pls {
			skey := p.topic + "_" + strconv.Itoa(int(v))
			tw, _ := NewTimeWheel(p.twsize, p.ce, skey)
			p.tws[v] = tw
			go p.twLoop(tw)
		}

	}
}
func (p *Puller) twLoop(tml *TimeWheel) {

	for {
		select {
		case <-tml.Start():

			curPos := tml.CurPos()

			curTime := time.Now().Unix()

			dtlist, _ := tml.GetTaskList(curPos)

			if len(dtlist) > 0 {
				for _, v := range dtlist {
					if v.Nturns > 0 {
						if v.Expire <= curTime {
							go p.pDly(tml, v, curPos)
						}
						continue
					}
					go p.pDly(tml, v, curPos)
				}
			}

			tml.InrcPos()

		case <-tml.stopChan:
			return
		}
	}
}
func (p *Puller) Pull() {

	go p.handleError()

	go p.handleRebalanceNotify()

	for {

		msg := <-p.pom.Recv()

		p.wnd <- true

		p.logger.WithFields(Fields{"message": string(msg.Value)}).Info("Receive Message")

		almsg := &AlpaceMsg{}

		almsg.oft = msg.Offset
		almsg.part = msg.Partition

		kmsg := &Kmessage{}

		err := json.Unmarshal(msg.Value, kmsg)

		if err != nil {
			p.logger.WithFields(Fields{"message": string(msg.Value)}).Warnf("Json Unmarchar Error:%s", err)
		}

		almsg.kmsg = kmsg

		if p.mdelay > 0 {
			go p.sDly(almsg)
			continue
		}

		go p.proc(almsg)
	}
}

func (p *Puller) cmtOft(partition int32, offset int64) error {

	p.lock.Lock()

	defer p.lock.Unlock()

	resKey := p.topic + ":" + strconv.Itoa(int(partition))

	var of int64

	v, err := p.ce.GetInt64(resKey)

	if err != nil {

		if err.Error() != ErrNil.Error() {
			return errors.New("Redis Get Offset Failed")
		}
		err := p.ce.SetInt64(resKey, offset)

		if err != nil {
			return errors.New("Redis Init Offset Failed")
		}
		of = offset - 1
	} else {
		of = v
	}

	if offset < of {
		return nil
	}

	if of+1 != offset {
		return errors.New("Commit Offset Failed")
	}

	cerr := p.ce.SetInt64(resKey, offset)

	if cerr != nil {
		return errors.New("Redis Set Offset Failed")
	}

	p.pom.MarkOffset(p.topic, partition, offset+1, p.gname)

	return nil
}

func (p *Puller) findOft() {

}
func (p *Puller) proc(almsg *AlpaceMsg) {

	<-p.wnd

	go p.callap(almsg)

}
func (p *Puller) callap(almsg *AlpaceMsg) {

	var retry int32 = 0

	for {

		retry = retry + 1

		if retry > p.msgMaxRetry {
			go p.smsg(almsg)
			break
		}

		if retry > 0 {
			time.Sleep(1 * time.Second)
		}

		err := p.hmsg(almsg.kmsg, retry)

		if err != nil {
			p.logger.Warnf("HanleMessag Failed err:%s", err)
			continue
		}

		cerr := p.cmtOft(almsg.part, almsg.oft)

		if cerr == nil {
			break
		}

		p.logger.Warnf("CommitOffset Failed err:%s", err)
	}

}

func (p *Puller) pDly(tw *TimeWheel, tk *Task, pos int) {

	almsg := &AlpaceMsg{}

	err := json.Unmarshal([]byte(tk.Val), almsg)

	if err != nil {
		p.logger.WithFields(Fields{"message": tk.Val}).Fatalf("Json Unmarchar Error:%s", err)
	}

	go p.callap(almsg)

	tw.DelTask(pos, tk)

}

func (p *Puller) sDly(msg *AlpaceMsg) {

	<-p.wnd

	tw, ok := p.tws[msg.part]

	if !ok {
		go p.callap(msg)
	}

	bmsg, err := json.Marshal(msg)

	if err != nil {
		p.logger.WithFields(Fields{"message": string(bmsg)}).Fatalf("Json marchar Error:%s", err)
	}

	var mdly time.Duration

	if msg.kmsg.Delay > 0 {
		mdly = time.Duration(msg.kmsg.Delay) * time.Second
	} else {
		mdly = p.mdelay
	}

	errk := tw.AddTask(string(bmsg), mdly)

	if errk != nil {
		go p.callap(msg)
	}
}

func (p *Puller) gAurl(cmd string) (string, error) {

	app, ok := p.apps[cmd]

	if !ok {
		return "", errors.New("Not Cmd Exists")
	}

	host := app.Servers[rand.Intn(len(app.Servers)-1)]

	var url bytes.Buffer

	url.WriteString(app.Protocol)
	url.WriteString(":")
	url.WriteString("//")
	url.WriteString(host)
	url.WriteString(app.Path)

	return url.String(), nil
}

func (p *Puller) hmsg(Kmsg *Kmessage, retry int32) error {

	url, err := p.gAurl(Kmsg.Cmd)

	if err != nil {
		return err
	}

	p.logger.WithFields(Fields{"logId": Kmsg.LogId, "url": url, "retry": retry}).Info("Request info")

	httpRequest := NewHttpRequest(p.logger)

	rer := httpRequest.Post(url, Kmsg.Data, Kmsg.LogId)

	return rer
}

func (p *Puller) smsg(message *AlpaceMsg) {

	bte, err := json.Marshal(message)

	if err != nil {
		p.logger.WithFields(Fields{"logId": message.kmsg.LogId}).Warnf("Json Marshal Message Failed Error:%s", err)
	}

	acls := p.zk.WorldACL()

	topicNodePath := "/" + p.topic

	nodePath := topicNodePath + "/" + message.kmsg.LogId

	for i := 0; i < int(p.zkRetryTimes); i++ {

		ex, err := p.zk.Exists(topicNodePath)

		if err != nil {
			p.logger.WithFields(Fields{"topic": p.topic, "retry": i, "topic_path": topicNodePath}).Warnf("[Check Topic Node Exist]Zookeeper Server Error Err:%s", err)
			continue
		}

		if !ex {
			err := p.zk.Create(topicNodePath, []byte{}, 0, acls)
			if err != nil {
				p.logger.WithFields(Fields{"topic": p.topic, "retry": i, "topic_path": topicNodePath}).Warnf("[Create Topic Node]Zookeeper Server Error Err:%s", err)
				continue
			}
		}

		errCreateNode := p.zk.Create(nodePath, bte, 0, acls)

		if errCreateNode != nil {

			if errCreateNode.Error() == ZkErrExists {
				return
			}

			p.logger.WithFields(Fields{"topic": p.topic, "retry": i, "topic_path": topicNodePath, "node_path": nodePath}).Warnf("[Create Topic Node Child]Zookeeper Server Error Err:%s", err)
			continue
		}

		return
	}
}
