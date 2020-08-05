/*
* @Author: leiyuya
* @Date:   2020-07-14 19:40:21
* @Last Modified by:   leiyuya
* @Last Modified time: 2020-08-05 10:21:44
 */
package alpaca

import (
	"encoding/json"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/juju/ratelimit"
)

type Pusher struct {
	topic  string
	pue    *Producer
	logger *Logger
	cds    map[string]GPusherCmd
	bt     *ratelimit.Bucket
}

func NewPusher(lg *Logger, pcfg *GPusherConfig) *Pusher {

	pue, err := InitProducer(NewPusherConfig(pcfg))

	if err != nil {
		lg.Fatalf("Init Async Proudcer failed err:%s", err)
	}

	if len(pcfg.Cmds) == 0 {
		lg.Fatal("No Commands")
	}

	cds := make(map[string]GPusherCmd)

	for _, v := range pcfg.Cmds {
		cds[v.Cmd] = v
	}

	return &Pusher{
		logger: lg,
		pue:    pue,
		topic:  pcfg.Topic,
		cds:    cds,
		bt:     ratelimit.NewBucketWithRate(1, pcfg.RateLimit),
	}
}

func (p *Pusher) pack(message *Kmessage) (*sarama.ProducerMessage, error) {

	msgJson, err := json.Marshal(message)

	if err != nil {
		p.logger.Warnf("Encode Message failed err:%s", err)
		return nil, err
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
	}
	if message.HashKey != "" {
		msg.Key = sarama.ByteEncoder(message.HashKey)
	}

	msg.Value = sarama.ByteEncoder(string(msgJson))

	return msg, nil
}

func (p *Pusher) ccmd(c string) bool {

	cmd, ok := p.cds[c]

	if !ok {
		return false
	}

	if !cmd.Status {
		return false
	}

	return true
}

func (p *Pusher) Push(message *Kmessage) error {

	tk := p.bt.TakeAvailable(1)

	if tk == 0 {
		return errors.New("Request rate is too fast")
	}

	cres := p.ccmd(message.Cmd)

	if !cres {
		return errors.New("Unidentified Cmd")
	}

	msg, err := p.pack(message)

	if err != nil {
		return err
	}

	p.pue.Send() <- msg

	for {
		select {
		case suc := <-p.pue.Successes():
			bytes, err := suc.Value.Encode()
			if err != nil {
				return err
			}
			p.logger.WithFields(Fields{"offset": suc.Offset, "partition": suc.Partition, "data": string(bytes), "LogId": message.LogId}).Info("Send Message")
			return nil
		case fail := <-p.pue.Errors():
			return fail.Err
		}
	}
}
