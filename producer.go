/*
* @Author: leiyuya
* @Date:   2020-07-14 14:57:12
* @Last Modified by:   leiyuya
* @Last Modified time: 2020-07-31 13:10:46
 */
package alpaca

import (
	"github.com/Shopify/sarama"
)

type Producer struct {
	Asp sarama.AsyncProducer
}

func InitProducer(conf *PushConfig) (*Producer, error) {

	aSyncProducer := &Producer{}

	var err error

	aSyncProducer.Asp, err = sarama.NewAsyncProducer(conf.Servers, conf.Sconf)

	return aSyncProducer, err
}

func (asp *Producer) Send() chan<- *sarama.ProducerMessage {
	return asp.Asp.Input()
}

func (asp *Producer) Successes() <-chan *sarama.ProducerMessage {
	return asp.Asp.Successes()
}

func (asp *Producer) Errors() <-chan *sarama.ProducerError {
	return asp.Asp.Errors()
}

func (asp *Producer) Close() (err error) {
	err = asp.Asp.Close()
	return err
}
