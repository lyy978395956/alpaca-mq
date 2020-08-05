/*
* @Author: leiyuya
* @Date:   2020-07-14 16:30:48
* @Last Modified by:   leiyuya
* @Last Modified time: 2020-07-31 15:34:34
 */
package alpaca

import (
	"github.com/Shopify/sarama"
	sarama_cluster "github.com/bsm/sarama-cluster"
)

type Consumer struct {
	consumer *sarama_cluster.Consumer
}

func InitConsumer(topic string, gname string, conf *PullConfig) (*Consumer, error) {
	c, err := sarama_cluster.NewConsumer(conf.servers, gname, []string{topic}, &conf.Config)
	var cs = &Consumer{
		consumer: c,
	}
	if err != nil {
		return nil, err
	}
	return cs, nil
}

func (cs *Consumer) Close() error {
	return cs.consumer.Close()
}

func (cs *Consumer) Recv() <-chan *sarama.ConsumerMessage {
	return cs.consumer.Messages()
}

func (cs *Consumer) Notifications() <-chan *sarama_cluster.Notification {
	return cs.consumer.Notifications()
}

func (cs *Consumer) Errors() <-chan error {
	return cs.consumer.Errors()
}

func (cs *Consumer) MarkOffset(topic string, partition int32, offset int64, groupId string) {
	cs.consumer.MarkPartitionOffset(topic, partition, offset, "")
}

func (cs *Consumer) Subscriptions() map[string][]int32 {
	return cs.consumer.Subscriptions()
}
func (cs *Consumer) ResetOffset(topic string, partition int32, offset int64, groupId string) {
	cs.consumer.ResetPartitionOffset(topic, partition, offset, "")
}

func (cs *Consumer) CommitOffsets() error {
	err := cs.consumer.CommitOffsets()
	if err != nil {
		return err
	} else {
		return nil
	}
}

func Topics(conf *PullConfig) ([]string, error) {
	client, err := sarama.NewClient(conf.servers, &conf.Config.Config)
	if err != nil {
		return []string{}, err
	}
	defer client.Close()
	return client.Topics()
}

func Partitions(topic string, conf *PullConfig) ([]int32, error) {
	client, err := sarama.NewClient(conf.servers, &conf.Config.Config)
	if err != nil {
		return []int32{}, err
	}
	defer client.Close()
	return client.Partitions(topic)
}
