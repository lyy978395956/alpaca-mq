/*
* @Author: leiyuya
* @Date:   2020-07-14 15:56:46
* @Last Modified by:   leiyuya
* @Last Modified time: 2020-08-05 10:15:41
 */
package alpaca

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/Shopify/sarama"
	sarama_cluster "github.com/bsm/sarama-cluster"
	yaml "gopkg.in/yaml.v2"
)

type App struct {
	Protocol   string   `yaml:"protocol"`
	Path       string   `yaml:path`
	Name       string   `yaml:"name"`
	ServerType string   `yaml:"server_type"`
	Cmd        string   `yaml:"cmd"`
	Servers    []string `yaml:"servers"`
}

type GPusherCmd struct {
	Cmd    string `yaml:"cmd"`
	Status bool   `yaml:"st"`
}
type GPusherConfig struct {
	Kafka     []string     `yaml:"kafka"`
	Topic     string       `yaml:"topic"`
	RateLimit int64        `yaml:"rate_limit"`
	Cmds      []GPusherCmd `yaml:"cmds"`
}

type GPullerConfig struct {
	Kafka         []string `yaml:"kafka"`
	Zookeeper     []string `yaml:"zookeeper"`
	Redis         string   `yaml:"redis"`
	ZkRetryTimes  int32    `yaml:"zk_retry_times"`
	MsgMaxRetry   int32    `yaml:"msg_max_retry"`
	OffsetCtTime  int32    `yaml:"offset_ct_time"`
	MsgDelay      int32    `yaml:"msg_delay"`
	TimeWheelSize int      `yaml:"time_wheel_size"`
	Wnd           int32    `yaml:"wnd"`
	GroupName     string   `yaml:"group_name"`
	Topic         string   `yaml:"topic"`
	Cmode         int8     `yaml:"cmode"`
	Gpath         string   `yaml:"gpath"`
	Alist         []string `yaml:"services"`
}

func InitGPusherCfg(cfgFile string) *GPusherConfig {

	gcfg := &GPusherConfig{}

	checkConfigFile(cfgFile)

	fileContent, err := ioutil.ReadFile(cfgFile)

	if err != nil {
		Alogger.Fatalf("Read Config File Failed Error:%s", err)
	}

	var errs error

	errs = yaml.Unmarshal(fileContent, gcfg)

	if errs != nil {
		Alogger.Fatalf("Parse Config File Failed Error:%s", errs)
	}

	return gcfg
}
func InitGPullerCfg(cfgFile string) *GPullerConfig {

	gcfg := &GPullerConfig{}

	checkConfigFile(cfgFile)

	fc, err := ioutil.ReadFile(cfgFile)

	if err != nil {
		Alogger.Fatalf("Read Config File Failed Error:%s", err)
	}

	var errs error

	errs = yaml.Unmarshal(fc, gcfg)

	if errs != nil {
		Alogger.Fatalf("Parse Config File Failed Error:%s", errs)
	}
	return gcfg
}
func InitAppCfg(apdir string, list []string) map[string]App {

	if len(list) == 0 {
		Alogger.Fatal("No App Configuration Available")
	}

	var errs error

	aps := make(map[string]App)

	for _, apfile := range list {

		apfPath := apdir + apfile

		s := App{}

		checkConfigFile(apfPath)

		fc, err := ioutil.ReadFile(apfPath)

		if err != nil {
			Alogger.WithFields(Fields{"app_file": apfile}).Fatalf("Init App Failed Err:", err)
		}

		errs = yaml.Unmarshal(fc, &s)

		if errs != nil {
			Alogger.WithFields(Fields{"app_file": apfile}).Fatalf("Init App Failed Err:", errs)
		}

		aps[s.Cmd] = s
	}

	return aps
}

type PushConfig struct {
	Servers []string
	Sconf   *sarama.Config
}

func NewPusherConfig(cf *GPusherConfig) *PushConfig {
	conf := &PushConfig{}
	conf.Sconf = sarama.NewConfig()

	conf.Sconf.Producer.Return.Successes = true
	conf.Sconf.Producer.Return.Errors = true

	conf.Sconf.Producer.RequiredAcks = sarama.WaitForAll

	conf.Sconf.Version = sarama.V2_5_0_0

	conf.Sconf.Producer.Retry.Max = 10

	conf.Servers = cf.Kafka
	return conf
}

type PullConfig struct {
	servers []string
	sarama_cluster.Config
}

func NewPullerConfig(gcf *GPullerConfig) *PullConfig {

	conf := &PullConfig{}
	conf.Config = *sarama_cluster.NewConfig()

	conf.Config.Config.Consumer.Return.Errors = true
	conf.Config.Group.Return.Notifications = true

	conf.Config.Config.Consumer.Offsets.CommitInterval = time.Duration(gcf.OffsetCtTime) * time.Second

	if gcf.Cmode == 1 {
		conf.Config.Config.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		conf.Config.Config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	conf.servers = gcf.Kafka

	return conf
}

func checkConfigFile(configPath string) {
	if configPath == "" {
		Alogger.Fatalf("Path:%s Err:The path is not set ", configPath)
	}
	_, err := os.Stat(configPath)

	if err != nil {
		Alogger.Fatalf("Path:%s Err:Conf load failed", configPath)
	}
	if os.IsNotExist(err) {
		Alogger.Fatalf("Path:%s Err:Path not exist", configPath)
	}
}
