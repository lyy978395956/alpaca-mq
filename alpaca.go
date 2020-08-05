/*
* @Author: leiyuya
* @Date:   2020-07-31 14:02:16
* @Last Modified by:   leiyuya
* @Last Modified time: 2020-08-05 09:54:38
 */
package alpaca

import (
	"time"
)

var (
	Alogger *Logger
)

func InitLog(logpath string) {

	Alogger = NewLogger(logpath)

	Alogger.Init(time.Hour)
}
func InitPuller(cfgfile string, apdir string) *Puller {

	Agpullcfg := InitGPullerCfg(cfgfile)

	return NewPuller(Alogger, Agpullcfg, InitAppCfg(apdir, Agpullcfg.Alist))
}

func InitPusher(cfgfile string) *Pusher {

	return NewPusher(Alogger, InitGPusherCfg(cfgfile))
}
