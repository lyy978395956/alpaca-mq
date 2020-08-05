/*
* @Author: leiyuya
* @Date:   2020-07-15 11:28:01
* @Last Modified by:   leiyuya
* @Last Modified time: 2020-08-04 16:06:20
 */
package alpaca

type Kmessage struct {
	Cmd     string
	LogId   string
	HashKey string
	Data    string
	Delay   int
}

type AlpaceMsg struct {
	kmsg *Kmessage
	part int32
	oft  int64
}
