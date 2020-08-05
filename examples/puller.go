/*
* @Author: leiyuya
* @Date:   2020-07-14 16:32:14
* @Last Modified by:   leiyuya
* @Last Modified time: 2020-08-05 09:55:22
 */
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
