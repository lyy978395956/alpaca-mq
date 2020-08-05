/*
* @Author: leiyuya
* @Date:   2020-08-03 10:15:36
* @Last Modified by:   leiyuya
* @Last Modified time: 2020-08-04 16:38:40
 */
package alpaca

import (
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"
)

type TimeWheel struct {
	ticker   *time.Ticker
	curtime  int64
	tdura    time.Duration
	cae      *Cache
	buckets  []bucket
	bnum     int
	curPos   int
	run      bool
	stopChan chan bool
}

type bucket struct {
	sKey string
	mu   sync.Mutex
}

type Task struct {
	Expire int64
	Nturns int
	Val    string
}

func NewTimeWheel(bucketNum int, cae *Cache, skey string) (*TimeWheel, error) {

	if bucketNum <= 0 {
		return nil, errors.New("Illegal bucketNum")
	}

	tw := &TimeWheel{
		tdura:    time.Second,
		cae:      cae,
		curPos:   0,
		bnum:     bucketNum,
		buckets:  make([]bucket, bucketNum),
		curtime:  time.Now().Unix(),
		stopChan: make(chan bool),
	}

	for i := 0; i < bucketNum; i++ {
		tw.buckets[i] = bucket{sKey: skey + "_" + strconv.Itoa(i)}
	}
	return tw, nil
}

func (t *TimeWheel) add(pos int, value *Task) error {
	t.buckets[pos].mu.Lock()
	defer t.buckets[pos].mu.Unlock()

	val, err := json.Marshal(value)

	if err != nil {
		return err
	}

	return t.cae.SAdd(t.buckets[pos].sKey, string(val))
}

func (t *TimeWheel) del(pos int, value *Task) error {
	t.buckets[pos].mu.Lock()
	defer t.buckets[pos].mu.Unlock()

	val, err := json.Marshal(value)

	if err != nil {
		return err
	}

	return t.cae.Srem(t.buckets[pos].sKey, string(val))
}

func (t *TimeWheel) list(pos int) ([]*Task, error) {
	t.buckets[pos].mu.Lock()
	defer t.buckets[pos].mu.Unlock()

	list, err := t.cae.SGet(t.buckets[pos].sKey)

	if err != nil {
		return []*Task{}, err
	}

	if len(list) == 0 {
		return []*Task{}, nil
	}

	var lt []*Task

	for _, v := range list {

		obj := &Task{}

		err := json.Unmarshal(v.([]byte), obj)

		if err != nil {
			return []*Task{}, err
		}

		lt = append(lt, obj)
	}

	return lt, nil
}

func (t *TimeWheel) GetTaskList(pos int) ([]*Task, error) {
	return t.list(pos)
}

func (t *TimeWheel) DelTask(pos int, tk *Task) error {
	return t.del(pos, tk)
}

func (t *TimeWheel) AddTask(value string, delay time.Duration) error {

	if delay <= 0 {
		return errors.New("Illegal time")
	}

	if delay < t.tdura {
		delay = t.tdura
	}

	expire, pos, turns := t.getPnturns(delay)

	tk := &Task{}
	tk.Nturns = turns
	tk.Val = value
	tk.Expire = expire
	return t.add(pos, tk)
}

func (t *TimeWheel) getPnturns(delay time.Duration) (int64, int, int) {

	diff := int(delay / t.tdura)

	turns := int(diff / t.bnum)

	pos := (t.curPos + diff) % t.bnum

	if turns > 0 && pos == t.curPos {
		turns--
	}

	expire := int64(diff) + t.curtime

	return expire, pos, turns
}

func (t *TimeWheel) Start() <-chan time.Time {
	t.ticker = time.NewTicker(t.tdura)
	t.run = true
	return t.ticker.C
}
func (t *TimeWheel) Stop() {
	t.stopChan <- true
	t.run = false
}
func (t *TimeWheel) CurPos() int {
	return t.curPos
}

func (t *TimeWheel) InrcPos() {
	t.curPos = (t.curPos + 1) % t.bnum
}
