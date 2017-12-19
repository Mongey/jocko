package commitlog

import (
	"io/ioutil"

	"github.com/hashicorp/raft"
)

func (l *CommitLog) FirstIndex() (uint64, error) {
	if l.isEmpty() {
		return 0, nil
	}
	return uint64(l.OldestOffset() + 1), nil
}

func (l *CommitLog) LastIndex() (uint64, error) {
	return uint64(l.NewestOffset()), nil
}

func (l *CommitLog) GetLog(index uint64, log *raft.Log) error {
	i := int64(index - 1)
	r, err := l.NewReader(i, 512)

	if err != nil {
		return err
	}
	p, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	log.Data = p
	log.Index = index

	return err
}

func (l *CommitLog) StoreLog(log *raft.Log) error {
	_, err := l.Append(log.Data)
	return err
}

func (l *CommitLog) StoreLogs(logs []*raft.Log) error {
	var err error

	for _, log := range logs {
		_, err = l.Append(log.Data)
	}
	return err
}

func (l *CommitLog) DeleteRange(min uint64, max uint64) error {
	return l.Truncate(int64(max - 1))
}
