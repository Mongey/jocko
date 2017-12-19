package commitlog_test

import (
	"reflect"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/travisjeffery/jocko/commitlog"
)

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestCommitLog_ImplementsLogStore(t *testing.T) {
	var store interface{} = &commitlog.CommitLog{}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatalf("CommitLog does not implement raft.LogStore")
	}
}

func TestCommitLogRead(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

	if err != nil {
		t.Fatalf("err: %s", err)
	}
	// Create the log
	log := testRaftLog(1, "howya now")

	if err := l.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	result := new(raft.Log)

	if err := l.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Errorf("expected %v, bad: %v", log, result)
	}
}

func TestCommitLog_ReadMulti(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

	log := new(raft.Log)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}

	if err := l.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Should return the proper log
	if err := l.GetLog(2, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, logs[1]) {
		t.Fatalf("bad: %#v -> %#v", log, logs[1])
	}
}

func TestCommitLog_FirstIndex(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

	if err != nil {
		t.Fatalf("err: %s", err)
	}

	idx, err := l.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	// Should get 0 index on empty log
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := l.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = l.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestCommitLog_LastIndex(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

	// Should get 0 index on empty log
	idx, err := l.LastIndex()
	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := l.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = l.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestCommitLog_DeleteRange(t *testing.T) {
	var err error
	l := setup(t)
	defer cleanup(t)

	// Create a set of logs
	log1 := testRaftLog(1, "log1")
	log2 := testRaftLog(2, "log2")
	log3 := testRaftLog(3, "log3")
	logs := []*raft.Log{log1, log2, log3}

	// Attempt to store the logs
	if err = l.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := l.DeleteRange(1, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := l.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1")
	}
	if err := l.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
}
