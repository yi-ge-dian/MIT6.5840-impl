package kvraft

import (
	"log"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

const ClientRequestTimeout = 500 * time.Microsecond

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key    string
	Value  string
	OpType OperationType
}

type OpReply struct {
	Value string
	Err   Err
}

type OperationType uint8

const (
	OpGet OperationType = iota
	OpPut
	OpAppend
)

func getOperationType(op string) OperationType {
	switch op {
	case "Get":
		return OpGet
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	default:
		panic("unknown operation type")
	}
}
