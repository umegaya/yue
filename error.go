package yue

import (
	"fmt"

	proto "yue/proto"
)

const (
	Invalid = iota
	ActorNotFound
	ActorNoSuchMethod
	ActorAlreadyHasProcess
	ActorProcessGone
	ActorRuntimeError
	ActorTimeout
	ActorGoCtxError
	ActorInvalidPayload
	ActorInvalidProcess
)


var errorStrings []string = []string{
	"invalid",
	"actor: not found: %v",
	"actor: no such method: %v %v",
	"actor: has enough process: %v %v",
	"process: not exist: %v",
	"actor: call timeout: %v",
	"actor: go context error: %v",
	"invalid payload type: %v",
	"invalid process: %v",
}

type ActorError struct {
	Type uint8
	Args []interface{}
}

func (e *ActorError) Error() string {
	return fmt.Sprintf(errorStrings[e.Type], e.Args...)
}

func (e *ActorError) Is(typ int) bool {
	return e.Type == uint8(typ)
}

func (e *ActorError) GoneProcessId() proto.ProcessId {
	if !e.Is(ActorProcessGone) {
		return proto.ProcessId(0)
	}
	return proto.ProcessId(e.Args[0].(uint64))
}

func newerr(typ int, args ...interface{}) *ActorError {
	return &ActorError {
		Type: uint8(typ),
		Args: args,
	}
}
