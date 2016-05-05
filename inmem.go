package yue

import (
	"log"
	"reflect"

	proto "yue/proto"
)


var	inputsBuf []reflect.Value = make([]reflect.Value, 16)
var emptyInputs []reflect.Value = make([]reflect.Value, 0)
func callmethod(m reflect.Value, args ...interface{}) []reflect.Value {
	var inputs []reflect.Value;
	if len(args) > 0 {
		if len(args) > len(inputsBuf) {
			l := len(inputsBuf)
			for {
				if l < len(args) {
					l = l * 2
				} else {
					break
				}
			}
			inputsBuf = make([]reflect.Value, 0, l)
		}
		inputs = inputsBuf[0:len(args)]
	} else {
		inputs = emptyInputs
	}
	for i, a := range args {
		inputs[i] = reflect.ValueOf(a)
	}
	return m.Call(inputs)
}

func callmethod2(m reflect.Value, args ...interface{}) (interface{}, error) {
	rv := callmethod(m, args...)
	nrv := len(rv)
	if nrv > 0 {
		if err, ok := rv[nrv - 1].Interface().(error); ok {
			return nil, err
		} else if nrv <= 2 {
			return rv[0], nil
		} else {
			return rv[0:nrv-1], nil
		}
	} else {
		return nil, nil
	}
}

func callctor(m reflect.Value, args ...interface{}) (interface{}, error) {
	rv := callmethod(m, args...)
	nrv := len(rv)
	if nrv > 0 {
		if err, ok := rv[nrv - 1].Interface().(error); ok {
			return nil, err
		}
		return rv[0].Interface(), nil
	} else {
		return nil, newerr(ActorRuntimeError, "ctor should return instance")
	}
}

//
// inmemoryExecuter
//
//Executer interface implemented by go. it lived in same process as which runs yue
type methodEntry struct {
	f reflect.Value
	hasError bool
}
func (me methodEntry) call(args ...interface{}) (interface{}, error) {
	if me.hasError {
		return callmethod2(me.f, args...)
	} else {
		return callmethod(me.f, args...), nil
	}
}

type inmemoryExecuter struct {
	pid proto.ProcessId
	instance reflect.Value
	methods map[string]*methodEntry
}

var errorType = reflect.TypeOf(new(error)).Elem()
func (im inmemoryExecuter) entryof(name string) *methodEntry {
	f := im.instance.MethodByName(name)
	t := f.Type()
	m := &methodEntry { f: f }
	if t.NumOut() > 0 {
		log.Printf("hasError: %v %v", t.Out(t.NumOut() - 1), errorType)
		m.hasError = (t.Out(t.NumOut() - 1) == errorType)
	} else {
		m.hasError = false
	}
	return m
}

func (im inmemoryExecuter) Call(method string, args ...interface{}) (interface {}, error) {
	if e, ok := im.methods[method]; ok {
		return e.call(args...)
	} else {
		e := im.entryof(method)
		im.methods[method] = e
		return e.call(args...)
	}
	return nil, newerr(ActorNoSuchMethod, im.pid, method)
}


//
// InmemoryExecuterFactory
//
type InmemoryExecuterFactory struct {
	Constructor interface{}
	ctor reflect.Value
}

func (im InmemoryExecuterFactory) Create(pid uint64, args ...interface{}) (Executer, error) {
	if !im.ctor.IsValid() {
		im.ctor = reflect.ValueOf(im.Constructor)
	}
	p, err := callctor(im.ctor, args...)
	if err != nil {
		return nil, err
	}
	v := reflect.ValueOf(p)
	if v.Kind() != reflect.Ptr {
		return nil, newerr(ActorInvalidProcess, v.Kind())
	}
	return &inmemoryExecuter {
		pid: proto.ProcessId(pid),
		instance: v,
		methods: make(map[string]*methodEntry),
	}, nil
}

func (im InmemoryExecuterFactory) Destroy(p Executer) {
	if p, ok := p.(inmemoryExecuter); ok {
		p.Call("destroy")
	} else {
		log.Fatalf("invalid executer passed: %v", p)
	}

}

