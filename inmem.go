package yue

import (
	"log"
	"reflect"

	proto "github.com/umegaya/yue/proto"
)


var	inputsBuf []reflect.Value = make([]reflect.Value, 16)
var emptyInputs []reflect.Value = make([]reflect.Value, 0)
func callmethod(m *methodEntry, args ...interface{}) []reflect.Value {
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
		//log.Printf("inp: %v %v %v %v", i, m.inparams[i], a, v.Elem().Kind())
		if a == nil {
			inputs[i] = reflect.New(m.inparams[i]).Elem()
		} else {
			inputs[i] = reflect.ValueOf(a).Convert(m.inparams[i])
		}
	}
	return m.f.Call(inputs)
}

func callmethod2(m *methodEntry, args ...interface{}) (interface{}, error) {
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

func callctor(m *methodEntry, args ...interface{}) (interface{}, error) {
	//log.Printf("callctor: %v %v", args...)
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

func entryby(f reflect.Value) *methodEntry {
	t := f.Type()
	m := &methodEntry { f: f }
	if t.NumOut() > 0 {
		//log.Printf("hasError: %v %v", t.Out(t.NumOut() - 1), errorType)
		m.hasError = (t.Out(t.NumOut() - 1) == errorType)
	} else {
		m.hasError = false
	}
	m.inparams = make([]reflect.Type, t.NumIn())
	for i := 0; i < t.NumIn(); i++ {
		m.inparams[i] = t.In(i)
	}
	return m
}

func funcarg(v reflect.Value, a interface{}) {
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(a.(bool))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(a.(int64))
    case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(a.(uint64))
    case reflect.Float32, reflect.Float64:
		v.SetFloat(a.(float64))
    case reflect.Complex64, reflect.Complex128:
    	v.SetComplex(a.(complex128))
    case reflect.Array, reflect.Interface, reflect.Map, reflect.Slice, reflect.String, reflect.Struct:
    	v.Set(reflect.ValueOf(a))
    case reflect.Uintptr, reflect.Chan, reflect.Func, reflect.Ptr, reflect.UnsafePointer:
    	log.Panicf("not supported arg type: %v", v.Kind())
    }
}


//
// inmemoryExecuter
//
//Executer interface implemented by go. it lived in same process as which runs yue
type methodEntry struct {
	f reflect.Value
	inparams []reflect.Type
	hasError bool
}
func (me *methodEntry) call(args ...interface{}) (interface{}, error) {
	if me.hasError {
		return callmethod2(me, args...)
	} else {
		return callmethod(me, args...), nil
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
	return entryby(f)
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
	ctor *methodEntry
}

func (im InmemoryExecuterFactory) Create(pid uint64, args ...interface{}) (Executer, error) {
	if im.ctor == nil {
		im.ctor = entryby(reflect.ValueOf(im.Constructor))
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

