package yue

import (
	"log"
	"sync"
	"time"
	"regexp"

	proto "github.com/umegaya/yue/proto"
)


//InmemoryFactory represents factory definition using container as go struct.
type InmemoryFactory struct {
	Constructor func (args ...interface {}) (interface {}, error)
}

//represents common (internal) actor object
//yue's actor is, like orleans of MS, virtual one. actual message processing is delegate to Process.
type actor struct {
	id string 
	processes []proto.ProcessId
	index int
	limit int 
}

func newactor(id string, conf *ActorConfig) *actor {
	return &actor {
		id: id,
		processes: make([]proto.ProcessId, 0),
		index: 0,
		limit: conf.Size,
	}
}

func (a *actor) call(ctx *rpcContext, method string, args ...interface {}) error {
RETRY:
	if err := sv().call(ctx, a.balance(), method, args...); err != nil {
		/*
		呼び出し側のエラー処理
		TODO: もし、applicationレベルではないエラー(!ActorRuntimeError)が発生した場合、障害の可能性が高いためシステムレベルでの対応が必要になる.
		現在把握されているエラーは次の通り
		ActorProcessGone: 呼び出し先のノードに指定されたprocessがいなかったので処理できなかった。
		NodeConnectionTimeout: pidがいるはずのノードに接続できない。

		### stateless actorの場合 (SpawnOption.Size > 1)
		ActorProcessGone: 指定されたidのプロセスが存在していない
		以下のケースが考えられる
			- ノードが死亡して別の同じIPのノードが立ち上がった(もしprocessがpanicで再起動中だけなのであれば、リトライを呼び出された側で繰り返した後タイムアウトになるはず)
		対策
			- そのprocessidは消してしまった方がいい。transactionをかけて、databaseのactor情報を更新する。
			- 仮にこのノードがnetwork partitionの障害の影響下にあり、さらにminorityだった場合は、databaseへの書き込みがエラーになる
			  - その場合はpanicしたほうがいいかもしれない。これに接続していたclientは別のノードに接続しに行くことになる
			- databaseへの書き込みがretry中に別のノードがprocessを増やすことも考えられることに注意。とはいえ、一度消えたprocess idが再利用されることはないので、間違えて新しく追加されたやつを消さないようにする.
		NodeConnectionTimeout: 指定されたidのプロセスが存在するノードへの接続が成功しなかった
		以下のケースが考えられる
			- 現在ターゲットのノードが落ちている。もしくはネットワーク分断が発生している。
		対策
			- 状況は極めてわからない。processIdを消すことはない。
		
		共通の対策
		statelessなので、actor情報をdatabaseからロードし直して、リスト内の他の利用可能そうなprocessにリクエストを投げる。
		全processで失敗したら、もしまだprocessのサイズに余裕があるのであれば自ノードで作成、そうでなければpanicしてもいいかも。
		(ActorProcessGoneのエラーの場合には、そのprocessIdをリストから除外しておく。)

		### statefull actorの場合 (otherwise)
		master lease的なものが必要。5secに一回とか、現在のactorのオーナーのノードがtimestampを更新する
		他はtxかけてロードし直して状態をチェックする。
		もしtimestampが古すぎる場合には、現在のノードが落ちたとみなし、他のノードで作ったprocessに置き換える
		すでにtxが走っていたら単純にリロードして、leaseの状態やprocess idを再度チェックする

		呼び出された側のエラー処理はProcess.Callの中で行われる
		*/
		if err.Is(ActorProcessGone) {
			//TODO: how we process the case of conf.Size == 1 (statefull actor)?
			pid := err.GoneProcessId()
			if conf := amgr().findConfig(a.id); conf != nil {
				factory := &idemfactory{ conf: conf }
				if err := db().txn(func (dbh dbh) error {
					//this code block may be executed repeatedly.
					if err := a.load(dbh); err != nil {
						return newerr(ActorRuntimeError, err.Error())
					} else {
						a.removeProcess(pid)
						//if no process become available, create new one.
						if len(a.processes) <= 0 {
							if err := a.addProcess(factory); err != nil {
								return err
							}
						}
					}
					return a.update(dbh)
				}); err != nil {
					factory.Destroy()
					return err
				}
				goto RETRY
			}
		} //TODO: process connection timeout. 
		return error(err)
	}
	return nil
}

func (a *actor) balance() proto.ProcessId {
	idx := a.index
	a.index++
	if a.index >= len(a.processes) {
		a.index = 0
	}
	return a.processes[idx]
}

//addProcess create process here, and add it to processes. 
//TODO: if this node already has so many processes, should we choose another node and delegate creation?
func (a *actor) addProcess(f *idemfactory) error {
	if a.limit <= len(a.processes) {
		return newerr(ActorAlreadyHasProcess, a.limit, a.processes)
	}
	p, err := f.Create(a)
	if err != nil {
		return err
	}
	for _, pid := range a.processes {
		if pid == p.Id {
			return nil
		}
	}
	a.processes = append(a.processes, p.Id)
	return nil
}

func (a *actor) removeProcess(pid proto.ProcessId) error {
	for idx, pp := range a.processes {
		if pid == pp {
			if idx > 0 {
				a.processes = append(a.processes[0:idx], a.processes[idx+2:]...)
			} else {
				a.processes = a.processes[1:]
			}
			pmgr().kill(pid)
			return nil
		}
	}
	return nil
}

func (a *actor) update(dbh dbh) error {
	pa, err := a.proto()
	if err == nil {
		_, err = store(dbh, pa, []string { "Processes" })
	}
	return err
}

func (a *actor) insert(dbh dbh) error {
	pa, err := a.proto()
	if err == nil {
		return dbh.Insert(pa)
	}
	return err
}

//loadActor loads actor which has given id.
func (a *actor) load(dbh dbh) *ActorError {
	pa := proto.Actor{}
	if err := dbh.SelectOne(&pa, "select * from yue.actors where id = $1", a.id); err != nil {
		//TODO: return ActorNotFound when it actually not found.
		return newerr(ActorNotFound, a.id)
	}
	if err := a.from(&pa); err != nil {
		return newerr(ActorRuntimeError, err.Error())
	}
	return nil
}


func (a *actor) from(p *proto.Actor) error {
	pl := proto.ProcessList{}
	if err := pl.Unmarshal(p.Processes); err != nil {
		return err
	}
	a.id = p.Id
	a.limit = int(pl.Limit)
	a.processes = make([]proto.ProcessId, len(pl.List))
	for i, pid := range pl.List {
		a.processes[i] = proto.ProcessId(pid)
	}
	return nil
}

func (a *actor) proto() (*proto.Actor, error) {
	list := proto.ProcessList {
		Limit: uint32(a.limit),
		List: make([]proto.ProcessId, len(a.processes)),
	}
	for i, pid := range a.processes {
		list.List[i] = proto.ProcessId(pid)
	}
	bytes, err := list.Marshal()
	if err != nil {
		return nil, err
	}
	return &proto.Actor {
		Id: a.id,
		Processes: bytes,
	}, nil
}



//idempotent process creator
type idemfactory struct {
	conf *ActorConfig
	params map[string]string
	proc *Process
}

//create creates process with idempotency. 
func (f *idemfactory) Create(owner *actor) (*Process, error) {
	if f.proc != nil {
		f.proc.owner = owner
		return f.proc, nil
	}
	return pmgr().spawn(owner, &(f.conf.SpawnConfig))
}

func (f *idemfactory) Destroy() {
	if f.proc != nil {
		pmgr().kill(f.proc.Id)
		f.proc = nil
	}
}


//SpawnOption represent options for spawn
type ActorConfig struct {
	SpawnConfig SpawnConfig //object which describe how to create 
	Size int		//how many process can join this actor? if == 1, means only 1 process can serve as this actor.
					//if <= 0, means unlimited processes can serve.
	Throttle int 	//only enable when Size != 1.
					//hard limit of request per second(rps). if rps exceed throttle, 
					//and # of processes less than Size, new process will be added. 
					//TODO: should throttle-exceeded actor reject request so that caller try another caller?
}

func (c *ActorConfig) copy(args []interface{}) *ActorConfig {
	cp := &ActorConfig{}
	*cp = *c
	if cp.SpawnConfig.args == nil {
		cp.SpawnConfig.args = args	
	} else {
		cp.SpawnConfig.args = append(args, cp.SpawnConfig.args...)
	}
	return cp
}

//represents common (internal) actor manager
type actormgr struct {
	configs map[string]*ActorConfig
	reconfigs map[*regexp.Regexp]*ActorConfig
	actors map[string]*actor
	restartq chan *Process //restart queue
	cmtx sync.RWMutex
	amtx sync.RWMutex
}

func newactormgr() *actormgr {
	return &actormgr {
		configs: make(map[string]*ActorConfig),
		reconfigs: make(map[*regexp.Regexp]*ActorConfig),
		actors: make(map[string]*actor),
		restartq: make(chan *Process),
		cmtx: sync.RWMutex{},
		amtx: sync.RWMutex{},
	}
}

//start does periodic task to maintain actor store. should run as goroutine
func (am *actormgr) start() {
	ticker := time.NewTicker(1000 * time.Millisecond)
	for {
		select {
		case <- ticker.C:
			//log.Printf("tick %v", t)
			//TODO: reload each actor's state
		case p := <- am.restartq:
			go p.boot()
		}
	}
}

//loadActor loads actor which has given id.
func (am *actormgr) loadActor(act *actor, dbh dbh) *ActorError {
	a := proto.Actor{}
	if err := dbh.SelectOne(&a, "select * from yue.actors where id = $1", act.id); err != nil {
		//TODO: return ActorNotFound when it actually not found.
		return newerr(ActorNotFound, act.id)
	}
	if err := act.from(&a); err != nil {
		return newerr(ActorRuntimeError, err.Error())
	}
	return nil
}

//
var idPattern *regexp.Regexp = regexp.MustCompile("/:([^/]+)")
func (am *actormgr) compilePattern(id string) (*regexp.Regexp, error) {
	if idPattern.MatchString(id) {
		resrc := idPattern.ReplaceAllString(id, "/(?P<${1}>[^/]+)")
		log.Printf("resrc = %v", resrc)
		return regexp.Compile(resrc)
	}
	return nil, nil
}

//registerActorConfig registers actor creation setting for given id
func (am *actormgr) registerConfig(id string, conf *ActorConfig, args []interface{}) {
	//avoid modifying original conf, cause it may be reused
	cp := conf.copy(args)
	defer am.cmtx.Unlock()
	am.cmtx.Lock()
	re, err := am.compilePattern(id)
	if err != nil {
		panic(err.Error())
	} else if re != nil {
		am.reconfigs[re] = cp
	} else {
		am.configs[id] = cp
	}
}

//findConfig finds actor creation option
func (am *actormgr) findConfig(id string) *ActorConfig {
	defer am.cmtx.RUnlock()
	am.cmtx.RLock()
	if c, ok := am.configs[id]; ok {
		return c
	} 
	for re, c := range am.reconfigs {
		if matches := re.FindStringSubmatch(id); matches != nil {
			if len(matches) > 1 {
				params := make([]interface{}, len(matches) - 1)
				for i, m := range matches[1:] { //0th index represents entire match
					log.Printf("matches %v %v", i, m)
					params[i] = m
				}
				return c.copy(params)
			} else {
				return c
			}
		}
	}
	return nil
}

//addToCache adds actor object to cache
func (am *actormgr) registerActor(a *actor) {
	defer am.amtx.Unlock()
	am.amtx.Lock()
	am.actors[a.id] = a
}

//findActor finds actor from its cache
func (am *actormgr) findActor(id string) (*actor, bool) {
	defer am.amtx.RUnlock()
	am.amtx.RLock()
	a, ok := am.actors[id]
	return a, ok
}

//ensureLoaded ensure actor which has given id and option *opts* loaded.
func (am *actormgr) ensureLoaded(id string) (*actor, error) {
	if a, ok := am.findActor(id); ok {
	log.Printf("ensureLoaded already %v", id)
		return a, nil
	}
	conf := am.findConfig(id)
	if conf == nil {
		return nil, newerr(ActorNotFound, id)
	}
	factory := &idemfactory{ conf: conf }
	a := newactor(id, conf)
	if err := db().txn(func (dbh dbh) error {
		//this code block be executed repeatedly.
		if err := a.load(dbh); err != nil {
			log.Printf("loadActor err: %v", err)
			if err.Is(ActorNotFound) {
				//aがない場合、追加でactorを作成する.
				if err := a.addProcess(factory); err != nil {
					return err
				} else if err := a.insert(dbh); err != nil {
					return err
				}
			} else {
				//その他のシステムエラー:一旦エラーを返す
				return err
			}
		} else if conf.Size != 1 {
			if conf.Throttle <= 0 {
				//aがある場合でthrottleが<=0の場合、追加でactorを作成する 
				if err := a.addProcess(factory); err != nil {
					return err
				} else if err := a.update(dbh); err != nil {
					return err
				}
			} //else aがある場合でthrottleが>0の場合、そのまま。どこかのactorがthrottle以上のリクエストを受けたらactorをさらに作成する
		} //else 1つのactorしか許されていない。そのままaを返す
		log.Printf("tx success")
		return nil
	}); err != nil {
		//TODO: エラーでなおかつ追加用のプロセスが作られてしまっていた場合、プロセスを削除する.
		factory.Destroy()
		return nil, err
	}
	am.registerActor(a)
	return a, nil
}

func (am *actormgr) unloadProcess(p *Process) error {
	a := p.owner
	return db().txn(func (dbh dbh) error {
		if err := a.removeProcess(p.Id); err != nil {
			return err
		}
		if _, err := dbh.Update(a.proto()); err != nil {
			return err
		}
		return nil
	})
}
