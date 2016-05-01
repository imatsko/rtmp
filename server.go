
package rtmp

import (
	"bytes"
	"net"
	"fmt"
	"reflect"
	"io/ioutil"
	"os"
	"bufio"
	"log"
	"time"
	"strings"
	_ "runtime/debug"
	"io"
)

type EventS struct {
	Id   int
	Msg  *Msg
	Peer RtmpPeer
}

func (e EventS) String() string {
	switch e.Id {
	case E_NEW:
		return "new"
	case E_PUBLISH:
		return "publish"
	case E_PLAY:
		return "play"
	case E_DATA:
		switch e.Msg.TypeId {
		case MSG_VIDEO:
			return fmt.Sprintf("ts %d video %d bytes key %t", e.Msg.CurTs, e.Msg.Data.Len(), e.Msg.Key)
		case MSG_AUDIO:
			return fmt.Sprintf("ts %d audio %d bytes", e.Msg.CurTs, e.Msg.Data.Len())
		}
	case E_CLOSE:
		return "close"
	}
	return ""
}


/*
server:
 connect
 createStream
 publish
client:
 connect
 createStream
 getStreamLength
 play
*/

const (
	E_NEW = iota
	E_PUBLISH
	E_PLAY
	E_DATA
	E_EXTRA
	E_CLOSE
)

type RtmpServer struct {
	Event     chan EventS
	EventDone chan int
}

func NewRtmpServer() *RtmpServer {
	return &RtmpServer{
	Event: make(chan EventS, 0),
		EventDone: make(chan int, 0),
	}
}

func (s *RtmpServer)Loop() {
	idmap := map[string]RtmpPeer{}
	pubmap := map[string]RtmpPeer{}

	for {
		e := <- s.Event
		if e.Id == E_DATA {
			l.Printf("data %v: %v", e.Peer, e)
		} else {
			l.Printf("event %v: %v", e.Peer, e)
		}
		switch e.Id {
		case E_NEW:
			idmap[e.Peer.Id()] = e.Peer
		case E_PUBLISH:
			if _, ok := pubmap[e.Peer.App()]; ok {
				l.Printf("event %v: duplicated publish with %v app %s", e.Peer, pubmap[e.Peer.App()], e.Peer.App())
				e.Peer.Stop()
			} else {
				e.Peer.SetRole(PUBLISHER)
				fmt.Printf("DUMP PUB: '%s'", e.Peer.App())
				pubmap[e.Peer.App()] = e.Peer
			}
		case E_PLAY:
			e.Peer.SetRole(PLAYER)
			for _, peer := range idmap {
				if peer.Role() == PUBLISHER && peer.App() == e.Peer.App() && peer.DataReady(){

					w, h, extraA, extraV, meta := peer.Meta()
					e.Peer.SetMeta(w, h, extraA, extraV, meta)
				}
			}
		case E_CLOSE:
			if e.Peer.Role() == PUBLISHER {
				delete(pubmap, e.Peer.App())
				for _, peer := range idmap {
					if peer.Role() == PLAYER && peer.App() == e.Peer.App() {
						peer.Stop()
					}
				}
			}
			delete(idmap, e.Peer.Id())
		case E_EXTRA:
			for _, peer := range idmap {
				fmt.Printf("DUMP peer %+v chk %v\n", peer, peer.Role() == PLAYER && peer.App() == e.Peer.App())
				if peer.Role() == PLAYER && peer.App() == e.Peer.App() {
					w, h, extraA, extraV, meta := e.Peer.Meta()
					peer.SetMeta(w, h, extraA, extraV, meta)
				}
			}
		case E_DATA:
			for _, peer := range idmap {
				if peer.Role() == PLAYER && peer.App() == e.Peer.App() {
					fmt.Printf("DUMP send %v\n", e.Msg)
					ok := peer.Send(e.Msg)
					if !ok {
						l.Printf("event %v: send failed", e.Peer)
					} else {
						l.Printf("event %v: send ok", e.Peer)
					}
				}
			}
		}
		s.EventDone <- 1
	}
}

type RtmpPeer interface {
	Id() string
	App() string
	SetApp(app string)
	Meta() (w int, h int, extraA []byte, extraV []byte, meta AMFObj)
	SetMeta(w int, h int, extraA []byte, extraV []byte, meta AMFObj)

	Role() int
	SetRole(int)

	DataReady() bool
	Stop() bool
	Send(m *Msg) bool
}


func (p *RtmpPeerImpl) Id() string { return p.id }
func (p *RtmpPeerImpl) App() string {return p.app}
func (p *RtmpPeerImpl) SetApp(app string) {
	fmt.Printf("APP: %s\n", app)
	p.app = app
}
func (p *RtmpPeerImpl) Meta() (w int, h int, extraA []byte, extraV []byte, meta AMFObj) {
	return p.W, p.H, p.extraA, p.extraV, p.meta
}
func (p *RtmpPeerImpl) SetMeta(w int, h int, extraA []byte, extraV []byte, meta AMFObj) {
	p.W = w
	p.H = h
	p.extraA = extraA
	p.extraV = extraV
	p.meta = meta
}

func (p *RtmpPeerImpl) Role() int {return p.role}
func (p *RtmpPeerImpl) SetRole(role int) {
	p.role = role
	if(role == PLAYER) {
		p.que = make(chan *Msg, 16)
	}
}

func (p *RtmpPeerImpl) DataReady() bool {return p.stat == WAIT_DATA}

func (p *RtmpPeerImpl) Stop() bool {
	if p.role == PUBLISHER {
		p.mr.Close()
		return true
	} else {
		return p.Send(nil)
	}
}

func (p *RtmpPeerImpl) Send(m *Msg) bool {
	ch := reflect.ValueOf(p.que)
	return ch.TrySend(reflect.ValueOf(m))
}


type RtmpPeerImpl struct {
	id string
	s *RtmpServer
	mr *MsgStream

	role int
	stat int
	app string
	W,H int
	extraA, extraV []byte
	meta AMFObj

	que chan *Msg
	l *log.Logger
}

func NewRtmpPeer(s *RtmpServer, c io.ReadWriteCloser) *RtmpPeerImpl {
	mr := NewMsgStream(c)
	peer := &RtmpPeerImpl{id: mr.id, s: s, mr:mr}
	s.Event <- EventS{Id:E_NEW, Peer: peer}
	<- s.EventDone
	return peer
}

func (p *RtmpPeerImpl) String() string {
	return p.Id()
}

func (p *RtmpPeerImpl) handleConnect(trans float64, app string) {

	l.Printf("stream %v: connect: %s", p.mr, app)

	p.app = app

	p.mr.WriteMsg32(2, MSG_ACK_SIZE, 0, 5000000)
	p.mr.WriteMsg32(2, MSG_BANDWIDTH, 0, 5000000)
	p.mr.WriteMsg32(2, MSG_CHUNK_SIZE, 0, 128)

	p.mr.WriteAMFCmd(3, 0, []AMFObj {
		AMFObj { atype : AMF_STRING, str : "_result", },
		AMFObj { atype : AMF_NUMBER, f64 : trans, },
		AMFObj { atype : AMF_OBJECT,
			obj : map[string] AMFObj {
				"fmtVer" : AMFObj { atype : AMF_STRING, str : "FMS/3,0,1,123", },
				"capabilities" : AMFObj { atype : AMF_NUMBER, f64 : 31, },
			},
		},
		AMFObj { atype : AMF_OBJECT,
			obj : map[string] AMFObj {
				"level" : AMFObj { atype : AMF_STRING, str : "status", },
				"code" : AMFObj { atype : AMF_STRING, str : "NetConnection.Connect.Success", },
				"description" : AMFObj { atype : AMF_STRING, str : "Connection Success.", },
				"objectEncoding" : AMFObj { atype : AMF_NUMBER, f64 : 0, },
			},
		},
	})
}

func (p *RtmpPeerImpl) handleMeta(obj AMFObj) {

	p.meta = obj
	p.W = int(obj.obj["width"].f64)
	p.H = int(obj.obj["height"].f64)

	l.Printf("stream %v: meta video %dx%d (%v)", p.mr, p.W, p.H, obj)
}

func (p *RtmpPeerImpl) handleCreateStream(trans float64) {

	l.Printf("stream %v: createStream", p.mr)

	p.mr.WriteAMFCmd(3, 0, []AMFObj {
		AMFObj { atype : AMF_STRING, str : "_result", },
		AMFObj { atype : AMF_NUMBER, f64 : trans, },
		AMFObj { atype : AMF_NULL, },
		AMFObj { atype : AMF_NUMBER, f64 : 1 },
	})
}

func (p *RtmpPeerImpl) handleGetStreamLength(trans float64) {
}

func (p *RtmpPeerImpl) handlePublish() {

	l.Printf("stream %v: publish", p.mr)

	p.mr.WriteAMFCmd(3, 0, []AMFObj {
		AMFObj { atype : AMF_STRING, str : "onStatus", },
		AMFObj { atype : AMF_NUMBER, f64 : 0, },
		AMFObj { atype : AMF_NULL, },
		AMFObj { atype : AMF_OBJECT,
			obj : map[string] AMFObj {
				"level" : AMFObj { atype : AMF_STRING, str : "status", },
				"code" : AMFObj { atype : AMF_STRING, str : "NetStream.Publish.Start", },
				"description" : AMFObj { atype : AMF_STRING, str : "Start publising.", },
			},
		},
	})

	p.s.Event <- EventS{Id:E_PUBLISH, Peer: p}
	<- p.s.EventDone
}

type testsrc struct {
	r *bufio.Reader
	dir string
	w,h int
	ts int
	codec string
	key bool
	idx int
	data []byte
}

func tsrcNew() (m *testsrc) {
	m = &testsrc{}
	m.dir = "/pixies/go/data/tmp"
	fi, _ := os.Open(fmt.Sprintf("%s/index", m.dir))
	m.r = bufio.NewReader(fi)
	l, _ := m.r.ReadString('\n')
	fmt.Sscanf(l, "%dx%d", &m.w, &m.h)
	return
}

func (m *testsrc) fetch() (err error) {
	l, err := m.r.ReadString('\n')
	if err != nil {
		return
	}
	a := strings.Split(l, ",")
	fmt.Sscanf(a[0], "%d", &m.ts)
	m.codec = a[1]
	fmt.Sscanf(a[2], "%d", &m.idx)
	switch m.codec {
	case "h264":
		fmt.Sscanf(a[3], "%t", &m.key)
		m.data, err = ioutil.ReadFile(fmt.Sprintf("%s/h264/%d.264", m.dir, m.idx))
	case "aac":
		m.data, err = ioutil.ReadFile(fmt.Sprintf("%s/aac/%d.aac", m.dir, m.idx))
	}
	return
}

func (p *RtmpPeerImpl) handlePlay(strid int) {

	l.Printf("stream %v: play", p.mr)

	var tsrc *testsrc
	//tsrc = tsrcNew()

	if tsrc == nil {
		p.s.Event <- EventS{Id:E_PLAY, Peer: p}
		<- p.s.EventDone
	} else {
		l.Printf("stream %v: test play data in %s", p.mr, tsrc.dir)
		p.W = tsrc.w
		p.H = tsrc.h
		l.Printf("stream %v: test video %dx%d", p.mr, p.W, p.H)
	}

	begin := func () {

		var b bytes.Buffer
		WriteInt(&b, 0, 2)
		WriteInt(&b, strid, 4)
		p.mr.WriteMsg(0, 2, MSG_USER, 0, 0, b.Bytes()) // stream begin 1

		p.mr.WriteAMFCmd(5, strid, []AMFObj {
			AMFObj { atype : AMF_STRING, str : "onStatus", },
			AMFObj { atype : AMF_NUMBER, f64 : 0, },
			AMFObj { atype : AMF_NULL, },
			AMFObj { atype : AMF_OBJECT,
				obj : map[string] AMFObj {
					"level" : AMFObj { atype : AMF_STRING, str : "status", },
					"code" : AMFObj { atype : AMF_STRING, str : "NetStream.Play.Start", },
					"description" : AMFObj { atype : AMF_STRING, str : "Start live.", },
				},
			},
		})

		l.Printf("stream %v: begin: video %dx%d", p.mr, p.W, p.H)

		p.mr.WriteAMFMeta(5, strid, []AMFObj {
			AMFObj { atype : AMF_STRING, str : "|RtmpSampleAccess", },
			AMFObj { atype : AMF_BOOLEAN, i: 1, },
			AMFObj { atype : AMF_BOOLEAN, i: 1, },
		})

		p.meta.obj["Server"] = AMFObj { atype : AMF_STRING, str : "Golang Rtmp Server", }
		p.meta.atype = AMF_OBJECT
		l.Printf("stream %v: %v", p.mr, p.meta)
		p.mr.WriteAMFMeta(5, strid, []AMFObj {
			AMFObj { atype : AMF_STRING, str : "onMetaData", },
			p.meta,
			/*
			AMFObj { atype : AMF_OBJECT,
				obj : map[string] AMFObj {
					"Server" : AMFObj { atype : AMF_STRING, str : "Golang Rtmp Server", },
					"width" : AMFObj { atype : AMF_NUMBER, f64 : float64(mr.W), },
					"height" : AMFObj { atype : AMF_NUMBER, f64 : float64(mr.H), },
					"displayWidth" : AMFObj { atype : AMF_NUMBER, f64 : float64(mr.W), },
					"displayHeight" : AMFObj { atype : AMF_NUMBER, f64 : float64(mr.H), },
					"duration" : AMFObj { atype : AMF_NUMBER, f64 : 0, },
					"framerate" : AMFObj { atype : AMF_NUMBER, f64 : 25000, },
					"videodatarate" : AMFObj { atype : AMF_NUMBER, f64 : 731, },
					"videocodecid" : AMFObj { atype : AMF_NUMBER, f64 : 7, },
					"audiodatarate" : AMFObj { atype : AMF_NUMBER, f64 : 122, },
					"audiocodecid" : AMFObj { atype : AMF_NUMBER, f64 : 10, },
				},
			},
			*/
		})
	}

	end := func () {

		l.Printf("stream %v: end", p.mr)

		var b bytes.Buffer
		WriteInt(&b, 1, 2)
		WriteInt(&b, strid, 4)
		p.mr.WriteMsg(0, 2, MSG_USER, 0, 0, b.Bytes()) // stream eof 1

		p.mr.WriteAMFCmd(5, strid, []AMFObj {
			AMFObj { atype : AMF_STRING, str : "onStatus", },
			AMFObj { atype : AMF_NUMBER, f64 : 0, },
			AMFObj { atype : AMF_NULL, },
			AMFObj { atype : AMF_OBJECT,
				obj : map[string] AMFObj {
					"level" : AMFObj { atype : AMF_STRING, str : "status", },
					"code" : AMFObj { atype : AMF_STRING, str : "NetStream.Play.Stop", },
					"description" : AMFObj { atype : AMF_STRING, str : "Stop live.", },
				},
			},
		})
	}

	if tsrc == nil {

		for {
			nr := 0

			for {
				m := <-p.que
				if m == nil {
					break
				}
				//if nr == 0 && !m.key {
				//	continue
				//}
				if nr == 0 {
					begin()
					l.Printf("stream %v: extra size %d %d", p.mr, len(p.extraA), len(p.extraV))
					p.mr.WriteAAC(strid, 0, p.extraA[2:])
					p.mr.WritePPS(strid, 0, p.extraV[5:])
				}
				l.Printf("data %v: got %v curts %v", p.mr, m, m.CurTs)
				switch m.TypeId {
				case MSG_AUDIO:
					p.mr.WriteAudio(strid, m.CurTs, m.Data.Bytes()[2:])
				case MSG_VIDEO:
					p.mr.WriteVideo(strid, m.CurTs, m.Key, m.Data.Bytes()[5:])
				}
				nr++
			}
			end()
		}

	} else {

		begin()

		lf, _ := os.Create("/tmp/rtmp.log")
		ll := log.New(lf, "", 0)

		starttm := time.Now()
		k := 0

		for {
			err := tsrc.fetch()
			if err != nil {
				panic(err)
			}
			switch tsrc.codec {
			case "h264":
				if tsrc.idx == 0 {
					p.mr.WritePPS(strid, 0, tsrc.data)
				} else {
					p.mr.WriteVideo(strid, tsrc.ts, tsrc.key, tsrc.data)
				}
			case "aac":
				if tsrc.idx == 0 {
					p.mr.WriteAAC(strid, 0, tsrc.data)
				} else {
					p.mr.WriteAudio(strid, tsrc.ts, tsrc.data)
				}
			}
			dur := time.Since(starttm).Nanoseconds()
			diff := tsrc.ts - 1000 - int(dur/1000000)
			if diff > 0 {
				time.Sleep(time.Duration(diff)*time.Millisecond)
			}
			l.Printf("data %v: ts %v dur %v diff %v", p.mr, tsrc.ts, int(dur/1000000), diff)
			ll.Printf("#%d %d,%s,%d %d", k, tsrc.ts, tsrc.codec, tsrc.idx, len(tsrc.data))
			k++
		}
	}
}

func (p *RtmpPeerImpl) Serve() {
	defer func() {
		if err := recover(); err != nil {
			p.s.Event <- EventS{Id:E_CLOSE, Peer: p}
			<- p.s.EventDone
			l.Printf("stream %v: closed %v", p.mr, err)
			//if err != "EOF" {
			//	l.Printf("stream %v: %v", mr, string(debug.Stack()))
			//}
		}
	}()

	if err := handShake(p.mr.r); err != nil {
		panic(err)
	}

	f, _ := os.Create("/tmp/pub.log")
	p.l = log.New(f, "", 0)

	for {
		m := p.mr.ReadMsg()
		if m == nil {
			continue
		}

		l.Printf("stream %v: msg %v", p.mr, m)

		if m.TypeId == MSG_AUDIO || m.TypeId == MSG_VIDEO {
			if p.stat == WAIT_EXTRA {
				if len(p.extraA) == 0 && m.TypeId == MSG_AUDIO {
			        p.l.Printf("event %v: extra got aac config", p.mr)
			        p.extraA = m.Data.Bytes()
		        }

				if len(p.extraV) == 0 && m.TypeId == MSG_VIDEO {
					l.Printf("event %v: extra got pps", p.mr)
					p.extraV = m.Data.Bytes()
				}
				if len(p.extraA) > 0 && len(p.extraV) > 0 {
					l.Printf("event %v: got all extra", p.mr)
					p.stat = WAIT_DATA
					p.s.Event <- EventS{Id:E_EXTRA, Peer: p}
					<- p.s.EventDone
				}

			} else {
				p.l.Printf("%d,%d", m.TypeId, m.Data.Len())
				p.s.Event <- EventS{Id:E_DATA, Msg:m, Peer: p}
				<- p.s.EventDone
			}
		}
		if m.TypeId == MSG_AMF_CMD || m.TypeId == MSG_AMF_META {
			a := ReadAMF(m.Data)
			l.Printf("server: amfobj %v\n", a)
			switch a.str {
			case "connect":
				a2 := ReadAMF(m.Data)
				a3 := ReadAMF(m.Data)
				if _, ok := a3.obj["app"]; !ok || a3.obj["app"].str == "" {
					panic("connect: app not found")
				}
				p.handleConnect(a2.f64, a3.obj["app"].str)
			case "@setDataFrame":
				ReadAMF(m.Data)
				a3 := ReadAMF(m.Data)
				p.handleMeta(a3)
				l.Printf("stream %v: setdataframe", p.mr)
			case "createStream":
				a2 := ReadAMF(m.Data)
				p.handleCreateStream(a2.f64)
			case "publish":
				p.handlePublish()
			case "play":
				p.handlePlay(m.StrId)
			}
		}
	}
}

func SimpleServer() {
	l.Printf("server: simple server starts")
	ln, err := net.Listen("tcp", ":1935")
	if err != nil {
		l.Printf("server: error: listen 1935 %s\n", err)
		return
	}

	s := NewRtmpServer()

	go s.Loop()
	for {
		c, err := ln.Accept()
		if err != nil {
			l.Printf("server: error: sock accept %s\n", err)
			break
		}
		go func (c net.Conn) {
			peer := NewRtmpPeer(s, c)
			peer.Serve()
		} (c)
	}
}







type RtmpPlayer struct {}

