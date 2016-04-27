
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

type eventID int

type eventS struct {
	id int
	m *Msg
	peer *RtmpPeer
}

func (e eventS) String() string {
	switch e.id {
	case E_NEW:
		return "new"
	case E_PUBLISH:
		return "publish"
	case E_PLAY:
		return "play"
	case E_DATA:
		switch e.m.typeid {
		case MSG_VIDEO:
			return fmt.Sprintf("ts %d video %d bytes key %t", e.m.curts, e.m.data.Len(), e.m.key)
		case MSG_AUDIO:
			return fmt.Sprintf("ts %d audio %d bytes", e.m.curts, e.m.data.Len())
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
	event chan eventS
	eventDone chan int
}

func NewRtmpServer() *RtmpServer {
	return &RtmpServer{
	event: make(chan eventS, 0),
		eventDone: make(chan int, 0),
	}
}

func (s *RtmpServer) ServeClient(c io.ReadWriteCloser) {
	peer := NewRtmpPeer(s, c)
	peer.serve()
}

func (s *RtmpServer)Loop() {
	idmap := map[string]*RtmpPeer{}
	pubmap := map[string]*RtmpPeer{}

	for {
		e := <- s.event
		if e.id == E_DATA {
			l.Printf("data %v: %v", e.peer, e)
		} else {
			l.Printf("event %v: %v", e.peer, e)
		}
		switch e.id {
		case E_NEW:
			idmap[e.peer.id] = e.peer
		case E_PUBLISH:
			if _, ok := pubmap[e.peer.app]; ok {
				l.Printf("event %v: duplicated publish with %v app %s", e.peer, pubmap[e.peer.app], e.peer.app)
				e.peer.mr.Close()
			} else {
				e.peer.role = PUBLISHER
				pubmap[e.peer.app] = e.peer
			}
		case E_PLAY:
			e.peer.role = PLAYER
			e.peer.que = make(chan *Msg, 16)
			for _, peer := range idmap {
				if peer.role == PUBLISHER && peer.app == e.peer.app && peer.stat == WAIT_DATA {
					e.peer.W = peer.W
					e.peer.H = peer.H
					e.peer.extraA = peer.extraA
					e.peer.extraV = peer.extraV
					e.peer.meta = peer.meta
				}
			}
		case E_CLOSE:
			if e.peer.role == PUBLISHER {
				delete(pubmap, e.peer.app)
				for _, peer := range idmap {
					if peer.role == PLAYER && peer.app == e.peer.app {
						ch := reflect.ValueOf(peer.que)
						var m *Msg = nil
						ch.TrySend(reflect.ValueOf(m))
					}
				}
			}
			delete(idmap, e.peer.id)
		case E_EXTRA:
			for _, peer := range idmap {
				if peer.role == PLAYER && peer.app == e.peer.app {
					peer.W = e.peer.W
					peer.H = e.peer.H
					peer.extraA = e.peer.extraA
					peer.extraV = e.peer.extraV
					peer.meta = e.peer.meta
				}
			}
		case E_DATA:
			for _, peer := range idmap {
				if peer.role == PLAYER && peer.app == e.peer.app {
					ch := reflect.ValueOf(peer.que)
					ok := ch.TrySend(reflect.ValueOf(e.m))
					if !ok {
						l.Printf("event %v: send failed", e.peer)
					} else {
						l.Printf("event %v: send ok", e.peer)
					}
				}
			}
		}
		s.eventDone <- 1
	}
}



type RtmpPeer struct {
	id string
	s *RtmpServer
	mr *MsgStream

	meta AMFObj
	role int
	stat int
	app string
	W,H int
	extraA, extraV []byte
	que chan *Msg
	l *log.Logger
}

func NewRtmpPeer(s *RtmpServer, c io.ReadWriteCloser) *RtmpPeer {
	mr := NewMsgStream(c)
	peer := &RtmpPeer{id: mr.id, s: s, mr:mr}
	s.event <- eventS{id:E_NEW, peer: peer}
	<- s.eventDone
	return peer
}

func (p *RtmpPeer) String() string {
	return p.id
}

func (p *RtmpPeer) handleConnect(trans float64, app string) {

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

func (p *RtmpPeer) handleMeta(obj AMFObj) {

	p.meta = obj
	p.W = int(obj.obj["width"].f64)
	p.H = int(obj.obj["height"].f64)

	l.Printf("stream %v: meta video %dx%d (%v)", p.mr, p.W, p.H, obj)
}

func (p *RtmpPeer) handleCreateStream(trans float64) {

	l.Printf("stream %v: createStream", p.mr)

	p.mr.WriteAMFCmd(3, 0, []AMFObj {
		AMFObj { atype : AMF_STRING, str : "_result", },
		AMFObj { atype : AMF_NUMBER, f64 : trans, },
		AMFObj { atype : AMF_NULL, },
		AMFObj { atype : AMF_NUMBER, f64 : 1 },
	})
}

func (p *RtmpPeer) handleGetStreamLength(trans float64) {
}

func (p *RtmpPeer) handlePublish() {

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

	p.s.event <- eventS{id:E_PUBLISH, peer: p}
	<- p.s.eventDone
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

func (p *RtmpPeer) handlePlay(strid int) {

	l.Printf("stream %v: play", p.mr)

	var tsrc *testsrc
	//tsrc = tsrcNew()

	if tsrc == nil {
		p.s.event <- eventS{id:E_PLAY, peer: p}
		<- p.s.eventDone
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
				l.Printf("data %v: got %v curts %v", p.mr, m, m.curts)
				switch m.typeid {
				case MSG_AUDIO:
					p.mr.WriteAudio(strid, m.curts, m.data.Bytes()[2:])
				case MSG_VIDEO:
					p.mr.WriteVideo(strid, m.curts, m.key, m.data.Bytes()[5:])
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

func (p *RtmpPeer) serve() {
	defer func() {
		if err := recover(); err != nil {
			p.s.event <- eventS{id:E_CLOSE, peer: p}
			<- p.s.eventDone
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

		if m.typeid == MSG_AUDIO || m.typeid == MSG_VIDEO {
			if p.stat == WAIT_EXTRA {
				if len(p.extraA) == 0 && m.typeid == MSG_AUDIO {
			        p.l.Printf("event %v: extra got aac config", p.mr)
			        p.extraA = m.data.Bytes()
		        }

				if len(p.extraV) == 0 && m.typeid == MSG_VIDEO {
					l.Printf("event %v: extra got pps", p.mr)
					p.extraV = m.data.Bytes()
				}
				if len(p.extraA) > 0 && len(p.extraV) > 0 {
					l.Printf("event %v: got all extra", p.mr)
					p.stat = WAIT_DATA
					p.s.event <- eventS{id:E_EXTRA, peer: p}
					<- p.s.eventDone
				}

			} else {
				p.l.Printf("%d,%d", m.typeid, m.data.Len())
				p.s.event <- eventS{id:E_DATA, m:m, peer: p}
				<- p.s.eventDone
			}
		}
		if m.typeid == MSG_AMF_CMD || m.typeid == MSG_AMF_META {
			a := ReadAMF(m.data)
			l.Printf("server: amfobj %v\n", a)
			switch a.str {
			case "connect":
				a2 := ReadAMF(m.data)
				a3 := ReadAMF(m.data)
				if _, ok := a3.obj["app"]; !ok || a3.obj["app"].str == "" {
					panic("connect: app not found")
				}
				p.handleConnect(a2.f64, a3.obj["app"].str)
			case "@setDataFrame":
				ReadAMF(m.data)
				a3 := ReadAMF(m.data)
				p.handleMeta(a3)
				l.Printf("stream %v: setdataframe", p.mr)
			case "createStream":
				a2 := ReadAMF(m.data)
				p.handleCreateStream(a2.f64)
			case "publish":
				p.handlePublish()
			case "play":
				p.handlePlay(m.strid)
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
			s.ServeClient(c)
		} (c)
	}
}




type RtmpPlayer struct {}

