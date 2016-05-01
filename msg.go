
package rtmp

import (
	"io"
	"bytes"
	"fmt"
)

var (
	MSG_CHUNK_SIZE         = 1
	MSG_ABORT              = 2
	MSG_ACK                = 3
	MSG_USER               = 4
	MSG_ACK_SIZE           = 5
	MSG_BANDWIDTH          = 6
	MSG_EDGE               = 7
	MSG_AUDIO              = 8
	MSG_VIDEO              = 9
	MSG_AMF3_META          = 15
	MSG_AMF3_SHARED        = 16
	MSG_AMF3_CMD           = 17
	MSG_AMF_META           = 18
	MSG_AMF_SHARED         = 19
	MSG_AMF_CMD            = 20
	MSG_AGGREGATE          = 22
	MSG_MAX                = 22
)

var (
	MsgTypeStr = []string {
		"?",
		"CHUNK_SIZE", "ABORT", "ACK",
		"USER", "ACK_SIZE", "BANDWIDTH", "EDGE",
		"AUDIO", "VIDEO",
		"AMF3_META", "AMF3_SHARED", "AFM3_CMD",
		"AMF_META", "AMF_SHARED", "AMF_CMD",
		"AGGREGATE",
	}
)

type chunkHeader struct {
	TypeId  int
	MLen    int
	Csid    int
	Cfmt    int
	Ts      int
	TsDelta int
	StrId   int
}

func readChunkHeader (r io.Reader) (m chunkHeader) {
	i := ReadInt(r, 1)
	m.Cfmt = (i>>6)&3;
	m.Csid = i&0x3f;

	if m.Csid == 0 {
		j := ReadInt(r, 1)
		m.Csid = j + 64
	}

	if m.Csid == 0x3f {
		j := ReadInt(r, 2)
		m.Csid = j + 64
	}

	if m.Cfmt == 0 {
		m.Ts = ReadInt(r, 3)
		m.MLen = ReadInt(r, 3)
		m.TypeId = ReadInt(r, 1)
		m.StrId = ReadIntLE(r, 4)
	}

	if m.Cfmt == 1 {
		m.TsDelta = ReadInt(r, 3)
		m.MLen = ReadInt(r, 3)
		m.TypeId = ReadInt(r, 1)
	}

	if m.Cfmt == 2 {
		m.TsDelta = ReadInt(r, 3)
	}

	if m.Ts == 0xffffff {
		m.Ts = ReadInt(r, 4)
	}
	if m.TsDelta == 0xffffff {
		m.TsDelta = ReadInt(r, 4)
	}

	//l.Printf("chunk:   %v", m)

	return
}

const (
	UNKNOWN = 0
	PLAYER = 1
	PUBLISHER = 2
)

const (
	WAIT_EXTRA = 0
	WAIT_DATA = 1
)


type MsgStream struct {
	r stream
	Msg map[int]*Msg
	vts, ats int

	id string
	strid int
}

type Msg struct {
	chunkHeader
	Data  *bytes.Buffer

	Key   bool
	CurTs int
}

func (m *Msg) String() string {
	var typestr string
	if m.TypeId < len(MsgTypeStr) {
		typestr = MsgTypeStr[m.TypeId]
	} else {
		typestr = "?"
	}
	return fmt.Sprintf("%s %d %v", typestr, m.MLen, m.chunkHeader)
}

var (
	mrseq = 0
)

func NewMsgStream(r io.ReadWriteCloser) *MsgStream {
	mrseq++
	return &MsgStream{
		r:stream{r},
		Msg:map[int]*Msg{},
		id:fmt.Sprintf("#%d", mrseq),
	}
}

func (mr *MsgStream) String() string {
	return mr.id
}

func (mr *MsgStream) Close() {
	mr.r.Close()
}

func (r *MsgStream) WriteMsg(cfmt, csid, typeid, strid, ts int, data []byte) {
	var b bytes.Buffer
	start := 0
	for i := 0; start < len(data); i++ {
		if i == 0 {
			if cfmt == 0 {
				WriteInt(&b, csid, 1)  // fmt=0 csid
				WriteInt(&b, ts, 3) // ts
				WriteInt(&b, len(data), 3) // message length
				WriteInt(&b, typeid, 1) // message type id
				WriteIntLE(&b, strid, 4) // message stream id
			} else {
				WriteInt(&b, 0x1<<6 + csid, 1)  // fmt=1 csid
				WriteInt(&b, ts, 3) // tsdelta
				WriteInt(&b, len(data), 3) // message length
				WriteInt(&b, typeid, 1) // message type id
			}
		} else {
			WriteBuf(&b, []byte{0x3<<6 + byte(csid)}) // fmt=3, csid
		}
		size := 128
		if len(data) - start < size {
			size = len(data) - start
		}
		WriteBuf(&b, data[start:start+size])
		WriteBuf(r.r, b.Bytes())
		b.Reset()
		start += size
	}
	l.Printf("Msg: csid %d ts %d paylen %d", csid, ts, len(data))
}

func (r *MsgStream) WriteAudio(strid, ts int, data []byte) {
	d := append([]byte{0xaf, 1}, data...)
	tsdelta := ts - r.ats
	r.ats = ts
	r.WriteMsg(1, 7, MSG_AUDIO, strid, tsdelta, d)
}

func (r *MsgStream) WriteAAC(strid, ts int, data []byte) {
	d := append([]byte{0xaf, 0}, data...)
	r.ats = ts
	r.WriteMsg(0, 7, MSG_AUDIO, strid, ts, d)
}

func (r *MsgStream) WriteVideo(strid,ts int, key bool, data []byte) {
	var b int
	if key {
		b = 0x17
	} else {
		b = 0x27
	}
	d := append([]byte{byte(b), 1, 0, 0, 0x50}, data...)
	tsdelta := ts - r.vts
	r.vts = ts
	r.WriteMsg(1, 6, MSG_VIDEO, strid, tsdelta, d)
}

func (r *MsgStream) WritePPS(strid, ts int, data []byte) {
	d := append([]byte{0x17, 0, 0, 0, 0}, data...)
	r.vts = ts
	r.WriteMsg(0, 6, MSG_VIDEO, strid, ts, d)
}

func (r *MsgStream) WriteAMFMeta(csid, strid int, a []AMFObj) {
	var b bytes.Buffer
	for _, v := range a {
		WriteAMF(&b, v)
	}
	r.WriteMsg(0, csid, MSG_AMF_META, strid, 0, b.Bytes())
}

func (r *MsgStream) WriteAMFCmd(csid, strid int, a []AMFObj) {
	var b bytes.Buffer
	for _, v := range a {
		WriteAMF(&b, v)
	}
	r.WriteMsg(0, csid, MSG_AMF_CMD, strid, 0, b.Bytes())
}

func (r *MsgStream) WriteMsg32(csid, typeid, strid, v int) {
	var b bytes.Buffer
	WriteInt(&b, v, 4)
	r.WriteMsg(0, csid, typeid, strid, 0, b.Bytes())
}

func (r *MsgStream) ReadMsg() *Msg {
	ch := readChunkHeader(r.r)
	m, ok := r.Msg[ch.Csid]
	if !ok {
		//l.Printf("chunk:   new")
		m = &Msg{ch, &bytes.Buffer{}, false, 0}
		r.Msg[ch.Csid] = m
	}

	switch ch.Cfmt {
	case 0:
		m.Ts = ch.Ts
		m.MLen = ch.MLen
		m.TypeId = ch.TypeId
		m.CurTs = m.Ts
	case 1:
		m.TsDelta = ch.TsDelta
		m.MLen = ch.MLen
		m.TypeId = ch.TypeId
		m.CurTs += m.TsDelta
	case 2:
		m.TsDelta = ch.TsDelta
	}

	left := m.MLen - m.Data.Len()
	size := 128
	if size > left {
		size = left
	}
	//l.Printf("chunk:   %v", m)
	if size > 0 {
		io.CopyN(m.Data, r.r, int64(size))
	}

	if size == left {
		rm := new(Msg)
		*rm = *m
		l.Printf("event: fmt%d %v curts %d pre %v", ch.Cfmt, m, m.CurTs, m.Data.Bytes()[:9])
		if m.TypeId == MSG_VIDEO && int(m.Data.Bytes()[0]) == 0x17 {
			rm.Key = true
		} else {
			rm.Key = false
		}
		m.Data = &bytes.Buffer{}
		return rm
	}

	return nil
}

