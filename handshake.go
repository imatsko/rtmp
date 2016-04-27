package rtmp

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"errors"
)

var (
	zeros = []byte{
		0x00, 0x00, 0x00, 0x00,
	}
)

func createHello(b []byte, epoch []byte) []byte {
	copy(b[0:4], epoch)
	copy(b[4:8], zeros)
	for i := 8; i < 1536; i++ {
		b[i] = byte(rand.Int() % 256)
	}
	return b[8:]
}

func createResponse(b []byte, epoch []byte, random_data []byte) {
	copy(b[:4], epoch)
	copy(b[4:8], zeros)
	copy(b[8:], random_data)
}

func parseHello(b []byte) (epoch []byte, peer_random_data []byte, err error) {
	epoch = b[:4]
	version := b[4:8]
	if !bytes.Equal(version, zeros) {
		err = errors.New("Non zero field")
	}
	l.Printf("handshake:   epoch %v version (should be zeros) %v", epoch, version)

	peer_random_data = b[9:]
	return
}

func checkResponse(b []byte, epoch []byte, random_data []byte) bool {
	same_epoch := bytes.Equal(b[:4], epoch)
	same_server := bytes.Equal(b[4:8], zeros)
	same_random := bytes.Equal(b[8:], random_data)

	fmt.Printf("same epoch %v same server %v same random %v\n", same_epoch, same_server, same_random)
	return same_epoch && same_server && same_random

}

func handShake(rw io.ReadWriter) (err error) {
	C0 := ReadBuf(rw, 1)
	if C0[0] != 0x3 {
		l.Printf("handshake: invalid rtmp version")
		return errors.New("invalid rtmp version")
	}
	C1 := ReadBuf(rw, 1536)

	epoch, clientData, err:= parseHello(C1)
	l.Printf("handshake: got client hello")

	S0 := make([]byte, 1, 1)
	S0[0] = 0x03

	S1 := make([]byte, 1536, 1536)

	serverData := createHello(S1, epoch)
	rw.Write(S0)
	rw.Write(S1)
	l.Printf("handshake: send server hello")

	S2 := make([]byte, 1536, 1536)

	createResponse(S2, epoch, clientData)

	rw.Write(S2)
	l.Printf("handshake: send server response")

	C2 := ReadBuf(rw, 1536)
	l.Printf("handshake: got client response")

	if !checkResponse(C2, epoch, serverData) {
		return errors.New("client response does not match")
	}
	return nil
}
