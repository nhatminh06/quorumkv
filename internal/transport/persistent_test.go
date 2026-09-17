package transport

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestServerPersistentFrames(t *testing.T) {
	tr, err := Listen("127.0.0.1:0", func(_ context.Context, m Message) (Message, error) { return m, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	conn, err := net.Dial("tcp", tr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Second))
	for i := 0; i < 100; i++ {
		if err := WriteFrame(conn, Message{Type: MessageTest, Payload: []byte{byte(i)}}); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		response, err := ReadFrame(conn)
		if err != nil {
			t.Fatalf("response %d: %v", i, err)
		}
		if len(response.Payload) != 1 || response.Payload[0] != byte(i) {
			t.Fatalf("response %d: %+v", i, response)
		}
	}
}
