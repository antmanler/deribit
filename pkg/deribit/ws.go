package deribit

import (
	"io"

	"github.com/gorilla/websocket"
	"github.com/refunc/go-observer"
)

// A ObjectStream is a jsonrpc2.ObjectStream that uses a WebSocket to
// send and receive JSON-RPC 2.0 objects.
type objectStream struct {
	conn   *websocket.Conn
	errors observer.Property
}

// newObjectStream creates a new jsonrpc2.ObjectStream for sending and
// receiving JSON-RPC 2.0 objects over a WebSocket.
func newObjectStream(conn *websocket.Conn, errors observer.Property) *objectStream {
	return &objectStream{conn: conn}
}

// WriteObject implements jsonrpc2.ObjectStream.
func (t *objectStream) WriteObject(obj interface{}) error {
	if err := t.conn.WriteJSON(obj); err != nil {
		t.errors.Update(err)
		return err
	}
	return nil
}

// ReadObject implements jsonrpc2.ObjectStream.
func (t *objectStream) ReadObject(v interface{}) error {
	err := t.conn.ReadJSON(v)
	if e, ok := err.(*websocket.CloseError); ok {
		if e.Code == websocket.CloseAbnormalClosure && e.Text == io.ErrUnexpectedEOF.Error() {
			// Suppress a noisy (but harmless) log message by
			// unwrapping this error.
			err = io.ErrUnexpectedEOF
		}
	}
	if err != nil {
		t.errors.Update(err)
	}
	return err
}

// Close implements jsonrpc2.ObjectStream.
func (t *objectStream) Close() error {
	return t.conn.Close()
}
