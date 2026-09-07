// Package dedup tracks concurrent client writes and their replicated results.
package dedup

import (
	"crypto/rand"
	"encoding/binary"
	"sync"

	"6.5840/kvsrv1/rpc"
)

type Client struct {
	mu            sync.Mutex
	id, next, ack uint64
	completed     map[uint64]bool
}

func (c *Client) Begin() (uint64, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.id == 0 {
		var data [8]byte
		if _, err := rand.Read(data[:]); err != nil {
			panic(err)
		}
		c.id = binary.LittleEndian.Uint64(data[:])
	}
	c.next++
	if c.next == 0 {
		panic("dedup: request sequence exhausted")
	}
	return c.id, c.next
}

func (c *Client) Ack() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ack
}

func (c *Client) Complete(sequence uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if sequence <= c.ack {
		return
	}
	if c.completed == nil {
		c.completed = make(map[uint64]bool)
	}
	c.completed[sequence] = true
	for c.completed[c.ack+1] {
		c.ack++
		delete(c.completed, c.ack)
	}
}

type Session struct {
	Ack     uint64
	Replies map[uint64]rpc.PutReply
}

// Table is replicated state. Callers serialize access and include it in
// snapshots and shard transfers; wall-clock expiry would be unsafe.
type Table struct{ Sessions map[uint64]*Session }

func (t *Table) Do(args rpc.PutArgs, apply func() rpc.PutReply) rpc.PutReply {
	if args.ClientID == 0 {
		return apply()
	}
	if args.Sequence == 0 || args.Ack >= args.Sequence {
		return rpc.PutReply{Err: rpc.ErrStaleRequest}
	}
	if t.Sessions == nil {
		t.Sessions = make(map[uint64]*Session)
	}
	session := t.Sessions[args.ClientID]
	if session == nil {
		session = &Session{Replies: make(map[uint64]rpc.PutReply)}
		t.Sessions[args.ClientID] = session
	}
	if args.Ack > session.Ack {
		session.Ack = args.Ack
		for sequence := range session.Replies {
			if sequence <= session.Ack {
				delete(session.Replies, sequence)
			}
		}
	}
	// The watermark prevents delayed copies from executing after reply cleanup.
	if args.Sequence <= session.Ack {
		return rpc.PutReply{Err: rpc.ErrStaleRequest}
	}
	if reply, ok := session.Replies[args.Sequence]; ok {
		return reply
	}
	reply := apply()
	session.Replies[args.Sequence] = reply
	return reply
}
