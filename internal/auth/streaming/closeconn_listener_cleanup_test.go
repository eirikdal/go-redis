package streaming

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/pool"
)

// stubNetConn is a minimal net.Conn for pool tests (avoids &net.TCPConn{} which breaks syscalls).
type stubNetConn struct{}

func (stubNetConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (stubNetConn) Write(p []byte) (int, error)      { return len(p), nil }
func (stubNetConn) Close() error                     { return nil }
func (stubNetConn) LocalAddr() net.Addr              { return nil }
func (stubNetConn) RemoteAddr() net.Addr             { return nil }
func (stubNetConn) SetDeadline(time.Time) error     { return nil }
func (stubNetConn) SetReadDeadline(time.Time) error  { return nil }
func (stubNetConn) SetWriteDeadline(time.Time) error { return nil }

// Regression: ConnPool.CloseConn and putConn's shouldCloseConn path used to skip
// ProcessOnRemove, so ReAuthPoolHook.OnRemove never ran and Manager kept listener
// entries (ConnReAuthCredentialsListener -> *pool.Conn -> buffers).
func TestCloseConn_invokes_OnRemove_streaming_listener_registry_cleaned(t *testing.T) {
	ctx := context.Background()
	connPool := pool.NewConnPool(&pool.Options{
		Dialer: func(context.Context) (net.Conn, error) {
			return stubNetConn{}, nil
		},
		PoolSize:           10,
		PoolTimeout:        time.Minute,
		DialTimeout:        time.Second,
		MaxConcurrentDials: 10,
	})
	t.Cleanup(func() { _ = connPool.Close() })

	mgr := NewManager(connPool, time.Second)
	connPool.AddPoolHook(mgr.PoolHook())

	cn, err := connPool.NewConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = mgr.Listener(cn, func(*pool.Conn, auth.Credentials) error { return nil }, func(*pool.Conn, error) {})
	if err != nil {
		t.Fatal(err)
	}
	if n := mgr.DebugCredentialsListenerCount(); n != 1 {
		t.Fatalf("expected 1 listener in registry, got %d", n)
	}

	if err := connPool.CloseConn(ctx, cn, pool.CloseReasonTest, pool.MetricStateIdle); err != nil {
		t.Fatal(err)
	}
	if n := mgr.DebugCredentialsListenerCount(); n != 0 {
		t.Fatalf("after CloseConn, expected registry empty, got %d (OnRemove / RemoveListener not run)", n)
	}
}

func TestRemove_invokes_OnRemove_streaming_listener_registry_cleaned(t *testing.T) {
	ctx := context.Background()
	connPool := pool.NewConnPool(&pool.Options{
		Dialer: func(context.Context) (net.Conn, error) {
			return stubNetConn{}, nil
		},
		PoolSize:           10,
		PoolTimeout:        time.Minute,
		DialTimeout:        time.Second,
		MaxConcurrentDials: 10,
	})
	t.Cleanup(func() { _ = connPool.Close() })

	mgr := NewManager(connPool, time.Second)
	connPool.AddPoolHook(mgr.PoolHook())

	cn, err := connPool.NewConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = mgr.Listener(cn, func(*pool.Conn, auth.Credentials) error { return nil }, func(*pool.Conn, error) {})
	if err != nil {
		t.Fatal(err)
	}

	// NewConn does not take a pool turn; use RemoveWithoutTurn (same as background cleanup).
	connPool.RemoveWithoutTurn(ctx, cn, nil)
	if n := mgr.DebugCredentialsListenerCount(); n != 0 {
		t.Fatalf("after RemoveWithoutTurn, expected registry empty, got %d", n)
	}
}
