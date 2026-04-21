package redis

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/auth/streaming"
	"github.com/redis/go-redis/v9/internal/pool"
)

// stubLeakTestConn is a minimal net.Conn for pool tests; it avoids hitting OS
// syscalls and is safe to use as a dial target for the ConnPool's Dialer when
// the test only exercises code paths that do not require protocol I/O.
type stubLeakTestConn struct{}

func (stubLeakTestConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (stubLeakTestConn) Write(p []byte) (int, error)      { return len(p), nil }
func (stubLeakTestConn) Close() error                     { return nil }
func (stubLeakTestConn) LocalAddr() net.Addr              { return nil }
func (stubLeakTestConn) RemoteAddr() net.Addr             { return nil }
func (stubLeakTestConn) SetDeadline(time.Time) error      { return nil }
func (stubLeakTestConn) SetReadDeadline(time.Time) error  { return nil }
func (stubLeakTestConn) SetWriteDeadline(time.Time) error { return nil }

// leakTestCreds is a minimal auth.Credentials for streaming-credentials tests.
type leakTestCreds struct{ user, pass string }

func (c leakTestCreds) BasicAuth() (string, string) { return c.user, c.pass }
func (c leakTestCreds) RawCredentials() string      { return c.user + ":" + c.pass }

// leakTestStreamingProvider is a minimal auth.StreamingCredentialsProvider that
// tracks live subscribers so the test can assert the provider's own bookkeeping
// is drained when connections are closed.
type leakTestStreamingProvider struct {
	subscribers map[auth.CredentialsListener]struct{}
}

func (p *leakTestStreamingProvider) Subscribe(l auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	if p.subscribers == nil {
		p.subscribers = make(map[auth.CredentialsListener]struct{})
	}
	p.subscribers[l] = struct{}{}
	unsub := func() error {
		delete(p.subscribers, l)
		return nil
	}
	return leakTestCreds{user: "u", pass: "p"}, unsub, nil
}

// TestBaseClient_SubscribeStreamingCredentials_DoesNotChainOnClose is a
// regression test for a slow memory leak where the StreamingCredentialsProvider
// branch of baseClient.initConn wrapped baseClient.onClose with each new pool
// connection's unsubscribe closure. Each wrapping captured the previous
// c.onClose AND the new unsubscribe; the latter captured a per-connection
// ConnReAuthCredentialsListener holding *pool.Conn and therefore its ~64 KiB
// of bufio read/write buffers plus TLS handshake state.
//
// For a long-lived client with normal pool churn (e.g. ConnMaxIdleTime
// evictions, re-auth error reconnects), this chain accumulated one layer per
// connection ever dialed and only unwound on client shutdown — producing a
// slow heap leak that became visible once streaming credentials providers
// (Entra ID, OAuth, …) were enabled.
//
// The fix moved the streaming-credentials setup into the
// baseClient.subscribeStreamingCredentials helper, which wires only the
// per-connection cn.SetOnClose(unsub) and must never mutate baseClient.onClose.
// Connection-level cleanup runs via cn.Close() → cn.onClose(); client-level
// cleanup runs via connPool.Close() iterating every pooled conn.
//
// The test asserts:
//  1. baseClient.onClose is NEVER mutated by the streaming-credentials path
//     across many subscribe/close cycles.
//  2. Per-connection close cleans up both the streaming.Manager listener
//     registry and the provider's subscribers set.
func TestBaseClient_SubscribeStreamingCredentials_DoesNotChainOnClose(t *testing.T) {
	ctx := context.Background()

	pp := pool.NewConnPool(&pool.Options{
		Dialer: func(context.Context) (net.Conn, error) {
			return stubLeakTestConn{}, nil
		},
		PoolSize:           100,
		PoolTimeout:        time.Second,
		DialTimeout:        time.Second,
		MaxConcurrentDials: 100,
	})
	t.Cleanup(func() { _ = pp.Close() })

	mgr := streaming.NewManager(pp, time.Second)
	pp.AddPoolHook(mgr.PoolHook())

	provider := &leakTestStreamingProvider{}
	c := &baseClient{
		opt: &Options{
			StreamingCredentialsProvider: provider,
		},
		connPool:                    pp,
		streamingCredentialsManager: mgr,
	}

	const iterations = 50
	for i := 0; i < iterations; i++ {
		cn, err := pp.NewConn(ctx)
		if err != nil {
			t.Fatalf("NewConn[%d]: %v", i, err)
		}

		user, pass, err := c.subscribeStreamingCredentials(cn)
		if err != nil {
			t.Fatalf("subscribeStreamingCredentials[%d]: %v", i, err)
		}
		if user != "u" || pass != "p" {
			t.Fatalf("iteration %d: unexpected basic auth %q/%q from provider", i, user, pass)
		}

		// Close the connection individually: this fires cn.onClose, which
		// invokes the unsubscribe captured in cn.SetOnClose.
		if err := pp.CloseConn(ctx, cn, pool.CloseReasonTest, pool.MetricStateIdle); err != nil {
			t.Fatalf("CloseConn[%d]: %v", i, err)
		}

		// Invariant: the streaming-credentials path MUST NOT mutate
		// baseClient.onClose per-connection. If this assertion fails, the
		// closure-chain retention leak has been reintroduced — see the
		// comment on subscribeStreamingCredentials and the commit that
		// added this test.
		if c.onClose != nil {
			t.Fatalf("baseClient.onClose was set by streaming-credentials path on iteration %d; "+
				"this reintroduces the closure-chain retention leak that the fix removed", i)
		}
	}

	if n := mgr.DebugCredentialsListenerCount(); n != 0 {
		t.Fatalf("streaming.Manager registry leaked: want 0 listeners after all conns closed, got %d", n)
	}
	if n := len(provider.subscribers); n != 0 {
		t.Fatalf("StreamingCredentialsProvider subscribers leaked: want 0 after all conns closed, got %d", n)
	}
}
