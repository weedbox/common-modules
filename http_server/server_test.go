package http_server

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// freePort reserves a port, releases it, and returns the number. There is a
// small window before onStart binds it, which is acceptable for tests and
// avoids hard-coding a port that a developer machine may already be using.
func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve a port: %v", err)
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

// newTestServer starts a real listener through onStart, bypassing fx. It
// returns the server and its address.
func newTestServer(t *testing.T, overrides map[string]any) (*HTTPServer, string) {
	t.Helper()

	viper.Reset()
	t.Cleanup(viper.Reset)

	// onStart writes through the package-level logger, which is normally
	// assigned by Module()'s provider.
	logger = zap.NewNop()

	hs := &HTTPServer{logger: logger, scope: "http_server"}
	hs.initDefaultConfigs()

	port := freePort(t)
	viper.Set("http_server.host", "127.0.0.1")
	viper.Set("http_server.port", port)
	viper.Set("http_server.loglevel", "prod") // no gin request logging in test output
	for k, v := range overrides {
		viper.Set("http_server."+k, v)
	}

	if err := hs.onStart(context.Background()); err != nil {
		t.Fatalf("onStart failed: %v", err)
	}
	t.Cleanup(func() { _ = hs.onStop(context.Background()) })

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))

	// ListenAndServe runs in its own goroutine; wait for the socket.
	waitForListener(t, addr)

	return hs, addr
}

func waitForListener(t *testing.T, addr string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server never started listening on %s", addr)
}

func TestServerAppliesDefaultConnectionTimeouts(t *testing.T) {
	hs, _ := newTestServer(t, nil)

	if hs.server.ReadHeaderTimeout != DefaultReadHeaderTimeout {
		t.Errorf("ReadHeaderTimeout = %v, want %v", hs.server.ReadHeaderTimeout, DefaultReadHeaderTimeout)
	}
	if hs.server.IdleTimeout != DefaultIdleTimeout {
		t.Errorf("IdleTimeout = %v, want %v", hs.server.IdleTimeout, DefaultIdleTimeout)
	}

	// These two are the regression pins, not an oversight: both are
	// whole-request deadlines, so setting either would cap SSE / streaming
	// responses and large multipart uploads. See the comment in onStart
	// before changing this.
	if hs.server.ReadTimeout != 0 {
		t.Errorf("ReadTimeout = %v, want 0 — it would cut off slow request bodies", hs.server.ReadTimeout)
	}
	if hs.server.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v, want 0 — it would cut off SSE streams", hs.server.WriteTimeout)
	}
}

func TestServerConnectionTimeoutsAreConfigurable(t *testing.T) {
	hs, _ := newTestServer(t, map[string]any{
		"read_header_timeout": "1500ms",
		"idle_timeout":        "3s",
	})

	if hs.server.ReadHeaderTimeout != 1500*time.Millisecond {
		t.Errorf("ReadHeaderTimeout = %v, want 1.5s", hs.server.ReadHeaderTimeout)
	}
	if hs.server.IdleTimeout != 3*time.Second {
		t.Errorf("IdleTimeout = %v, want 3s", hs.server.IdleTimeout)
	}
}

func TestServerConnectionTimeoutsCanBeDisabled(t *testing.T) {
	hs, _ := newTestServer(t, map[string]any{
		"read_header_timeout": 0,
		"idle_timeout":        0,
	})

	if hs.server.ReadHeaderTimeout != 0 || hs.server.IdleTimeout != 0 {
		t.Errorf("timeouts = (%v, %v), want both 0 — 0 is the documented opt-out",
			hs.server.ReadHeaderTimeout, hs.server.IdleTimeout)
	}
}

// dialAndStallHeaders opens a connection, sends a header block that is never
// terminated, and reports how the server reacted: nil once the server closed
// the connection, or the client's own deadline error if it never did.
func dialAndStallHeaders(t *testing.T, addr string, wait time.Duration) error {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	// No blank line: as far as the server is concerned the headers are still
	// coming, and this client intends to never finish them.
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n")); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(wait)); err != nil {
		t.Fatalf("failed to set read deadline: %v", err)
	}

	// Drain whatever the server sends (it may answer 408 before closing)
	// until the connection ends or our own deadline fires.
	buf := make([]byte, 512)
	for {
		if _, err = conn.Read(buf); err != nil {
			break
		}
	}
	if err == io.EOF {
		return nil
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return err // our deadline, i.e. the server is still holding the connection
	}
	return nil // connection reset / closed by the server
}

func TestServerClosesConnectionsThatNeverSendHeaders(t *testing.T) {
	_, addr := newTestServer(t, map[string]any{"read_header_timeout": "200ms"})

	// Generous multiple of the timeout: this asserts the server acts, not
	// that it acts at a precise moment.
	if err := dialAndStallHeaders(t, addr, 5*time.Second); err != nil {
		t.Fatalf("server still held a half-open connection after 5s: %v "+
			"(ReadHeaderTimeout is not in effect — each such connection pins a goroutine and an fd)", err)
	}
}

func TestServerHoldsHalfOpenConnectionsWhenDisabled(t *testing.T) {
	_, addr := newTestServer(t, map[string]any{"read_header_timeout": 0})

	// The inverse of the test above, so the opt-out is proved rather than
	// assumed: with no header deadline the connection is still there.
	if err := dialAndStallHeaders(t, addr, time.Second); err == nil {
		t.Fatal("server closed the connection with read_header_timeout=0; the documented opt-out is not working")
	}
}

func TestServerStillServesNormalRequests(t *testing.T) {
	hs, addr := newTestServer(t, map[string]any{"read_header_timeout": "200ms"})

	hs.router.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

	resp, err := http.Get("http://" + addr + "/ping")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "pong" {
		t.Fatalf("got %d %q, want 200 \"pong\"", resp.StatusCode, body)
	}
}
