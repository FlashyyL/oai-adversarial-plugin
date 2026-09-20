package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// A loopback SOCKS proxy reports a different bound address per connection.
// It keeps HTTP connections alive, so reuse would repeat the first address.
func TestProbeAttemptUsesFreshSOCKSConnectionAndBoundAddress(t *testing.T) {
	// Capture a child process instead of racing unrelated goroutines by
	// replacing the process-wide os.Stderr pointer.
	if os.Getenv("CPA_EGRESS_LOG_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProbeAttemptUsesFreshSOCKSConnectionAndBoundAddress$")
		cmd.Env = append(os.Environ(), "CPA_EGRESS_LOG_CHILD=1", "LKS_MIRROR_URL=")
		logs, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("isolated probe diagnostics failed: %v\n%s", err, logs)
		}
		if strings.Count(string(logs), "[INFO] - probe-egress ") != 3 ||
			strings.Count(string(logs), "reason=proxy_reported_unverified") != 2 ||
			!strings.Contains(string(logs), "reason=proxy_reported_unspecified") ||
			!strings.Contains(string(logs), "recorded=false http_status=200 success=true") {
			t.Fatalf("missing per-attempt diagnostic: %s", logs)
		}
		for _, secret := range []string{"203.0.113.10", "203.0.113.11", "socks5h://", "mock-only"} {
			if strings.Contains(string(logs), secret) {
				t.Fatal("diagnostic exposed an address or credential")
			}
		}
		return
	}
	enablePublicEgressLookup = false
	e := newPrefetchTestEngine(t)
	if err := os.WriteFile(e.cfg.Config.CredFile, []byte(`{"access_token":"mock-only"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var connections []net.Conn
	var workers sync.WaitGroup
	workers.Add(1)
	addresses := []string{"203.0.113.10", "203.0.113.11", "0.0.0.0"}
	state := synthStateToken(time.Now())
	go func() {
		defer workers.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			index := len(connections)
			connections = append(connections, conn)
			mu.Unlock()
			workers.Add(1)
			go func(index int, conn net.Conn) {
				defer workers.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				if index >= len(addresses) {
					return
				}
				header := make([]byte, 2)
				if _, err := io.ReadFull(conn, header); err != nil || header[0] != 5 {
					return
				}
				if _, err := io.CopyN(io.Discard, conn, int64(header[1])); err != nil {
					return
				}
				if _, err := conn.Write([]byte{5, 0}); err != nil {
					return
				}
				request := make([]byte, 5)
				if _, err := io.ReadFull(conn, request); err != nil || request[3] != 3 {
					return
				}
				if _, err := io.CopyN(io.Discard, conn, int64(request[4])+2); err != nil {
					return
				}
				reply := append([]byte{5, 0, 0, 1}, net.ParseIP(addresses[index]).To4()...)
				reply = append(reply, 0, 80)
				if _, err := conn.Write(reply); err != nil {
					return
				}
				reader := bufio.NewReader(conn)
				for {
					req, err := http.ReadRequest(reader)
					if err != nil {
						return
					}
					_, _ = io.Copy(io.Discard, req.Body)
					_ = req.Body.Close()
					// A mismatch is still a completed connection with address evidence.
					model := "gpt-6-astra"
					if index == 1 {
						model = "gpt-5.6-luna"
					}
					body := fmt.Sprintf("data: {\"type\":\"response.created\",\"response\":{\"model\":%q}}\n\n", model)
					_, err = fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n%s: %s\r\nContent-Type: text/event-stream\r\n\r\n%s", len(body), turnStateHeader, state, body)
					if err != nil {
						return
					}
				}
			}(index, conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	cfg := e.cfg.Config
	cfg.UpstreamURL = "http://probe.example.test/responses" // handled entirely by the loopback mock
	cfg.Timeout = 3 * time.Second
	proxy := "socks5h://" + listener.Addr().String()
	for i, expected := range []string{"203.0.113.10", "203.0.113.11", ""} {
		record, _ := e.probeOnce("gpt-6-astra", proxy, cfg)
		if record.EgressAddr != expected {
			t.Fatalf("attempt %d: bound address %q, want %q", i, record.EgressAddr, expected)
		}
		if i == 1 {
			if record.Success || !strings.Contains(record.Error, "model mismatch") {
				t.Fatal("a failed model check should retain the connection's address")
			}
		} else if !record.Success {
			t.Fatalf("attempt %d: unexpected failure: %s", i, record.Error)
		}
	}
	mu.Lock()
	count := len(connections)
	mu.Unlock()
	if count != 3 {
		t.Fatalf("got %d SOCKS connections for 3 probes", count)
	}
}

func TestSOCKSEgressDiagnosticReplies(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		reply                  []byte
		stage, kind, errorKind string
		code                   int
	}{
		{"ipv4_zero", []byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 80}, "complete", "unspecified", "", 0},
		{"ipv6_zero", append([]byte{5, 0, 0, 4}, make([]byte, 18)...), "complete", "unspecified", "", 0},
		{"empty_domain", []byte{5, 0, 0, 3, 0, 0, 80}, "complete", "empty", "", 0},
		{"private", []byte{5, 0, 0, 1, 10, 0, 0, 1, 0, 80}, "complete", "non_public", "", 0},
		{"refused", []byte{5, 5, 0, 1}, "connect_reply", "not_received", "protocol_or_io", 5},
		{"truncated", []byte{5, 0, 0, 1, 1}, "bound_address", "not_received", "truncated_reply", 0},
		{"bad_type", []byte{5, 0, 0, 9}, "bound_address", "not_received", "protocol_or_io", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(3 * time.Second))
				greeting := make([]byte, 3)
				if _, err := io.ReadFull(conn, greeting); err != nil {
					return
				}
				conn.Write([]byte{5, 0})
				request := make([]byte, 5)
				if _, err := io.ReadFull(conn, request); err != nil {
					return
				}
				if _, err := io.CopyN(io.Discard, conn, int64(request[4])+2); err != nil {
					return
				}
				conn.Write(tc.reply)
			}()
			binder := &socksBind{host: listener.Addr().String()}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			conn, err := binder.dialContext(ctx, "tcp", "upstream.example.test:443")
			if conn != nil {
				conn.Close()
			}
			<-done
			diagnostic, dials := binder.diagnostics()
			if (err != nil) != (tc.errorKind != "") || dials != 1 || diagnostic.Stage != tc.stage || diagnostic.AddressKind != tc.kind || diagnostic.Error != tc.errorKind || diagnostic.Reply != tc.code {
				t.Fatalf("unexpected diagnostic: %+v dials=%d err=%v", diagnostic, dials, err)
			}
		})
	}
}

func TestProbeEgressDiagnosticProtocolAndPrivacy(t *testing.T) {
	for _, tc := range []struct {
		spec, reason string
		binder       *socksBind
	}{
		{"http://secret-user:secret-pass@proxy.example:8080", "http_no_standard_exit_field", nil},
		{"https://secret-user:secret-pass@proxy.example:443", "http_no_standard_exit_field", nil},
		{"direct", "direct_no_proxy_report", nil},
		{"socks5h://secret-user:secret-pass@proxy.example:1080", "dial_not_started", &socksBind{}},
		{"socks5h://proxy.example:1080", "dial_pending_at_return", &socksBind{dials: 1, diagnostic: socksDiagnostic{Stage: "in_progress"}}},
	} {
		line := probeEgressLog(probeRecord{Time: "2026-09-19T00:00:00Z", Model: "test\nmodel", Error: "secret-token", EgressAddr: "203.0.113.42"}, "exit-test", tc.spec, tc.binder, "roundtrip")
		if !strings.Contains(line, "reason="+tc.reason) || strings.Count(line, "\n") != 1 {
			t.Fatalf("bad diagnostic: %s", line)
		}
		for _, secret := range []string{"secret-user", "secret-pass", "proxy.example", "secret-token", "203.0.113.42"} {
			if strings.Contains(line, secret) {
				t.Fatal("diagnostic leaked sensitive input")
			}
		}
	}
}
