package main

import (
	"bufio"
	"context"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWeeklyQuotaProxyInheritsHostOnlyWhenAbsent(t *testing.T) {
	old := weeklyQuotaHostCall
	t.Cleanup(func() { weeklyQuotaHostCall = old })
	calls := 0
	weeklyQuotaHostCall = func(method string, payload any) (json.RawMessage, error) {
		calls++
		p := payload.(map[string]any)
		if method != "host.http.do" || p["method"] != "GET" || p["url"] != weeklyQuotaURL || p["headers"].(http.Header).Get("Authorization") != "Bearer synthetic" {
			t.Fatal("quota inheritance changed host contract")
		}
		return json.Marshal(map[string]any{"StatusCode": 200, "Body": []byte(`{"quota":"synthetic"}`)})
	}
	if raw, err := weeklyQuotaHTTP(context.Background(), "", http.Header{"Authorization": {"Bearer synthetic"}}); err != nil || !strings.Contains(string(raw), "synthetic") || calls != 1 {
		t.Fatal("missing proxy did not inherit")
	}
	for _, spec := range []string{"invalid", "ftp://127.0.0.1:1", "http://user:secret@127.0.0.1:1/", "socks5h://user:secret@127.0.0.1:1"} {
		_, err := weeklyQuotaHTTP(context.Background(), spec, nil)
		if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "127.0.0.1") || calls != 1 {
			t.Fatal("explicit failed proxy leaked details or fell back to host")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := weeklyQuotaHTTP(ctx, "", nil); err == nil || calls != 1 {
		t.Fatal("canceled query reached host")
	}
}

func TestWeeklyQuotaProxyDirectBypassesEnvironmentAndRejectsRedirects(t *testing.T) {
	var unexpected atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { unexpected.Add(1); http.Error(w, "unwanted proxy", 502) }))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("ALL_PROXY", proxy.URL)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, proxy.URL, 302)
			return
		}
		w.Write([]byte("direct"))
	}))
	defer origin.Close()
	for _, spec := range []string{"direct", "none", "DiReCt"} {
		client, err := weeklyQuotaProxyClient(spec)
		if err != nil {
			t.Fatal(err)
		}
		defer client.CloseIdleConnections()
		if client.Transport.(*http.Transport).Proxy != nil {
			t.Fatal("direct still uses proxy function")
		}
		response, err := client.Get(origin.URL)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if string(body) != "direct" {
			t.Fatal("wrong direct output")
		}
		response, err = client.Get(origin.URL + "/redirect")
		if err != nil || response.StatusCode != 302 {
			t.Fatal("redirect was followed")
		}
		response.Body.Close()
	}
	if unexpected.Load() != 0 {
		t.Fatal("explicit direct used environment proxy or leaked through redirect")
	}
}

func TestWeeklyQuotaProxyHTTPAndHTTPSCONNECTAndCredentials(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "http", true: "https"}[secure], func(t *testing.T) {
			var calls atomic.Int64
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "CONNECT" || r.Host != "chatgpt.com:443" || r.Header.Get("Proxy-Authorization") != "Basic c3ludGhldGljOnNlY3JldA==" {
					t.Error("CONNECT target/auth not preserved")
				}
				calls.Add(1)
				http.Error(w, "synthetic proxy refusal", 502)
			})
			var server *httptest.Server
			if secure {
				server = httptest.NewTLSServer(handler)
			} else {
				server = httptest.NewServer(handler)
			}
			defer server.Close()
			parsed, _ := url.Parse(server.URL)
			parsed.User = url.UserPassword("synthetic", "secret")
			client, err := weeklyQuotaProxyClient(parsed.String())
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			if secure {
				roots := x509.NewCertPool()
				roots.AddCert(server.Certificate())
				client.Transport.(*http.Transport).TLSClientConfig = server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
				client.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify = false
				client.Transport.(*http.Transport).TLSClientConfig.RootCAs = roots
			}
			if _, err = client.Get(weeklyQuotaURL); err == nil || calls.Load() != 1 {
				t.Fatal("explicit CONNECT path not used")
			}
		})
	}
}

func TestWeeklyQuotaProxyCredentialEditsDiscardInFlightObservation(t *testing.T) {
	oldList, oldGet, oldCall := hostAuthListFunc, hostAuthGetFunc, weeklyQuotaHostCall
	t.Cleanup(func() { hostAuthListFunc, hostAuthGetFunc, weeklyQuotaHostCall = oldList, oldGet, oldCall })
	entry := hostAuthEntry{ID: "quota-fixture", AuthIndex: "quota-index", Provider: "codex"}
	hostAuthListFunc = func() ([]hostAuthEntry, error) { return []hostAuthEntry{entry}, nil }
	proxyURL := ""
	hostAuthGetFunc = func(string) (json.RawMessage, error) {
		return json.Marshal(map[string]any{"access_token": "synthetic-token", "account_id": "synthetic-account", "proxy_url": proxyURL})
	}
	weeklyQuotaHostCall = func(string, any) (json.RawMessage, error) {
		proxyURL = "direct"
		return json.Marshal(map[string]any{"StatusCode": 200, "Body": []byte(`{"rate_limit":{"secondary_window":{"used_percent":0,"limit_window_seconds":604800,"reset_after_seconds":3600}}}`)})
	}
	if _, err := fetchWeeklyQuota(context.Background(), entry); err == nil || err.Error() != "weekly_quota_account_unavailable" {
		t.Fatal("proxy changed during query but result accepted")
	}
	proxyURL = "  socks5h://synthetic:secret@127.0.0.1:1080/  "
	credential, _, err := weeklyCredential(entry)
	if err != nil || credential.ProxyURL != strings.TrimSpace(proxyURL) {
		t.Fatal("credential proxy not captured")
	}
}

func TestWeeklyQuotaProxySOCKSRemoteDNSAuthAndCancellation(t *testing.T) {
	for _, scheme := range []string{"socks5", "socks5h"} {
		t.Run(scheme, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan bool, 1)
			go func() {
				valid := false
				defer func() { done <- valid }()
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(2 * time.Second))
				reader := bufio.NewReader(conn)
				head := make([]byte, 2)
				if _, err = io.ReadFull(reader, head); err != nil || head[0] != 5 {
					return
				}
				if _, err = io.CopyN(io.Discard, reader, int64(head[1])); err != nil {
					return
				}
				conn.Write([]byte{5, 2})
				if _, err = io.ReadFull(reader, head); err != nil || head[0] != 1 {
					return
				}
				user := make([]byte, int(head[1]))
				if _, err = io.ReadFull(reader, user); err != nil {
					return
				}
				length, err := reader.ReadByte()
				if err != nil {
					return
				}
				password := make([]byte, int(length))
				if _, err = io.ReadFull(reader, password); err != nil {
					return
				}
				if string(user) != "synthetic" || string(password) != "secret" {
					return
				}
				conn.Write([]byte{1, 0})
				request := make([]byte, 5)
				if _, err = io.ReadFull(reader, request); err != nil || request[3] != 3 {
					return
				}
				host := make([]byte, int(request[4]))
				if _, err = io.ReadFull(reader, host); err != nil {
					return
				}
				if _, err = io.ReadFull(reader, head); err != nil {
					return
				}
				valid = string(host) == "chatgpt.com" && head[0] == 1 && head[1] == 187
				conn.Write([]byte{5, 5, 0, 1, 127, 0, 0, 1, 0, 0})
			}()
			_, err = weeklyQuotaHTTP(context.Background(), scheme+"://synthetic:secret@"+listener.Addr().String(), nil)
			if err == nil || err.Error() != "weekly_quota_query_failed" || !<-done {
				t.Fatal("SOCKS auth/domain routing or sanitized error failed")
			}
		})
	}
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
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		io.Copy(io.Discard, conn)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = weeklyQuotaHTTP(ctx, "socks5h://"+listener.Addr().String(), nil)
	if err == nil || time.Since(started) > time.Second {
		t.Fatal("stalled SOCKS handshake did not honor cancellation")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled SOCKS connection remained open")
	}
}

func TestWeeklyQuotaProxyPreservesTLSVerification(t *testing.T) {
	var requests atomic.Int64
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); w.Write([]byte("must not arrive")) }))
	defer upstream.Close()
	client, err := weeklyQuotaProxyClient("direct")
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	if _, err = client.Get(upstream.URL); err == nil || requests.Load() != 0 {
		t.Fatal("quota query accepted an untrusted TLS certificate")
	}
}
