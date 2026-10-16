package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/proxy"
)

const weeklyQuotaTimeout = 20 * time.Second
const weeklyQuotaBodyLimit = 128 << 10

var weeklyQuotaHostCall = callHost

// CPA's C ABI host.http.do does not bind auth_id. Inherit it only when the
// physical credential has no proxy; explicit routes own a private transport.
func weeklyQuotaHTTP(ctx context.Context, proxyURL string, headers http.Header) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, errors.New("weekly_quota_query_failed")
	}
	if proxyURL == "" {
		raw, err := weeklyQuotaHostCall("host.http.do", map[string]any{"method": "GET", "url": weeklyQuotaURL, "headers": headers})
		if err != nil || ctx.Err() != nil {
			return nil, errors.New("weekly_quota_query_failed")
		}
		var response struct {
			StatusCode int
			Body       []byte
		}
		if json.Unmarshal(raw, &response) != nil || response.StatusCode != http.StatusOK || len(response.Body) > weeklyQuotaBodyLimit {
			return nil, errors.New("weekly_quota_query_failed")
		}
		return response.Body, nil
	}
	client, err := weeklyQuotaProxyClient(proxyURL)
	if err != nil {
		return nil, err
	}
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(ctx, weeklyQuotaTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, weeklyQuotaURL, nil)
	if err != nil {
		return nil, errors.New("weekly_quota_query_failed")
	}
	req.Header = headers.Clone()
	response, err := client.Do(req)
	if err != nil {
		return nil, errors.New("weekly_quota_query_failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("weekly_quota_query_failed")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, weeklyQuotaBodyLimit+1))
	if err != nil || len(body) > weeklyQuotaBodyLimit {
		return nil, errors.New("weekly_quota_query_failed")
	}
	return body, nil
}

func weeklyQuotaProxyClient(spec string) (*http.Client, error) {
	transport := &http.Transport{
		DialContext:       (&net.Dialer{Timeout: weeklyQuotaTimeout}).DialContext,
		ForceAttemptHTTP2: true, TLSHandshakeTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second, IdleConnTimeout: 30 * time.Second,
	}
	if !strings.EqualFold(spec, "direct") && !strings.EqualFold(spec, "none") {
		parsed, err := url.Parse(spec)
		if err != nil || parsed.Hostname() == "" || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
			return nil, errors.New("weekly_quota_proxy_invalid")
		}
		switch strings.ToLower(parsed.Scheme) {
		case "http", "https":
			transport.Proxy = http.ProxyURL(parsed)
		case "socks5", "socks5h":
			var auth *proxy.Auth
			if parsed.User != nil {
				password, _ := parsed.User.Password()
				auth = &proxy.Auth{User: parsed.User.Username(), Password: password}
			}
			dialer, err := proxy.SOCKS5("tcp", parsed.Host, auth, &net.Dialer{Timeout: weeklyQuotaTimeout})
			if err != nil {
				return nil, errors.New("weekly_quota_proxy_invalid")
			}
			contextDialer, ok := dialer.(proxy.ContextDialer)
			if !ok {
				return nil, errors.New("weekly_quota_proxy_invalid")
			}
			transport.DialContext = contextDialer.DialContext
		default:
			return nil, errors.New("weekly_quota_proxy_invalid")
		}
	}
	return &http.Client{Transport: transport, Timeout: weeklyQuotaTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}
