package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// enablePublicEgressLookup is the production default. Tests that mock a
// SOCKS/HTTP proxy turn it off so an extra lookup connection cannot steal
// the handshake the fixture expected.
var enablePublicEgressLookup = true

type publicEgress struct {
	IP       string
	Location string
}

type publicEgressCacheEntry struct {
	value publicEgress
	until time.Time
}

var publicEgressCache sync.Map

const publicEgressCacheTTL = 60 * time.Second

var publicEgressEndpoints = []string{
	"https://ipwho.is/",
	"https://ipapi.co/json/",
	"https://ifconfig.co/json",
	"https://api.ipify.org",
}

var countryNameZH = map[string]string{
	"AU": "澳大利亚",
	"BR": "巴西",
	"CA": "加拿大",
	"CN": "中国",
	"DE": "德国",
	"ES": "西班牙",
	"FR": "法国",
	"GB": "英国",
	"HK": "香港",
	"IN": "印度",
	"IT": "意大利",
	"JP": "日本",
	"KR": "韩国",
	"NL": "荷兰",
	"PT": "葡萄牙",
	"SG": "新加坡",
	"TW": "台湾",
	"UK": "英国",
	"US": "美国",
}

func cachedPublicEgress(proxySpec string, client *http.Client) publicEgress {
	if !enablePublicEgressLookup || client == nil || os.Getenv("CPA_EGRESS_LOG_CHILD") == "1" {
		return publicEgress{}
	}
	if item, ok := publicEgressCache.Load(proxySpec); ok {
		cached := item.(publicEgressCacheEntry)
		if time.Now().Before(cached.until) {
			return cached.value
		}
	}
	found := lookupPublicEgress(client)
	if found.IP != "" {
		publicEgressCache.Store(proxySpec, publicEgressCacheEntry{
			value: found,
			until: time.Now().Add(publicEgressCacheTTL),
		})
	}
	return found
}

func lookupPublicEgress(client *http.Client) publicEgress {
	for _, rawURL := range publicEgressEndpoints {
		body, ok := fetchThrough(client, 3*time.Second, rawURL)
		if !ok {
			continue
		}
		if found := parsePublicEgress(body); found.IP != "" {
			return found
		}
	}
	return publicEgress{}
}

func parsePublicEgress(body []byte) publicEgress {
	trimmed := strings.TrimSpace(string(body))
	if ip := parsePublicIP(trimmed); ip != "" {
		return publicEgress{IP: ip}
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return publicEgress{}
	}
	if status, _ := raw["status"].(string); status != "" && status != "success" {
		return publicEgress{}
	}
	if success, ok := raw["success"].(bool); ok && !success {
		return publicEgress{}
	}
	ip := parsePublicIP(firstJSONString(raw, "ip", "query"))
	if ip == "" {
		return publicEgress{}
	}
	city := firstJSONString(raw, "city")
	region := firstJSONString(raw, "region", "regionName", "region_name")
	country := localizeCountry(
		firstJSONString(raw, "country_code", "countryCode", "country_iso"),
		firstJSONString(raw, "country", "country_name"),
	)
	return publicEgress{IP: ip, Location: formatEgressLocation(city, region, country)}
}

func parsePublicIP(value string) string {
	ip := net.ParseIP(strings.TrimSpace(value))
	if ip == nil || ip.IsUnspecified() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return ""
	}
	return ip.String()
}

func firstJSONString(raw map[string]any, keys ...string) string {
	for _, key := range keys {
		switch value := raw[key].(type) {
		case string:
			if strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		}
	}
	return ""
}

func localizeCountry(code, name string) string {
	if zh, ok := countryNameZH[strings.ToUpper(strings.TrimSpace(code))]; ok {
		return zh
	}
	return strings.TrimSpace(name)
}

func fetchThrough(client *http.Client, timeout time.Duration, rawURL string) ([]byte, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, false
	}
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	req.Header.Set("User-Agent", "cpa-timezone-egress/1.0")
	resp, err := client.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
		return nil, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return nil, false
	}
	return body, true
}

func formatEgressLocation(parts ...string) string {
	seen := map[string]bool{}
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || part == "0" || seen[part] {
			continue
		}
		seen[part] = true
		out = append(out, part)
	}
	return strings.Join(out, " · ")
}

func applyPublicEgress(record *probeRecord, proxySpec string, client *http.Client) {
	found := cachedPublicEgress(proxySpec, client)
	if found.IP == "" {
		return
	}
	record.EgressAddr = found.IP
	record.EgressLocation = found.Location
	record.EgressSource = "public"
}
