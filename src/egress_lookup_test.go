package main

import (
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	enablePublicEgressLookup = false
	os.Exit(m.Run())
}

func TestFormatEgressLocation(t *testing.T) {
	got := formatEgressLocation("阿什本", "弗吉尼亚", "美国")
	if got != "阿什本 · 弗吉尼亚 · 美国" {
		t.Fatalf("got %q", got)
	}
	if formatEgressLocation("新加坡", "新加坡", "新加坡") != "新加坡" {
		t.Fatal("duplicate place names should collapse")
	}
	if formatEgressLocation("", "0", "德国") != "德国" {
		t.Fatal("empty placeholders should be dropped")
	}
}

func TestParsePublicEgressJSON(t *testing.T) {
	found := parsePublicEgress([]byte(`{"ip":"203.0.113.44","success":true,"city":"Ashburn","region":"Virginia","country":"United States","country_code":"US"}`))
	if found.IP != "203.0.113.44" || found.Location != "Ashburn · Virginia · 美国" {
		t.Fatalf("got %+v", found)
	}
	plain := parsePublicEgress([]byte("198.51.100.20\n"))
	if plain.IP != "198.51.100.20" || plain.Location != "" {
		t.Fatalf("plain IP %+v", plain)
	}
	if parsePublicEgress([]byte(`{"status":"fail"}`)).IP != "" {
		t.Fatal("failed geo payload must be ignored")
	}
	if parsePublicEgress([]byte(`{"ip":"10.0.0.1","country":"US"}`)).IP != "" {
		t.Fatal("private addresses must be ignored")
	}
}

func TestApplyPublicEgressPrefersLookup(t *testing.T) {
	record := probeRecord{EgressAddr: "10.0.0.1", EgressSource: "socks_bind"}
	applyPublicEgress(&record, http.DefaultClient, true)
	if record.EgressAddr != "10.0.0.1" || record.EgressSource != "socks_bind" {
		t.Fatalf("disabled lookup must keep SOCKS evidence: %+v", record)
	}
}

func TestPinProxySessionPinsRotating1024Proxy(t *testing.T) {
	spec := "socks5://user-region-US:secret@hk.1024proxy.io:3000"
	got, pinned := pinProxySession(spec)
	if !pinned {
		t.Fatal("1024proxy rotating username should be pinned")
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	user := parsed.User.Username()
	if !strings.Contains(user, "user-region-US-sid-") || !strings.HasSuffix(user, "-t-3") {
		t.Fatalf("unexpected sticky username %q", user)
	}
	pass, _ := parsed.User.Password()
	if pass != "secret" || parsed.Host != "hk.1024proxy.io:3000" {
		t.Fatalf("must keep host and password: %s", got)
	}
	again, already := pinProxySession(got)
	if !already || again != got {
		t.Fatal("already-sticky usernames must be left unchanged")
	}
}

func TestPinProxySessionLeavesOtherProxiesAlone(t *testing.T) {
	for _, spec := range []string{
		"direct",
		"socks5://user:pass@other.example:1080",
		"http://user-region-US:secret@hk.example.com:8080",
	} {
		got, pinned := pinProxySession(spec)
		if pinned || got != spec {
			t.Fatalf("%q: got %q pinned=%v", spec, got, pinned)
		}
	}
}
