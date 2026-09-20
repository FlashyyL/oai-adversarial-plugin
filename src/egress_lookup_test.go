package main

import (
	"net/http"
	"os"
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
	applyPublicEgress(&record, "direct", http.DefaultClient)
	if record.EgressAddr != "10.0.0.1" || record.EgressSource != "socks_bind" {
		t.Fatalf("disabled lookup must keep SOCKS evidence: %+v", record)
	}
}
