package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
)

func TestInterceptScopesToCodexAndReturnsRewrittenBody(t *testing.T) {
	for _, provider := range []string{"codex", "claude", "gemini", ""} {
		req := interceptRequest{RequestID: "scope-" + provider, ToFormat: provider, SourceFormat: "openai-response", Model: "gpt-5.5", Body: []byte(`{"input":"hello"}`)}
		raw, _ := json.Marshal(req)
		resp, err := intercept(raw)
		if err != nil {
			t.Fatal(err)
		}
		if provider == "codex" {
			if !bytes.Contains(resp.Body, []byte(targetTimezone)) || resp.Terminate {
				t.Error("Codex request was not normalized")
			}
		} else if len(resp.Body) > 0 || resp.Terminate {
			t.Errorf("unrelated provider modified: %s", provider)
		}
	}
}

func TestMalformedCodexRequestIsNotSentUnnormalized(t *testing.T) {
	raw, _ := json.Marshal(interceptRequest{ToFormat: "codex", Body: []byte(`invalid`)})
	resp, err := intercept(raw)
	if err != nil || !resp.Terminate || resp.StatusCode != http.StatusBadRequest || !json.Valid(resp.ResponseBody) {
		t.Fatalf("expected a bounded rejection: %+v, %v", resp, err)
	}
}

func TestAuditHistoryIsBoundedConcurrentAndDeduplicated(t *testing.T) {
	var state auditState
	var workers sync.WaitGroup
	for i := 0; i < historyLimit+30; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			record := auditRecord{RequestID: fmt.Sprintf("request-%d", i), conversion: conversion{Original: []string{}, Target: targetTimezone, Action: "inserted"}}
			state.record(record)
			state.snapshot()
		}(i)
	}
	workers.Wait()
	snapshot := state.snapshot()
	if len(snapshot["records"].([]auditRecord)) != historyLimit || snapshot["total"] != uint64(historyLimit+30) {
		t.Fatalf("incorrect bounded history: %v", snapshot["total"])
	}
	latest := snapshot["records"].([]auditRecord)[0]
	state.record(latest)
	if state.snapshot()["total"] != snapshot["total"] {
		t.Error("a retry was counted as another request")
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil || bytes.Contains(encoded, []byte(`"Body"`)) || bytes.Contains(encoded, []byte(`"Headers"`)) {
		t.Error("history must only contain conversion metadata")
	}
}

func TestDashboardAndManagementRoutesAreSeparated(t *testing.T) {
	for _, tc := range []struct {
		path, contentType string
		status            int
	}{
		{"/v0/resource/plugins/timezone-override/status", "text/html; charset=utf-8", 200},
		{"/v0/management/timezone-override/requests", "application/json; charset=utf-8", 200},
		{"/v0/resource/plugins/oai-adversarial-plugin/status", "text/html; charset=utf-8", 200},
		{"/v0/management/oai-adversarial-plugin/requests", "application/json; charset=utf-8", 200},
		{"/not-registered", "", 404},
	} {
		raw, _ := json.Marshal(managementRequest{Method: "GET", Path: tc.path})
		resp, err := management(raw)
		if err != nil || resp.StatusCode != tc.status || resp.Headers.Get("Content-Type") != tc.contentType {
			t.Fatalf("unexpected management response for %s: %+v %v", tc.path, resp, err)
		}
	}
	registration, err := handleMethod("management.register", nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(registration)
	for _, path := range []string{
		`/oai-adversarial-plugin/requests`, `/oai-adversarial-plugin/probe-control`,
		`/timezone-override/requests`, `/timezone-override/probe-control`,
		`"Path":"/status"`,
	} {
		if !bytes.Contains(encoded, []byte(path)) {
			t.Errorf("missing native CPA route: %s", path)
		}
	}
}
