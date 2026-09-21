package main

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// CPA 7.3 uses literal top-level keys, not dotted paths. These optional aliases
// overlay the legacy nested YAML without exposing tokens or proxy passwords.
type panelField struct {
	Name        string
	Type        string
	Description string
	EnumValues  []string
	path        []string
	min         int
	max         int
}

var panelFields = []panelField{
	{Name: "session-guard-mode", Type: "enum", EnumValues: []string{"off", "observe", "enforce"}, Description: "会话票据来源守卫：observe 观测；enforce 剥离已知异账号票据，只允许当前账号有效票据接管。", path: []string{"session-guard-mode"}},
	{Name: "session-provenance-ttl-minutes", Type: "integer", Description: "会话与票据来源记录的内存有效期（1–1440 分钟）。", path: []string{"session-provenance-ttl-minutes"}, min: 1, max: 1440},
	{Name: "timezone", Type: "string", Description: "请求目标 IANA 时区；默认 America/Los_Angeles。与面板显示及休眠使用的 UTC+8 无关。"},
	{Name: "operation-mode", Type: "enum", EnumValues: []string{"business-only", "probe"}, Description: "运行模式：business-only 仅业务观测、不发主动探测；probe 启用探测能力（任务由探测控制台控制）。留空继承旧 YAML。"},
	{Name: "experimental-account-routing", Type: "boolean", Description: "实验性账号路由：仅在同一优先级候选中优先健康账号，按配置的长度策略、模型一致性和鉴权结果评估。关闭时仍只读记录健康证据。"},
	{Name: "accepted-state-lengths", Type: "array", Description: "可接受的 State 字节长度，JSON 整数数组；默认 [292,332]。1–32 个不同值，每个 1–4096。按实际账号配置，不根据 Team 等套餐名称推断。", path: []string{"accepted-state-lengths"}},
	{Name: "account-degraded-cooldown-minutes", Type: "integer", Description: "业务异常或鉴权失败后的路由冷却分钟数（1–1440）。不禁用 CPA 账号。", min: 1, max: 1440},
	{Name: "account-failure-cooldown-minutes", Type: "integer", Description: "连续瞬时失败后的路由冷却分钟数（1–120）。", min: 1, max: 120},
	{Name: "account-failure-threshold", Type: "integer", Description: "连续瞬时失败达到多少次后暂时避开该账号（1–10）。", min: 1, max: 10},
	{Name: "probe-account-mode", Type: "enum", EnumValues: []string{"all-accounts", "highest-priority", "fixed"}, Description: "探测账号：all-accounts 为所有可用账号独立维护票据；highest-priority 自动选择 CPA 中优先级最高的健康 Codex 账号；fixed 使用凭证文件。", path: []string{"probe", "account-mode"}},
	{Name: "probe-candidate-limit", Type: "integer", Description: "每轮允许尝试的候选账号上限（1–50）。", path: []string{"probe", "candidate-limit"}, min: 1, max: 50},
	{Name: "override-policy", Type: "enum", EnumValues: []string{"preserve-healthy-client", "always"}, Description: "覆写策略兼容旧配置；账号隔离模式只使用本账号的有效基线，不按长度信任客户端票据。"},
	{Name: "override-models", Type: "array", Description: "覆写目标模型前缀，JSON 字符串数组；留空继承旧 YAML。", path: []string{"models"}},
	{Name: "probe-models", Type: "array", Description: "主动探测模型列表；仅业务观测模式下不发探测请求。", path: []string{"probe", "models"}},
	{Name: "probe-credential-file", Type: "string", Description: "主动探测凭证文件路径（不是 Token）；仅 fixed 账号模式使用。自动选号通过 CPA Host Auth 获取凭据。", path: []string{"probe", "cred-file"}},
	{Name: "probe-proxies-file", Type: "string", Description: "已有代理出口文件路径；不要填写供应商取 IP URL。代理认证请在代理池页面管理。", path: []string{"probe", "proxies-file"}},
	{Name: "state-ttl-minutes", Type: "integer", Description: "基线有效期（分钟，1–1440）。", path: []string{"probe", "ttl-minutes"}, min: 1, max: 1440},
	{Name: "prefetch-minutes", Type: "integer", Description: "提前预备分钟数，0 关闭；必须小于有效期。探测控制台保存的运行设置优先。", path: []string{"probe", "prefetch-minutes"}, min: 0, max: 1439},
	{Name: "probe-interval-seconds", Type: "integer", Description: "串行间隔秒数（1–3600）；探测控制台保存的运行设置优先。", path: []string{"probe", "probe-interval-seconds"}, min: 1, max: 3600},
	{Name: "probe-timeout-seconds", Type: "integer", Description: "单次探测超时秒数（1–300）。", path: []string{"probe", "timeout-seconds"}, min: 1, max: 300},
	{Name: "attempts-per-proxy", Type: "integer", Description: "每个普通代理每轮基础尝试次数（1–100）。", path: []string{"probe", "attempts-per-proxy"}, min: 1, max: 100},
	{Name: "max-attempts-per-round", Type: "integer", Description: "每轮总探测上限；0 继承出口预算（0–1000000）。", path: []string{"probe", "max-attempts-per-round"}, min: 0, max: 1000000},
	{Name: "pool-attempts", Type: "integer", Description: "聚合代理每轮基础尝试预算（0–1000000）；不会购买或提取代理。", path: []string{"probe", "pool-attempts"}, min: 0, max: 1000000},
	{Name: "exit-fail-threshold", Type: "integer", Description: "普通出口连续失败熔断阈值（1–100）。", path: []string{"probe", "exit-fail-threshold"}, min: 1, max: 100},
	{Name: "exit-cooldown-minutes", Type: "integer", Description: "普通出口失败冷却分钟数（0–10080）。", path: []string{"probe", "exit-cooldown-minutes"}, min: 0, max: 10080},
	{Name: "exit-success-cooldown-minutes", Type: "integer", Description: "普通出口成功后轮休分钟数，0 关闭（0–10080）。", path: []string{"probe", "exit-success-cooldown-minutes"}, min: 0, max: 10080},
}

func visualConfigFields() []map[string]any {
	fields := make([]map[string]any, 0, len(panelFields))
	for _, f := range panelFields {
		fields = append(fields, map[string]any{"Name": f.Name, "Type": f.Type, "Description": f.Description, "EnumValues": f.EnumValues})
	}
	return fields
}

func normalizePanelConfig(input []byte) ([]byte, error) {
	var root map[string]any
	if err := yaml.Unmarshal(input, &root); err != nil {
		return nil, fmt.Errorf("invalid plugin YAML")
	}
	if root == nil {
		root = map[string]any{}
	}
	config, ok := root["turn-state-override"].(map[string]any)
	if !ok {
		if root["turn-state-override"] != nil {
			return nil, fmt.Errorf("turn-state-override must be an object")
		}
		config = map[string]any{}
	}
	probe, ok := config["probe"].(map[string]any)
	if !ok {
		if config["probe"] != nil {
			return nil, fmt.Errorf("turn-state-override.probe must be an object")
		}
		probe = map[string]any{}
	}
	config["probe"] = probe
	root["turn-state-override"] = config
	for _, f := range panelFields {
		value, present := root[f.Name]
		if !present || value == nil {
			continue
		}
		valid := false
		switch f.Type {
		case "boolean":
			_, valid = value.(bool)
		case "integer":
			n, ok := value.(int)
			valid = ok && n >= f.min && n <= f.max
		case "string":
			s, ok := value.(string)
			valid = ok && !strings.ContainsAny(s, "\r\n\x00") && !strings.Contains(s, "://")
		case "enum":
			s, ok := value.(string)
			if ok {
				for _, choice := range f.EnumValues {
					if s == choice {
						valid = true
					}
				}
			}
		case "array":
			if f.Name == "accepted-state-lengths" {
				_, err := parseStateLengths(value)
				valid = err == nil
				break
			}
			items, ok := value.([]any)
			valid = ok && len(items) > 0 && len(items) <= 32
			for _, item := range items {
				s, ok := item.(string)
				if !ok || strings.TrimSpace(s) == "" {
					valid = false
				}
			}
		}
		if !valid {
			return nil, fmt.Errorf("invalid configuration field: %s", f.Name)
		}
		if len(f.path) == 1 {
			config[f.path[0]] = value
		}
		if len(f.path) == 2 {
			probe[f.path[1]] = value
		}
	}
	if policy, ok := root["override-policy"].(string); ok {
		config["force"] = policy == "always"
	}
	if mode, ok := root["operation-mode"].(string); ok {
		config["enabled"] = true
		probe["enabled"] = mode == "probe"
		if mode == "business-only" {
			probe["prefetch-minutes"] = 0
		}
	}
	if mode, ok := root["probe-account-mode"].(string); ok && mode == "highest-priority" {
		delete(probe, "cred-file")
	}
	// Validate the final merged values, without changing legacy-only semantics.
	if value, exists := probe["account-mode"]; exists {
		mode, ok := value.(string)
		if !ok || (mode != "fixed" && mode != "highest-priority" && mode != "all-accounts") {
			return nil, fmt.Errorf("invalid probe account-mode")
		}
	}
	if root["prefetch-minutes"] != nil || root["state-ttl-minutes"] != nil {
		ttl := probeDefaultsTTLMinutes
		if n, ok := probe["ttl-minutes"].(int); ok {
			ttl = n
		}
		if n, ok := probe["prefetch-minutes"].(int); ok && n >= ttl {
			return nil, fmt.Errorf("prefetch-minutes must be less than state-ttl-minutes")
		}
	}
	lengths, err := parseStateLengths(config["accepted-state-lengths"])
	if err != nil {
		return nil, err
	}
	config["accepted-state-lengths"] = lengths
	return yaml.Marshal(root)
}
