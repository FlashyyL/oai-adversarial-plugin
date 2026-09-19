# AgentEase 可视化配置

CPA 插件管理 → O/对抗插件 → 配置字段。版本 `1.5.37-agentease.1`
声明 16 个顶层字段，兼容 CPA 7.3.7 的字段编辑器。

推荐仅业务观测配置：

```yaml
operation-mode: business-only
override-policy: preserve-healthy-client
override-models: [gpt-6-astra]
```

此模式不需要固定账号或静态 state，不会启动探测。没有有效基线时不注入；
真实业务流量观测到符合现有模型及 292/332 长度规则的 state 后存入基线。
长度规则是本地策略，不证明账号正常或模型能力。

`probe` 模式使用显式凭证路径，不提供自动选号。启用能力不等于开始任务；
开始/停止仍在状态路由台控制。生产使用前配置已有代理，不能填写取 IP API。

字段包括运行模式、覆写策略、模型数组、凭证/代理文件路径、TTL、预备窗口、
串行间隔、超时、每出口和每轮预算、失败阈值与冷却时间。数组填写 JSON，
例如 `["gpt-6-astra"]`。表单不提供 Token、代理密码或静态 state 输入。

已设置的顶层字段优先于旧 `turn-state-override` YAML；删除字段后恢复继承。
旧环境的表单空值表示未设置别名，不代表底层配置为空。状态路由台的
`runtime-settings.json` 对预备窗口、间隔及出口设置仍有最终优先权。
降智拒绝开关继续在状态路由台管理；时区仍为原版固定 America/Los_Angeles。

本版本修复关闭探测且静态值为空时误报“覆写：配置错误”的校验。
升级后应同时检查 `/v0/management/plugins` 返回非空 `config_fields`、插件
摘要 `turn_state_override.error` 为空、探测关闭及业务观测记录，不能只检查版本号。
