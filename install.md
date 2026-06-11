# 合同审核插件安装指南

## 前置条件

- 已安装 Hermes Agent
- 至少配置了一个消息通道（飞书 / 微信），否则插件无法推送通知

## 安装步骤

### 1. 放置插件

将 `contract_review` 目录放到 Hermes 插件目录下：

```bash
# Hermes 插件目录通常在 ~/.hermes/plugins/
cp -r contract_review ~/.hermes/plugins/
```

### 2. 放置技能文件

将 `SKILL.md` 放到 Hermes 技能目录：

```bash
# 技能目录通常在 ~/.hermes/skills/contract_review/
mkdir -p ~/.hermes/skills/contract_review
cp SKILL.md ~/.hermes/skills/contract_review/
```

### 3. 配置环境变量

在 Hermes 的环境配置（`.env` 或对应配置渠道）中添加以下变量：

```env
# 后端服务地址（根据实际服务器 IP 修改）
CONTRACT_REVIEW_A2A_BASE_URL=http://10.10.1.98:8080/a2a
CONTRACT_REVIEW_AUTH_BASE_URL=http://10.10.1.98:3001
CONTRACT_REVIEW_AGENT_REST_BASE_URL=http://10.10.1.98:8100
```

> 注意：如果后端地址变更，修改以上 IP 部分即可。

### 4. 重启 Hermes

完成以上步骤后重启 Hermes Agent 使配置生效,并使能插件。
