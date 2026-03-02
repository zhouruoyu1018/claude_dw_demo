# MCP Server 部署指南

本文件提供 `search-hive-metadata` 的部署与接入步骤。

## 1. 安装依赖

```bash
cd scripts/
pip install -r requirements.txt
```

## 2. 配置数据库连接

编辑 `scripts/config.yaml`：

```yaml
mysql:
  host: your-mysql-host
  port: 3306
  user: your-username
  password: your-password
  database: hive_metadata_db
```

## 3. 启动 MCP Server

```bash
python scripts/mcp_server.py
```

## 4. 配置 Claude Code

在 Claude Code 配置中添加 MCP Server：

```json
{
  "mcpServers": {
    "hive-metadata": {
      "command": "python",
      "args": ["path/to/mcp_server.py"],
      "env": {
        "MYSQL_HOST": "your-host",
        "MYSQL_USER": "your-user",
        "MYSQL_PASSWORD": "your-password"
      }
    }
  }
}
```

## 常见问题

- 连接超时：确认 MySQL 网络白名单与端口连通
- 无返回数据：检查 `tbl_base_info` 是否有数据
- 工具不可用：先单独运行 `python scripts/mcp_server.py` 看启动日志
