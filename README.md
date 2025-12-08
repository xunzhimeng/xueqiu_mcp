# Snowball MCP (雪球 MCP)

基于雪球API的MCP服务，让您通过Claude或其他AI助手轻松获取股票数据。

## 项目简介

本项目基于[pysnowball](https://github.com/uname-yang/pysnowball)封装了雪球API，并通过MCP协议提供服务，使您能够在Claude等AI助手中直接查询股票数据。

## 安装方法

### 方式一：在线安装（推荐）✨

无需克隆仓库，直接使用 `uvx` 运行：

```bash
# 设置环境变量
export XUEQIU_TOKEN="xq_a_token=xxxxx;u=xxxx"

# 直接运行
uvx snowball-mcp
```

### 方式二：Claude Desktop / Cursor / MCPHub 配置

在 MCP 客户端配置文件中添加：

```json
{
  "mcpServers": {
    "snowball-mcp": {
      "command": "uvx",
      "args": ["snowball-mcp"],
      "env": {
        "XUEQIU_TOKEN": "xq_a_token=xxxxx;u=xxxx"
      }
    }
  }
}
```

### 方式三：本地开发安装

```bash
# 克隆仓库
git clone https://github.com/xunzhimeng/xueqiu_mcp.git
cd xueqiu_mcp

# 使用uv安装依赖
uv pip install -e .

# 创建 .env 文件配置 token
echo 'XUEQIU_TOKEN="xq_a_token=xxxxx;u=xxxx"' > .env

# 运行
snowball-mcp
```

## 配置雪球Token

关于如何获取雪球token，请参考[pysnowball文档](https://github.com/uname-yang/pysnowball/blob/master/how_to_get_token.md)。

### 多 Token 配置（可选）

为了分散请求压力、减少限流错误，支持配置多个 Token：

```bash
# 方式一：逗号分隔
XUEQIU_TOKEN="token1,token2,token3"

# 方式二：多个环境变量
XUEQIU_TOKEN_1="token1"
XUEQIU_TOKEN_2="token2"
XUEQIU_TOKEN_3="token3"
```

多 Token 会自动轮换使用，当某个 Token 连续失败时会临时禁用并切换到下一个。

## 功能特性

- 获取股票实时行情
- 获取K线数据（支持日/周/月/季/年线和分钟级数据）
- 查询指数收益
- 获取深港通/沪港通北向数据
- 基金相关数据查询
- 关键词搜索股票代码
- 财务报表数据（利润表、资产负债表、现金流量表等）
- 资金流向数据
- 融资融券数据
- 大宗交易数据
- 机构评级和持仓数据

### 无需登录的功能

以下功能无需配置 Token 即可使用：
- `suggest_stock` - 股票搜索
- `quotec` - 基础行情
- `pankou` - 盘口数据

## 展示图

![image](./images/cursor_mcp.png)

![image](./images/claude_mcp.png)

## 致谢

本项目 fork 自 [liqiongyu/xueqiu_mcp](https://github.com/liqiongyu/xueqiu_mcp)，在此基础上进行了改进和优化。

- [liqiongyu/xueqiu_mcp](https://github.com/liqiongyu/xueqiu_mcp) - 原始项目
- [pysnowball](https://github.com/uname-yang/pysnowball) - 雪球股票数据接口的Python版本
- [fastmcp](https://github.com/jlowin/fastmcp) - MCP服务框架

## 许可证

[MIT License](./LICENSE)