import os
import time
import threading
import pysnowball as ball
from fastmcp import FastMCP
from dotenv import load_dotenv
import datetime
import json

load_dotenv()


# ==================== Token 轮换管理器 ====================

class TokenRotator:
    """多 Token 轮换管理器
    
    机制：
    1. 支持多个 Token（逗号分隔或多个环境变量）
    2. 轮换使用 Token（Round-Robin）
    3. 仅当有多个 Token 时，失败的 Token 才会临时禁用
    """
    
    def __init__(self, cooldown_seconds: float = 60.0, max_failures: int = 3):
        self.tokens = []
        self.current_index = 0
        self.cooldown_seconds = cooldown_seconds
        self.max_failures = max_failures
        self.token_status = {}  # {token: {'failures': int, 'disabled_until': float}}
        self._lock = threading.Lock()
        self._load_tokens()
    
    def _load_tokens(self):
        """从环境变量加载 Token"""
        # 方式1: 逗号分隔的单一变量
        tokens_str = os.getenv("XUEQIU_TOKEN", "")
        if ',' in tokens_str:
            self.tokens = [t.strip() for t in tokens_str.split(',') if t.strip()]
        elif tokens_str:
            self.tokens = [tokens_str]
        
        # 方式2: 多个变量 XUEQIU_TOKEN_1, XUEQIU_TOKEN_2 ...
        for i in range(1, 10):
            token = os.getenv(f"XUEQIU_TOKEN_{i}")
            if token and token not in self.tokens:
                self.tokens.append(token)
        
        # 初始化状态
        for token in self.tokens:
            self.token_status[token] = {'failures': 0, 'disabled_until': 0.0}
        
        if self.tokens:
            print(f"[TokenRotator] 已加载 {len(self.tokens)} 个 Token")
        else:
            print("[TokenRotator] 警告: 未配置任何 Token，部分功能将不可用")
    
    def get_next_token(self) -> str | None:
        """获取下一个可用的 Token"""
        if not self.tokens:
            return None
        
        with self._lock:
            current_time = time.time()
            
            # 如果只有一个 Token，直接返回，不做禁用检查
            if len(self.tokens) == 1:
                return self.tokens[0]
            
            # 多个 Token 时，查找可用的
            for _ in range(len(self.tokens)):
                token = self.tokens[self.current_index]
                status = self.token_status[token]
                
                # 检查是否在冷却期
                if current_time >= status['disabled_until']:
                    # 可用，移动到下一个索引
                    self.current_index = (self.current_index + 1) % len(self.tokens)
                    return token
                
                # 当前 Token 不可用，尝试下一个
                self.current_index = (self.current_index + 1) % len(self.tokens)
            
            # 所有 Token 都在冷却期，返回第一个（强制使用）
            print("[TokenRotator] 所有 Token 都在冷却期，强制使用第一个")
            return self.tokens[0]
    
    def report_failure(self, token: str):
        """报告 Token 失败"""
        if not token or token not in self.token_status:
            return
        
        with self._lock:
            # 只有多个 Token 时才禁用
            if len(self.tokens) <= 1:
                return
            
            status = self.token_status[token]
            status['failures'] += 1
            
            if status['failures'] >= self.max_failures:
                status['disabled_until'] = time.time() + self.cooldown_seconds
                print(f"[TokenRotator] Token 连续失败 {status['failures']} 次，禁用 {self.cooldown_seconds} 秒")
    
    def report_success(self, token: str):
        """报告 Token 成功，重置失败计数"""
        if not token or token not in self.token_status:
            return
        
        with self._lock:
            self.token_status[token]['failures'] = 0
    
    def apply_token(self) -> str | None:
        """获取下一个 Token 并应用到 pysnowball"""
        token = self.get_next_token()
        if token:
            ball.set_token(token)
        return token


# 全局 Token 轮换器
_token_rotator = TokenRotator(cooldown_seconds=60.0, max_failures=3)


# ==================== 自适应请求频率限制器 ====================

class AdaptiveRateLimiter:
    """自适应请求频率限制器
    
    机制：
    1. 初始间隔为 min_interval (默认1.5s)
    2. 遇到错误时，调用 backoff() 增加间隔 (x1.2)，最大不超过 max_interval (8.0s)
    3. 如果一段时间 (recovery_timeout) 没有请求，自动恢复到 min_interval
    """
    
    def __init__(self, min_interval: float = 1.5, max_interval: float = 8.0, recovery_timeout: float = 60.0):
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.current_interval = min_interval
        self.recovery_timeout = recovery_timeout
        self.last_request_time = 0.0
        self._lock = threading.Lock()
    
    def wait(self):
        """等待直到可以发起下一个请求"""
        with self._lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            
            # 如果距离上次请求已经过了恢复期，重置限制
            if time_since_last > self.recovery_timeout:
                self.current_interval = self.min_interval
            
            # 计算需要等待的时间
            if time_since_last < self.current_interval:
                sleep_time = self.current_interval - time_since_last
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()

    def backoff(self):
        """触发退避机制，增加等待间隔"""
        with self._lock:
            self.current_interval = min(self.current_interval * 1.2, self.max_interval)
            print(f"[RateLimit] 触发限流退避，当前间隔: {self.current_interval:.2f}s")


# 全局自适应限流器（增加了间隔时间）
_rate_limiter = AdaptiveRateLimiter(min_interval=1.5, max_interval=8.0, recovery_timeout=60.0)


def rate_limited_call(func, *args, **kwargs):
    """带限流的 API 调用包装函数，包含 Token 轮换、错误处理和自动重试"""
    _rate_limiter.wait()
    
    # 应用下一个可用 Token
    current_token = _token_rotator.apply_token()
    
    try:
        result = func(*args, **kwargs)
        # 成功时报告
        if current_token:
            _token_rotator.report_success(current_token)
        return result
    except Exception as e:
        # 遇到异常，触发退避
        _rate_limiter.backoff()
        
        # 报告 Token 失败
        if current_token:
            _token_rotator.report_failure(current_token)
        
        # 打印日志
        print(f"[Retry] 请求失败: {e}，将在 2 秒后重试...")
        
        # 重试机制：等待 2 秒后重试一次
        time.sleep(2.0)
        
        # 尝试使用下一个 Token
        retry_token = _token_rotator.apply_token()
        
        try:
            result = func(*args, **kwargs)
            if retry_token:
                _token_rotator.report_success(retry_token)
            return result
        except Exception as retry_e:
            # 重试依然失败
            if retry_token:
                _token_rotator.report_failure(retry_token)
            
            e = retry_e
            
            # 处理 pysnowball 抛出的异常，通常是 bytes 类型的响应内容
            if hasattr(e, 'args') and e.args and isinstance(e.args[0], bytes):
                try:
                    error_data = json.loads(e.args[0].decode('utf-8'))
                    # 检查是否是 token 失效错误 (400016)
                    if error_data.get('error_code') == '400016' or \
                       '重新登录' in error_data.get('error_description', ''):
                        raise ValueError(
                            "🔴 雪球 API Token 失效 (错误码: 400016)\n"
                            "错误信息: 遇到错误，请刷新页面或者重新登录帐号后再试\n"
                            "解决方案: 请更新 XUEQIU_TOKEN 环境变量\n"
                            "获取方式: https://github.com/uname-yang/pysnowball/blob/master/how_to_get_token.md"
                        )
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass
            raise e


mcp = FastMCP(
    name="Snowball MCP",
    instructions="""你是一个中国股票市场数据助手，通过雪球(Xueqiu/Snowball)API获取股票、基金、指数等金融数据。

## 股票代码格式
- A股：SZ000002（深圳）、SH600000（上海）
- 港股：HK00700
- 美股：AAPL、GOOGL

## 常用功能
- 实时行情：quotec, quote_detail, pankou
- K线数据：kline（支持日/周/月/分钟级别）
- 财务数据：income（利润表）、balance（资产负债表）、cash_flow（现金流量表）
- 资金流向：capital_flow, capital_history
- 指数数据：index_basic_info, index_weight_top10
- 基金数据：fund_detail, fund_nav_history
- 北向资金：northbound_shareholding_sh, northbound_shareholding_sz
- 搜索股票：suggest_stock

## 无需登录的功能
- suggest_stock（股票搜索）
- quotec（基础行情）
- pankou（盘口数据）

## 注意事项
- 使用前需确保 XUEQIU_TOKEN 环境变量已正确设置
- 支持多 Token 配置：XUEQIU_TOKEN=token1,token2 或 XUEQIU_TOKEN_1, XUEQIU_TOKEN_2
- 数据来源于雪球，仅供参考，不构成投资建议
"""
)


def convert_timestamps(data):
    """递归地将数据中的所有 timestamp 转换为 datetime 字符串"""
    if isinstance(data, dict):
        for key, value in list(data.items()):
            if key == 'timestamp' and isinstance(value, (int, float)) and value > 1000000000000:  # 毫秒级时间戳
                data[key] = datetime.datetime.fromtimestamp(value/1000).strftime('%Y-%m-%d %H:%M:%S')
            elif key == 'timestamp' and isinstance(value, (int, float)) and value > 1000000000:  # 秒级时间戳
                data[key] = datetime.datetime.fromtimestamp(value).strftime('%Y-%m-%d %H:%M:%S')
            elif key.endswith('_date') and isinstance(value, (int, float)) and value > 1000000000000:  # 毫秒级时间戳
                data[key] = datetime.datetime.fromtimestamp(value/1000).strftime('%Y-%m-%d %H:%M:%S')
            elif key.endswith('_date') and isinstance(value, (int, float)) and value > 1000000000:  # 秒级时间戳
                data[key] = datetime.datetime.fromtimestamp(value).strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(value, (dict, list)):
                data[key] = convert_timestamps(value)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            data[i] = convert_timestamps(item)
    return data


def process_data(data, process_config=None):
    """
    通用数据处理函数，可扩展添加各种数据处理操作
    
    Args:
        data: 原始数据
        process_config: 处理配置字典，用于指定要执行的处理操作
            例如: {'convert_timestamps': True, 'other_process': params}
    
    Returns:
        处理后的数据
    """
    if process_config is None:
        # 默认配置
        process_config = {
            'convert_timestamps': True
        }
    
    # 如果开启了时间戳转换
    if process_config.get('convert_timestamps', True):
        data = convert_timestamps(data)
    
    return data


# ==================== 无需 Token 的工具（放在最前面）====================

@mcp.tool()
def suggest_stock(keyword: str = "茅台") -> dict:
    """【无需登录】关键词搜索股票代码
    
    Args:
        keyword: 搜索关键词，如股票名称、代码等
    """
    result = rate_limited_call(ball.suggest_stock, keyword)
    return process_data(result)


@mcp.tool()
def quotec(stock_code: str = "SZ000002") -> dict:
    """【无需登录】获取股票行情数据
    
    Args:
        stock_code: 股票代码，如 SZ000002、SH600000
    """
    result = rate_limited_call(ball.quotec, stock_code)
    return process_data(result)


@mcp.tool()
def pankou(stock_code: str = "SZ000002") -> dict:
    """【无需登录】获取实时盘口数据，包含买卖五档报价
    
    Args:
        stock_code: 股票代码
    """
    result = rate_limited_call(ball.pankou, stock_code)
    return process_data(result)


# ==================== 需要 Token 的工具 ====================

@mcp.tool()
def quote_detail(stock_code: str = "SZ000002") -> dict:
    """获取股票行情详细数据"""
    result = rate_limited_call(ball.quote_detail, stock_code)
    return process_data(result)


@mcp.tool()
def kline(stock_code: str = "SZ000002", period: str = "day", count: int = 284) -> dict:
    """获取K线数据
    
    Args:
        stock_code: 股票代码，例如 SZ000002
        period: K线周期，可选值：day（日线）、week（周线）、month（月线）、quarter（季线）、year（年线）、
                120m（120分钟）、60m（60分钟）、30m（30分钟）、15m（15分钟）、5m（5分钟）、1m（1分钟）
        count: 返回数据数量，默认284条
    """
    result = rate_limited_call(ball.kline, stock_code, period=period, count=count)
    return process_data(result)


@mcp.tool()
def earningforecast(stock_code: str = "SZ000002") -> dict:
    """按年度获取业绩预告数据"""
    result = rate_limited_call(ball.earningforecast, stock_code)
    return process_data(result)


@mcp.tool()
def report(stock_code: str = "SZ000002") -> dict:
    """获取机构评级数据"""
    result = rate_limited_call(ball.report, stock_code)
    return process_data(result)


@mcp.tool()
def capital_flow(stock_code: str = "SZ000002") -> dict:
    """获取当日资金流如流出数据，每分钟数据"""
    result = rate_limited_call(ball.capital_flow, stock_code)
    return process_data(result)


@mcp.tool()
def capital_history(stock_code: str = "SZ000002") -> dict:
    """获取历史资金流如流出数据，每日数据"""
    result = rate_limited_call(ball.capital_history, stock_code)
    return process_data(result)


@mcp.tool()
def capital_assort(stock_code: str = "SZ000002") -> dict:
    """获取资金成交分布数据"""
    result = rate_limited_call(ball.capital_assort, stock_code)
    return process_data(result)


@mcp.tool()
def blocktrans(stock_code: str = "SZ000002") -> dict:
    """获取大宗交易数据"""
    result = rate_limited_call(ball.blocktrans, stock_code)
    return process_data(result)


@mcp.tool()
def margin(stock_code: str = "SZ000002") -> dict:
    """获取融资融券数据"""
    result = rate_limited_call(ball.margin, stock_code)
    return process_data(result)


@mcp.tool()
def indicator(stock_code: str = "SZ000002", is_annals: int = 1, count: int = 5) -> dict:
    """按年度、季度获取业绩报表数据
    
    Args:
        stock_code: 股票代码
        is_annals: 只获取年报,默认为1
        count: 返回数据数量,默认5条
    """
    result = rate_limited_call(ball.indicator, symbol=stock_code, is_annals=is_annals, count=count)
    return process_data(result)


@mcp.tool()
def income(stock_code: str = "SZ000002", is_annals: int = 1, count: int = 5) -> dict:
    """获取利润表数据
    
    Args:
        stock_code: 股票代码
        is_annals: 只获取年报,默认为1
        count: 返回数据数量,默认5条
    """
    result = rate_limited_call(ball.income, symbol=stock_code, is_annals=is_annals, count=count)
    return process_data(result)


@mcp.tool()
def balance(stock_code: str = "SZ000002", is_annals: int = 1, count: int = 5) -> dict:
    """获取资产负债表数据
    
    Args:
        stock_code: 股票代码
        is_annals: 只获取年报,默认为1
        count: 返回数据数量,默认5条
    """
    result = rate_limited_call(ball.balance, symbol=stock_code, is_annals=is_annals, count=count)
    return process_data(result)


@mcp.tool()
def cash_flow(stock_code: str = "SZ000002", is_annals: int = 1, count: int = 5) -> dict:
    """获取现金流量表数据
    
    Args:
        stock_code: 股票代码
        is_annals: 只获取年报,默认为1
        count: 返回数据数量,默认5条
    """
    result = rate_limited_call(ball.cash_flow, symbol=stock_code, is_annals=is_annals, count=count)
    return process_data(result)


@mcp.tool()
def business(stock_code: str = "SZ000002", count: int = 5) -> dict:
    """获取主营业务构成数据
    
    Args:
        stock_code: 股票代码
        count: 返回数据数量,默认5条
    """
    result = rate_limited_call(ball.business, symbol=stock_code, count=count)
    return process_data(result)


@mcp.tool()
def top_holders(stock_code: str = "SZ000002", circula: int = 1) -> dict:
    """获取十大股东数据
    
    Args:
        stock_code: 股票代码
        circula: 只获取流通股,默认为1
    """
    result = rate_limited_call(ball.top_holders, symbol=stock_code, circula=circula)
    return process_data(result)


@mcp.tool()
def main_indicator(stock_code: str = "SZ000002") -> dict:
    """获取F10主要指标数据"""
    result = rate_limited_call(ball.main_indicator, stock_code)
    return process_data(result)


@mcp.tool()
def holders(stock_code: str = "SZ000002") -> dict:
    """获取F10股东人数数据"""
    result = rate_limited_call(ball.holders, stock_code)
    return process_data(result)


@mcp.tool()
def org_holding_change(stock_code: str = "SZ000002") -> dict:
    """获取F10机构持仓数据"""
    result = rate_limited_call(ball.org_holding_change, stock_code)
    return process_data(result)


@mcp.tool()
def bonus(stock_code: str = "SZ000002", page: int = 1, size: int = 10) -> dict:
    """获取F10分红融资数据
    
    Args:
        stock_code: 股票代码
        page: 第几页 默认1
        size: 每页含有多少数据 默认10
    """
    result = rate_limited_call(ball.bonus, stock_code, page=page, size=size)
    return process_data(result)


@mcp.tool()
def industry_compare(stock_code: str = "SZ000002") -> dict:
    """获取F10行业对比数据"""
    result = rate_limited_call(ball.industry_compare, stock_code)
    return process_data(result)


@mcp.tool()
def watch_list() -> dict:
    """获取用户自选列表"""
    result = rate_limited_call(ball.watch_list)
    return process_data(result)


@mcp.tool()
def watch_stock(pid: int) -> dict:
    """获取用户自选列表详情
    
    Args:
        pid: 自选列表ID
    """
    result = rate_limited_call(ball.watch_stock, pid)
    return process_data(result)


@mcp.tool()
def nav_daily(cube_symbol: str = "SZ000002") -> dict:
    """获取组合净值数据
    
    Args:
        cube_symbol: 组合代码
    """
    result = rate_limited_call(ball.nav_daily, cube_symbol)
    return process_data(result)


@mcp.tool()
def rebalancing_history(cube_symbol: str = "SZ000002") -> dict:
    """获取组合历史交易信息
    
    Args:
        cube_symbol: 组合代码
    """
    result = rate_limited_call(ball.rebalancing_history, cube_symbol)
    return process_data(result)


@mcp.tool()
def convertible_bond(page_size: int = 5, page_count: int = 1) -> dict:
    """获取可转债信息
    
    Args:
        page_size: 每页显示数量
        page_count: 页码
    """
    result = rate_limited_call(ball.convertible_bond, page_size=page_size, page_count=page_count)
    return process_data(result)


@mcp.tool()
def index_basic_info(index_code: str = "SZ000002") -> dict:
    """获取指数基本信息
    
    Args:
        index_code: 指数代码
    """
    result = rate_limited_call(ball.index_basic_info, index_code)
    return process_data(result)


@mcp.tool()
def index_details_data(index_code: str = "SZ000002") -> dict:
    """获取指数详细信息
    
    Args:
        index_code: 指数代码
    """
    result = rate_limited_call(ball.index_details_data, index_code)
    return process_data(result)


@mcp.tool()
def index_weight_top10(index_code: str = "SZ000002") -> dict:
    """获取指数权重股前十
    
    Args:
        index_code: 指数代码
    """
    result = rate_limited_call(ball.index_weight_top10, index_code)
    return process_data(result)


@mcp.tool()
def index_perf_7(index_code: str = "SZ000002") -> dict:
    """获取指数最近7天收益数据
    
    Args:
        index_code: 指数代码
    """
    result = rate_limited_call(ball.index_perf_7, index_code)
    return process_data(result)


@mcp.tool()
def index_perf_30(index_code: str = "SZ000002") -> dict:
    """获取指数最近30天收益数据
    
    Args:
        index_code: 指数代码
    """
    result = rate_limited_call(ball.index_perf_30, index_code)
    return process_data(result)


@mcp.tool()
def index_perf_90(index_code: str = "SZ000002") -> dict:
    """获取指数最近90天收益数据
    
    Args:
        index_code: 指数代码
    """
    result = rate_limited_call(ball.index_perf_90, index_code)
    return process_data(result)


@mcp.tool()
def northbound_shareholding_sh(date: str = None) -> dict:
    """获取深港通北向数据
    
    Args:
        date: 日期，默认当天，格式：'2022/01/19'
    """
    result = rate_limited_call(ball.northbound_shareholding_sh, date)
    return process_data(result)


@mcp.tool()
def northbound_shareholding_sz(date: str = None) -> dict:
    """获取沪港通北向数据
    
    Args:
        date: 日期，默认当天，格式：'2022/01/19'
    """
    result = rate_limited_call(ball.northbound_shareholding_sz, date)
    return process_data(result)


@mcp.tool()
def fund_detail(fund_code: str) -> dict:
    """获取基金详细信息
    
    Args:
        fund_code: 基金代码
    """
    result = rate_limited_call(ball.fund_detail, fund_code)
    return process_data(result)


@mcp.tool()
def fund_info(fund_code: str = "SZ000002") -> dict:
    """获取基金基本信息
    
    Args:
        fund_code: 基金代码
    """
    result = rate_limited_call(ball.fund_info, fund_code)
    return process_data(result)


@mcp.tool()
def fund_growth(fund_code: str = "SZ000002") -> dict:
    """获取基金增长数据
    
    Args:
        fund_code: 基金代码
    """
    result = rate_limited_call(ball.fund_growth, fund_code)
    return process_data(result)


@mcp.tool()
def fund_nav_history(fund_code: str = "SZ000002") -> dict:
    """获取基金历史净值数据
    
    Args:
        fund_code: 基金代码
    """
    result = rate_limited_call(ball.fund_nav_history, fund_code)
    return process_data(result)


@mcp.tool()
def fund_achievement(fund_code: str = "SZ000002") -> dict:
    """获取基金业绩表现数据
    
    Args:
        fund_code: 基金代码
    """
    result = rate_limited_call(ball.fund_achievement, fund_code)
    return process_data(result)


@mcp.tool()
def fund_asset(fund_code: str = "SZ000002") -> dict:
    """获取基金资产配置数据
    
    Args:
        fund_code: 基金代码
    """
    result = rate_limited_call(ball.fund_asset, fund_code)
    return process_data(result)


@mcp.tool()
def fund_manager(fund_code: str = "SZ000002") -> dict:
    """获取基金经理信息
    
    Args:
        fund_code: 基金代码
    """
    result = rate_limited_call(ball.fund_manager, fund_code)
    return process_data(result)


@mcp.tool()
def fund_trade_date(fund_code: str = "SZ000002") -> dict:
    """获取基金交易日期信息
    
    Args:
        fund_code: 基金代码
    """
    result = rate_limited_call(ball.fund_trade_date, fund_code)
    return process_data(result)


@mcp.tool()
def fund_derived(fund_code: str = "SZ000002") -> dict:
    """获取基金衍生数据
    
    Args:
        fund_code: 基金代码
    """
    result = rate_limited_call(ball.fund_derived, fund_code)
    return process_data(result)
