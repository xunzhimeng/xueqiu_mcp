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


def timestamp_to_datetime(ts):
    """将时间戳转换为日期时间字符串"""
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        if ts > 1000000000000:  # 毫秒级
            return datetime.datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
        elif ts > 1000000000:  # 秒级
            return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return ts


def format_number(val, unit='auto'):
    """格式化大数字，转为亿/万单位
    
    Args:
        val: 数值
        unit: 'auto'自动选择, 'yi'强制亿, 'wan'强制万, 'raw'不转换
    
    Returns:
        转换后的数值（保留2位小数）
    """
    if val is None:
        return None
    if not isinstance(val, (int, float)):
        return val
    
    if unit == 'raw':
        return val
    
    abs_val = abs(val)
    if unit == 'yi' or (unit == 'auto' and abs_val >= 100000000):
        return round(val / 100000000, 2)  # 亿
    elif unit == 'wan' or (unit == 'auto' and abs_val >= 10000):
        return round(val / 10000, 2)  # 万
    else:
        return round(val, 2)


def simplify_quote_data(data):
    """精简实时行情数据，使用紧凑的二维数组格式
    
    成交量用万手，成交额和市值用亿元
    """
    if not data:
        return data
    
    if isinstance(data, dict) and 'data' in data:
        items = data.get('data', [])
        if isinstance(items, list):
            # 列定义（带单位说明）
            columns = ['symbol', 'current', 'percent', 'chg', 'volume(万手)', 'amount(亿)', 'market_cap(亿)', 'turnover%', 'time']
            rows = []
            for item in items:
                if isinstance(item, dict):
                    rows.append([
                        item.get('symbol'),
                        item.get('current'),
                        item.get('percent'),
                        item.get('chg'),
                        format_number(item.get('volume'), 'wan') if item.get('volume') else None,  # 万手
                        format_number(item.get('amount'), 'yi'),  # 亿元
                        format_number(item.get('market_capital'), 'yi'),  # 亿元
                        item.get('turnover_rate'),
                        item.get('timestamp', '').split(' ')[1] if item.get('timestamp') else None,  # 只保留时间
                    ])
            return {'columns': columns, 'data': rows, 'error_code': data.get('error_code', 0)}
    return data


def simplify_quote_detail_data(data):
    """精简行情详情数据，使用紧凑格式"""
    if not data or 'data' not in data:
        return data
    
    inner = data.get('data', {})
    q = inner.get('quote', {})
    
    # 市场状态
    market_status = inner.get('market', {}).get('status', '未知') if 'market' in inner else None
    
    columns = ['symbol', 'name', 'current', 'percent', 'chg', 'open', 'close', 'high', 'low', 
               'high52w', 'low52w', 'vol(万)', 'amt(亿)', 'turnover%', 
               'pe', 'pb', 'ps', 'pcf', 'cap(亿)', 'float_cap(亿)', 'eps', 'yield%']
    
    quote_data = [
        q.get('symbol'),
        q.get('name'),
        q.get('current'),
        q.get('percent'),
        q.get('chg'),
        q.get('open'),
        q.get('last_close'),
        q.get('high'),
        q.get('low'),
        q.get('high52w'),
        q.get('low52w'),
        format_number(q.get('volume'), 'wan'),
        format_number(q.get('amount'), 'yi'),
        q.get('turnover_rate'),
        q.get('pe_ttm'),
        q.get('pb'),
        q.get('ps'),
        q.get('pcf'),
        format_number(q.get('market_capital'), 'yi'),
        format_number(q.get('float_market_capital'), 'yi'),
        q.get('eps'),
        q.get('dividend_yield'),
    ]
    
    return {
        'market_status': market_status,
        'columns': columns,
        'data': quote_data,
        'error_code': data.get('error_code', 0)
    }


def simplify_kline_data(data):
    """精简K线数据，使用紧凑的二维数组格式
    
    成交量用万手，成交额和市值用亿元
    """
    if not data or 'data' not in data:
        return data
    
    inner = data.get('data', {})
    columns = inner.get('column', [])
    items = inner.get('item', [])
    
    # 输出列定义（带单位说明）
    out_cols = ['date', 'open', 'high', 'low', 'close', 'vol(万)', 'amt(亿)', 'pe', 'pb', 'ps', 'pcf', 'cap(亿)']
    # 原始列名映射
    src_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'amount', 'pe', 'pb', 'ps', 'pcf', 'market_capital']
    # 需要转换单位的列
    unit_map = {'vol(万)': 'wan', 'amt(亿)': 'yi', 'cap(亿)': 'yi'}
    
    # 获取源列索引
    col_indices = {}
    for src, out in zip(src_cols, out_cols):
        if src in columns:
            col_indices[out] = columns.index(src)
    
    simplified_data = []
    for item in items:
        row = []
        for out_col in out_cols:
            idx = col_indices.get(out_col)
            if idx is not None and idx < len(item):
                val = item[idx]
                if out_col == 'date' and val:
                    dt_str = timestamp_to_datetime(val)
                    row.append(dt_str.split(' ')[0] if dt_str else None)
                elif out_col in unit_map:
                    row.append(format_number(val, unit_map[out_col]))
                else:
                    row.append(round(val, 2) if isinstance(val, float) else val)
            else:
                row.append(None)
        simplified_data.append(row)
    
    return {
        'symbol': inner.get('symbol'),
        'columns': out_cols,
        'data': simplified_data,
        'error_code': data.get('error_code', 0)
    }


def simplify_financial_indicator(data):
    """精简业绩指标数据，使用紧凑的二维数组格式"""
    if not data or 'data' not in data:
        return data
    
    inner = data.get('data', {})
    columns = ['report', 'roe%', 'eps', 'bvps', 'cfps', 'gross%', 'roa%']
    rows = []
    
    for item in inner.get('list', []):
        def get_val(key):
            v = item.get(key)
            return round(v[0], 2) if isinstance(v, list) and v else (round(v, 2) if isinstance(v, (int, float)) else v)
        
        rows.append([
            item.get('report_name'),
            get_val('avg_roe'),
            get_val('basic_eps'),
            get_val('np_per_share'),
            get_val('operate_cash_flow_ps'),
            get_val('gross_selling_rate'),
            get_val('net_interest_of_total_assets'),
        ])
    
    return {
        'name': inner.get('quote_name'),
        'columns': columns,
        'data': rows,
        'error_code': data.get('error_code', 0)
    }


def simplify_income_data(data):
    """精简利润表数据，使用紧凑的二维数组格式，金额用亿元"""
    if not data or 'data' not in data:
        return data
    
    inner = data.get('data', {})
    columns = ['report', 'revenue(亿)', 'rev_yoy%', 'profit(亿)', 'profit_yoy%', 'op(亿)']
    rows = []
    
    for item in inner.get('list', []):
        def get_val(key, idx=0):
            v = item.get(key)
            if isinstance(v, list) and len(v) > idx:
                return v[idx]
            return v if idx == 0 else None
        
        rows.append([
            item.get('report_name'),
            format_number(get_val('total_revenue', 0), 'yi'),
            round(get_val('total_revenue', 1) * 100, 2) if get_val('total_revenue', 1) else None,  # 转百分比
            format_number(get_val('net_profit', 0), 'yi'),
            round(get_val('net_profit', 1) * 100, 2) if get_val('net_profit', 1) else None,
            format_number(get_val('op', 0), 'yi'),
        ])
    
    return {
        'name': inner.get('quote_name'),
        'columns': columns,
        'data': rows,
        'error_code': data.get('error_code', 0)
    }


def simplify_balance_data(data):
    """精简资产负债表数据，使用紧凑的二维数组格式，金额用亿元"""
    if not data or 'data' not in data:
        return data
    
    inner = data.get('data', {})
    columns = ['report', 'assets(亿)', 'liab(亿)', 'liab_ratio%']
    rows = []
    
    for item in inner.get('list', []):
        def get_val(key):
            v = item.get(key)
            return v[0] if isinstance(v, list) and v else v
        
        rows.append([
            item.get('report_name'),
            format_number(get_val('total_assets'), 'yi'),
            format_number(get_val('total_liab'), 'yi'),
            round(get_val('asset_liab_ratio') * 100, 2) if get_val('asset_liab_ratio') else None,
        ])
    
    return {
        'name': inner.get('quote_name'),
        'columns': columns,
        'data': rows,
        'error_code': data.get('error_code', 0)
    }


def simplify_cashflow_data(data):
    """精简现金流量表数据，使用紧凑的二维数组格式，金额用亿元"""
    if not data or 'data' not in data:
        return data
    
    inner = data.get('data', {})
    columns = ['report', 'cf_operating(亿)', 'cf_investing(亿)', 'cf_financing(亿)']
    rows = []
    
    for item in inner.get('list', []):
        def get_val(key):
            v = item.get(key)
            return v[0] if isinstance(v, list) and v else v
        
        rows.append([
            item.get('report_name'),
            format_number(get_val('ncf_from_oa'), 'yi'),
            format_number(get_val('ncf_from_ia'), 'yi'),
            format_number(get_val('ncf_from_fa'), 'yi'),
        ])
    
    return {
        'name': inner.get('quote_name'),
        'columns': columns,
        'data': rows,
        'error_code': data.get('error_code', 0)
    }


def simplify_capital_assort(data):
    """精简资金分布数据，使用紧凑格式，金额用亿元"""
    if not data or 'data' not in data:
        return data
    
    inner = data.get('data', {})
    buy_total = inner.get('buy_total') or 0
    sell_total = inner.get('sell_total') or 0
    
    return {
        'time': inner.get('timestamp', '').split(' ')[0] if inner.get('timestamp') else None,
        'columns': ['type', 'large(亿)', 'medium(亿)', 'small(亿)', 'total(亿)'],
        'data': [
            ['buy', format_number(inner.get('buy_large'), 'yi'), format_number(inner.get('buy_medium'), 'yi'), 
             format_number(inner.get('buy_small'), 'yi'), format_number(buy_total, 'yi')],
            ['sell', format_number(inner.get('sell_large'), 'yi'), format_number(inner.get('sell_medium'), 'yi'), 
             format_number(inner.get('sell_small'), 'yi'), format_number(sell_total, 'yi')],
        ],
        'net_inflow(亿)': format_number(buy_total - sell_total, 'yi'),
        'error_code': data.get('error_code', 0)
    }


def simplify_bonus_data(data):
    """精简分红融资数据，使用紧凑格式"""
    if not data or 'data' not in data:
        return data
    
    inner = data.get('data', {})
    
    # 分红记录
    div_rows = []
    for item in inner.get('items', []):
        ex_date = item.get('ashare_ex_dividend_date', '')
        div_rows.append([
            item.get('dividend_year'),
            item.get('plan_explain'),
            ex_date.split(' ')[0] if ex_date else None,
        ])
    
    # 增发记录
    add_rows = []
    for item in inner.get('addtions', []):
        add_rows.append([
            item.get('actual_issue_price'),
            format_number(item.get('actual_issue_vol'), 'wan'),
            format_number(item.get('actual_rc_net_amt'), 'yi'),
        ])
    
    return {
        'dividends': {'columns': ['year', 'plan', 'ex_date'], 'data': div_rows},
        'additions': {'columns': ['price', 'shares(万)', 'amount(亿)'], 'data': add_rows},
        'error_code': data.get('error_code', 0)
    }


def simplify_main_indicator(data):
    """精简主要指标数据，使用紧凑格式"""
    if not data or 'data' not in data:
        return data
    
    items = data.get('data', {}).get('items', [])
    if not items:
        return {'error_code': data.get('error_code', 0), 'data': None}
    
    item = items[0]
    return {
        'report': item.get('report_date'),
        'columns': ['pe_ttm', 'pb', 'eps', 'bvps', 'roe%', 'gross%', 'net%', 'revenue(亿)', 'profit(亿)', 'liab%', 'cap(亿)', 'yield%'],
        'data': [
            item.get('pe_ttm'),
            item.get('pb'),
            item.get('basic_eps'),
            item.get('np_per_share'),
            item.get('avg_roe'),
            item.get('gross_selling_rate'),
            item.get('net_selling_rate'),
            format_number(item.get('total_revenue'), 'yi'),
            format_number(item.get('net_profit_atsopc'), 'yi'),
            item.get('asset_liab_ratio'),
            format_number(item.get('market_capital'), 'yi'),
            item.get('dividend_yield'),
        ],
        'error_code': data.get('error_code', 0)
    }


def simplify_capital_history(data):
    """精简历史资金流向数据，使用紧凑格式，金额用亿元"""
    if not data or 'data' not in data:
        return data
    
    inner = data.get('data', {})
    rows = []
    for item in inner.get('items', []):
        ts = item.get('timestamp', '')
        rows.append([
            ts.split(' ')[0] if ts else None,
            format_number(item.get('amount'), 'yi')
        ])
    
    return {
        'sum5(亿)': format_number(inner.get('sum5'), 'yi'),
        'sum10(亿)': format_number(inner.get('sum10'), 'yi'),
        'sum20(亿)': format_number(inner.get('sum20'), 'yi'),
        'columns': ['date', 'amount(亿)'],
        'data': rows,
        'error_code': data.get('error_code', 0)
    }


def simplify_margin_data(data):
    """精简融资融券数据，使用紧凑格式，金额用亿元"""
    if not data or 'data' not in data:
        return data
    
    items = data.get('data', {}).get('items', [])
    columns = ['date', 'balance(亿)', 'buy(亿)', 'net_buy(亿)']
    rows = []
    for item in items[:10]:  # 只保留最近10条
        td_date = item.get('td_date', '')
        rows.append([
            td_date.split(' ')[0] if td_date else None,
            format_number(item.get('margin_trading_balance'), 'yi'),
            format_number(item.get('margin_trading_buy_amt'), 'yi'),
            format_number(item.get('margin_trading_net_buy_amt'), 'yi'),
        ])
    
    return {'columns': columns, 'data': rows, 'error_code': data.get('error_code', 0)}


def simplify_top_holders(data):
    """精简十大股东数据，使用紧凑格式"""
    if not data or 'data' not in data:
        return data
    
    inner = data.get('data', {})
    columns = ['name', 'ratio%', 'chg%']
    rows = []
    for item in inner.get('items', []):
        rows.append([
            item.get('holder_name'),
            item.get('held_ratio'),
            item.get('chg'),
        ])
    
    return {
        'total_ratio%': inner.get('total', {}).get('held_ratio'),
        'columns': columns,
        'data': rows,
        'error_code': data.get('error_code', 0)
    }


def simplify_org_holding(data):
    """精简机构持仓数据，使用紧凑格式"""
    if not data or 'data' not in data:
        return data
    
    items = data.get('data', {}).get('items', [])
    columns = ['report', 'inst_num', 'ratio%', 'chg%']
    rows = []
    for item in items:
        rows.append([
            item.get('chg_date'),
            item.get('institution_num'),
            item.get('held_ratio'),
            item.get('chg'),
        ])
    
    return {'columns': columns, 'data': rows, 'error_code': data.get('error_code', 0)}


def simplify_business_data(data):
    """精简主营业务数据，使用紧凑格式，金额用亿元"""
    if not data or 'data' not in data:
        return data
    
    inner = data.get('data', {})
    seg_columns = ['name', 'revenue(亿)', 'ratio%', 'gross%']
    reports = []
    
    for report in inner.get('list', []):
        seg_rows = []
        # 只取按产品分类的数据 (class_standard=2通常是按产品)
        for cls in report.get('class_list', []):
            if cls.get('class_standard') == 2:
                for biz in cls.get('business_list', []):
                    seg_rows.append([
                        biz.get('project_announced_name'),
                        format_number(biz.get('prime_operating_income'), 'yi'),
                        round(biz.get('income_ratio', 0) * 100, 2) if biz.get('income_ratio') else None,
                        round(biz.get('gross_profit_rate', 0) * 100, 2) if biz.get('gross_profit_rate') else None,
                    ])
                break
        reports.append({'report': report.get('report_name'), 'data': seg_rows})
    
    return {
        'name': inner.get('quote_name'),
        'main_business': inner.get('main_operation_business'),
        'industry': inner.get('industry', {}).get('ind_name'),
        'seg_columns': seg_columns,
        'reports': reports,
        'error_code': data.get('error_code', 0)
    }


def simplify_pankou_data(data):
    """精简盘口数据，使用紧凑格式"""
    if not data or 'data' not in data:
        return data
    
    d = data.get('data', {})
    
    # 买卖五档数据
    bids = []
    asks = []
    for i in range(1, 6):
        bp = d.get(f'bp{i}')
        bc = d.get(f'bc{i}')
        if bp is not None:
            bids.append([bp, format_number(bc, 'wan') if bc else None])
        
        sp = d.get(f'sp{i}')
        sc = d.get(f'sc{i}')
        if sp is not None:
            asks.append([sp, format_number(sc, 'wan') if sc else None])
    
    return {
        'symbol': d.get('symbol'),
        'current': d.get('current'),
        'time': timestamp_to_datetime(d.get('timestamp')),
        'bid_columns': ['price', 'vol(万)'],
        'bids': bids,  # 买盘 [[价格, 数量], ...]
        'ask_columns': ['price', 'vol(万)'],
        'asks': asks,  # 卖盘 [[价格, 数量], ...]
        'buy_pct': d.get('buypct'),  # 买盘占比
        'sell_pct': d.get('sellpct'),  # 卖盘占比
        'diff': format_number(d.get('diff'), 'wan'),  # 委差(万)
        'ratio': d.get('ratio'),  # 委比
        'error_code': data.get('error_code', 0)
    }


def simplify_blocktrans_data(data):
    """精简大宗交易数据，使用紧凑格式，金额用万元"""
    if not data or 'data' not in data:
        return data
    
    items = data.get('data', {}).get('items', [])
    columns = ['date', 'price', 'vol(万)', 'amt(万)', 'premium%', 'buyer', 'seller']
    rows = []
    
    for item in items[:20]:  # 只保留最近20条
        td_date = item.get('td_date')
        rows.append([
            timestamp_to_datetime(td_date).split(' ')[0] if td_date else None,
            item.get('trans_price'),
            format_number(item.get('vol'), 'wan'),
            format_number(item.get('trans_amt'), 'wan'),
            item.get('premium_rat'),
            item.get('buy_branch_org_name', '')[:20] if item.get('buy_branch_org_name') else None,
            item.get('sell_branch_org_name', '')[:20] if item.get('sell_branch_org_name') else None,
        ])
    
    return {'columns': columns, 'data': rows, 'error_code': data.get('error_code', 0)}


def simplify_holders_data(data):
    """精简股东人数数据，使用紧凑格式"""
    if not data or 'data' not in data:
        return data
    
    items = data.get('data', {}).get('items', [])
    columns = ['date', 'holders(万)', 'chg%', 'per_share', 'per_float', 'top10_ratio%', 'price']
    rows = []
    
    for item in items:
        ts = item.get('timestamp')
        rows.append([
            timestamp_to_datetime(ts).split(' ')[0] if ts else None,
            format_number(item.get('ashare_holder'), 'wan'),
            item.get('chg'),
            round(item.get('per_amount', 0), 2) if item.get('per_amount') else None,
            round(item.get('per_float', 0), 2) if item.get('per_float') else None,
            item.get('top_holder_ratio'),
            item.get('price'),
        ])
    
    return {'columns': columns, 'data': rows, 'error_code': data.get('error_code', 0)}


def simplify_industry_compare_data(data):
    """精简行业对比数据，使用紧凑格式"""
    if not data or 'data' not in data:
        return data
    
    inner = data.get('data', {})
    items = inner.get('items', [])
    
    columns = ['symbol', 'name', 'pe', 'pb', 'roe%', 'gross%', 'net%', 'revenue(亿)', 'profit(亿)', 'cap(亿)']
    rows = []
    
    for item in items:
        rows.append([
            item.get('symbol'),
            item.get('name'),
            round(item.get('pe_ttm', 0), 2) if item.get('pe_ttm') else None,
            round(item.get('pb', 0), 2) if item.get('pb') else None,
            round(item.get('avg_roe', 0), 2) if item.get('avg_roe') else None,
            round(item.get('gross_selling_rate', 0), 2) if item.get('gross_selling_rate') else None,
            round(item.get('net_selling_rate', 0), 2) if item.get('net_selling_rate') else None,
            format_number(item.get('total_revenue'), 'yi'),
            format_number(item.get('net_profit_atsopc'), 'yi'),
            format_number(item.get('market_capital'), 'yi'),
        ])
    
    # 行业平均值
    avg = inner.get('avg', {})
    avg_data = {
        'pe': round(avg.get('pe_ttm', 0), 2) if avg.get('pe_ttm') else None,
        'pb': round(avg.get('pb', 0), 2) if avg.get('pb') else None,
        'roe%': round(avg.get('avg_roe', 0), 2) if avg.get('avg_roe') else None,
        'gross%': round(avg.get('gross_selling_rate', 0), 2) if avg.get('gross_selling_rate') else None,
    }
    
    return {
        'industry': inner.get('ind_name'),
        'report': inner.get('report_name'),
        'count': inner.get('count'),
        'avg': avg_data,
        'columns': columns,
        'data': rows,
        'error_code': data.get('error_code', 0)
    }


def simplify_capital_flow_data(data):
    """精简当日资金流向数据（分钟级），使用紧凑格式，金额用万元"""
    if not data or 'data' not in data:
        return data
    
    items = data.get('data', {}).get('items', [])
    columns = ['time', 'main_in(万)', 'main_out(万)', 'net(万)']
    rows = []
    
    # 只保留每10分钟一条数据，减少数据量
    for i, item in enumerate(items):
        if i % 10 == 0:  # 每10条取1条
            ts = item.get('timestamp')
            time_str = timestamp_to_datetime(ts).split(' ')[1] if ts else None
            main_in = item.get('amount0', 0) + item.get('amount1', 0)  # 特大单+大单流入
            main_out = item.get('amount4', 0) + item.get('amount5', 0)  # 特大单+大单流出
            rows.append([
                time_str,
                format_number(main_in, 'wan'),
                format_number(main_out, 'wan'),
                format_number(main_in - main_out, 'wan'),
            ])
    
    return {'columns': columns, 'data': rows, 'error_code': data.get('error_code', 0)}


def simplify_index_basic_info(data):
    """精简指数基本信息"""
    if not data or 'data' not in data:
        return data
    
    d = data.get('data', {})
    return {
        'symbol': d.get('symbol'),
        'name': d.get('name'),
        'current': d.get('current'),
        'percent': d.get('percent'),
        'chg': d.get('chg'),
        'high': d.get('high'),
        'low': d.get('low'),
        'open': d.get('open'),
        'last_close': d.get('last_close'),
        'volume(亿)': format_number(d.get('volume'), 'yi'),
        'amount(亿)': format_number(d.get('amount'), 'yi'),
        'time': timestamp_to_datetime(d.get('timestamp')),
        'error_code': data.get('error_code', 0)
    }


def simplify_index_weight_top10(data):
    """精简指数权重股前十数据"""
    if not data or 'data' not in data:
        return data
    
    inner = data.get('data')
    if not inner:
        return {'columns': [], 'data': [], 'error_code': data.get('error_code', 0)}
    
    items = inner.get('items', [])
    columns = ['symbol', 'name', 'weight%', 'current', 'percent']
    rows = []
    
    for item in items:
        rows.append([
            item.get('symbol'),
            item.get('name'),
            item.get('weight'),
            item.get('current'),
            item.get('percent'),
        ])
    
    return {'columns': columns, 'data': rows, 'error_code': data.get('error_code', 0)}


def simplify_fund_detail(data):
    """精简基金详情数据"""
    if not data or 'data' not in data:
        return data
    
    d = data.get('data', {})
    
    # 如果有fd_data结构
    fd = d.get('fd_data', {})
    if fd:
        return {
            'symbol': d.get('symbol'),
            'name': d.get('name'),
            'nav': fd.get('unit_nav'),
            'nav_date': fd.get('nav_date'),
            'nav_chg%': fd.get('day_nav_growth'),
            'found_date': fd.get('found_date'),
            'fund_scale(亿)': format_number(fd.get('fund_scale'), 'yi') if fd.get('fund_scale') else None,
            'manager': fd.get('manager_name'),
            'company': fd.get('fund_company_name'),
            'benchmark': fd.get('perf_bm'),
            'invest_target': fd.get('invest_target'),
            'error_code': data.get('error_code', 0)
        }
    
    # 如果有fund_position结构（实际API返回）
    fp = d.get('fund_position', {})
    stock_list = fp.get('stock_list', [])
    
    # 持仓股票精简
    holdings = []
    for stock in stock_list[:10]:  # 只取前10
        holdings.append([
            stock.get('xq_symbol'),
            stock.get('name'),
            stock.get('percent'),
            stock.get('current_price'),
            stock.get('change_percentage'),
        ])
    
    return {
        'asset_total(亿)': format_number(fp.get('asset_tot'), 'yi'),
        'asset_val(亿)': format_number(fp.get('asset_val'), 'yi'),
        'stock_pct': fp.get('stock_percent'),
        'cash_pct': fp.get('cash_percent'),
        'bond_pct': fp.get('bond_percent'),
        'report_date': timestamp_to_datetime(fp.get('enddate')).split(' ')[0] if fp.get('enddate') else None,
        'holdings_columns': ['symbol', 'name', 'weight%', 'price', 'chg%'],
        'holdings': holdings,
        'error_code': data.get('error_code', 0)
    }


def simplify_fund_nav_history(data):
    """精简基金历史净值数据"""
    if not data or 'data' not in data:
        return data
    
    items = data.get('data', {}).get('items', [])
    columns = ['date', 'nav', 'chg%']
    rows = []
    
    for item in items:
        rows.append([
            item.get('date'),
            float(item.get('nav', 0)) if item.get('nav') else None,
            float(item.get('percentage', 0)) if item.get('percentage') else None,
        ])
    
    return {
        'columns': columns,
        'data': rows,
        'total': data.get('data', {}).get('total_items'),
        'error_code': data.get('result_code', 0)
    }


def simplify_convertible_bond(data):
    """精简可转债数据"""
    if not data or 'result' not in data:
        return data
    
    items = data.get('result', {}).get('data', [])
    columns = ['code', 'name', 'stock_code', 'stock_name', 'rating', 'scale(亿)', 'conv_price', 'issue_date', 'expire_date']
    rows = []
    
    for item in items:
        rows.append([
            item.get('SECURITY_CODE'),
            item.get('SECURITY_NAME_ABBR'),
            item.get('CONVERT_STOCK_CODE'),
            item.get('SECURITY_SHORT_NAME'),
            item.get('RATING'),
            item.get('ACTUAL_ISSUE_SCALE'),
            item.get('INITIAL_TRANSFER_PRICE'),
            item.get('VALUE_DATE', '').split(' ')[0] if item.get('VALUE_DATE') else None,
            item.get('EXPIRE_DATE', '').split(' ')[0] if item.get('EXPIRE_DATE') else None,
        ])
    
    return {
        'columns': columns,
        'data': rows,
        'pages': data.get('result', {}).get('pages'),
        'error_code': 0
    }


def process_data(data, process_config=None):
    """
    通用数据处理函数，可扩展添加各种数据处理操作
    
    Args:
        data: 原始数据
        process_config: 处理配置字典，用于指定要执行的处理操作
            例如: {'convert_timestamps': True, 'simplify': 'quote'}
    
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
    
    # 数据精简
    simplify_type = process_config.get('simplify')
    if simplify_type == 'quote':
        data = simplify_quote_data(data)
    elif simplify_type == 'quote_detail':
        data = simplify_quote_detail_data(data)
    elif simplify_type == 'kline':
        data = simplify_kline_data(data)
    elif simplify_type == 'indicator':
        data = simplify_financial_indicator(data)
    elif simplify_type == 'income':
        data = simplify_income_data(data)
    elif simplify_type == 'balance':
        data = simplify_balance_data(data)
    elif simplify_type == 'cashflow':
        data = simplify_cashflow_data(data)
    elif simplify_type == 'capital_assort':
        data = simplify_capital_assort(data)
    elif simplify_type == 'capital_history':
        data = simplify_capital_history(data)
    elif simplify_type == 'bonus':
        data = simplify_bonus_data(data)
    elif simplify_type == 'main_indicator':
        data = simplify_main_indicator(data)
    elif simplify_type == 'margin':
        data = simplify_margin_data(data)
    elif simplify_type == 'top_holders':
        data = simplify_top_holders(data)
    elif simplify_type == 'org_holding':
        data = simplify_org_holding(data)
    elif simplify_type == 'business':
        data = simplify_business_data(data)
    elif simplify_type == 'pankou':
        data = simplify_pankou_data(data)
    elif simplify_type == 'blocktrans':
        data = simplify_blocktrans_data(data)
    elif simplify_type == 'holders':
        data = simplify_holders_data(data)
    elif simplify_type == 'industry_compare':
        data = simplify_industry_compare_data(data)
    elif simplify_type == 'capital_flow':
        data = simplify_capital_flow_data(data)
    elif simplify_type == 'index_basic':
        data = simplify_index_basic_info(data)
    elif simplify_type == 'index_weight':
        data = simplify_index_weight_top10(data)
    elif simplify_type == 'fund_detail':
        data = simplify_fund_detail(data)
    elif simplify_type == 'fund_nav':
        data = simplify_fund_nav_history(data)
    elif simplify_type == 'convertible_bond':
        data = simplify_convertible_bond(data)
    
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
    return process_data(result, {'simplify': 'quote'})


@mcp.tool()
def pankou(stock_code: str = "SZ000002") -> dict:
    """【无需登录】获取实时盘口数据，包含买卖五档报价
    
    Args:
        stock_code: 股票代码
    """
    result = rate_limited_call(ball.pankou, stock_code)
    return process_data(result, {'simplify': 'pankou'})


# ==================== 需要 Token 的工具 ====================

@mcp.tool()
def quote_detail(stock_code: str = "SZ000002") -> dict:
    """获取股票行情详细数据"""
    result = rate_limited_call(ball.quote_detail, stock_code)
    return process_data(result, {'simplify': 'quote_detail'})


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
    return process_data(result, {'simplify': 'kline'})


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
    return process_data(result, {'simplify': 'capital_flow'})


@mcp.tool()
def capital_history(stock_code: str = "SZ000002") -> dict:
    """获取历史资金流入流出数据，每日数据"""
    result = rate_limited_call(ball.capital_history, stock_code)
    return process_data(result, {'simplify': 'capital_history'})


@mcp.tool()
def capital_assort(stock_code: str = "SZ000002") -> dict:
    """获取资金成交分布数据"""
    result = rate_limited_call(ball.capital_assort, stock_code)
    return process_data(result, {'simplify': 'capital_assort'})


@mcp.tool()
def blocktrans(stock_code: str = "SZ000002") -> dict:
    """获取大宗交易数据"""
    result = rate_limited_call(ball.blocktrans, stock_code)
    return process_data(result, {'simplify': 'blocktrans'})


@mcp.tool()
def margin(stock_code: str = "SZ000002") -> dict:
    """获取融资融券数据"""
    result = rate_limited_call(ball.margin, stock_code)
    return process_data(result, {'simplify': 'margin'})


@mcp.tool()
def indicator(stock_code: str = "SZ000002", is_annals: int = 1, count: int = 5) -> dict:
    """按年度、季度获取业绩报表数据
    
    Args:
        stock_code: 股票代码
        is_annals: 只获取年报,默认为1
        count: 返回数据数量,默认5条
    """
    result = rate_limited_call(ball.indicator, symbol=stock_code, is_annals=is_annals, count=count)
    return process_data(result, {'simplify': 'indicator'})


@mcp.tool()
def income(stock_code: str = "SZ000002", is_annals: int = 1, count: int = 5) -> dict:
    """获取利润表数据
    
    Args:
        stock_code: 股票代码
        is_annals: 只获取年报,默认为1
        count: 返回数据数量,默认5条
    """
    result = rate_limited_call(ball.income, symbol=stock_code, is_annals=is_annals, count=count)
    return process_data(result, {'simplify': 'income'})


@mcp.tool()
def balance(stock_code: str = "SZ000002", is_annals: int = 1, count: int = 5) -> dict:
    """获取资产负债表数据
    
    Args:
        stock_code: 股票代码
        is_annals: 只获取年报,默认为1
        count: 返回数据数量,默认5条
    """
    result = rate_limited_call(ball.balance, symbol=stock_code, is_annals=is_annals, count=count)
    return process_data(result, {'simplify': 'balance'})


@mcp.tool()
def cash_flow(stock_code: str = "SZ000002", is_annals: int = 1, count: int = 5) -> dict:
    """获取现金流量表数据
    
    Args:
        stock_code: 股票代码
        is_annals: 只获取年报,默认为1
        count: 返回数据数量,默认5条
    """
    result = rate_limited_call(ball.cash_flow, symbol=stock_code, is_annals=is_annals, count=count)
    return process_data(result, {'simplify': 'cashflow'})


@mcp.tool()
def business(stock_code: str = "SZ000002", count: int = 5) -> dict:
    """获取主营业务构成数据
    
    Args:
        stock_code: 股票代码
        count: 返回数据数量,默认5条
    """
    result = rate_limited_call(ball.business, symbol=stock_code, count=count)
    return process_data(result, {'simplify': 'business'})


@mcp.tool()
def top_holders(stock_code: str = "SZ000002", circula: int = 1) -> dict:
    """获取十大股东数据
    
    Args:
        stock_code: 股票代码
        circula: 只获取流通股,默认为1
    """
    result = rate_limited_call(ball.top_holders, symbol=stock_code, circula=circula)
    return process_data(result, {'simplify': 'top_holders'})


@mcp.tool()
def main_indicator(stock_code: str = "SZ000002") -> dict:
    """获取F10主要指标数据"""
    result = rate_limited_call(ball.main_indicator, stock_code)
    return process_data(result, {'simplify': 'main_indicator'})


@mcp.tool()
def holders(stock_code: str = "SZ000002") -> dict:
    """获取F10股东人数数据"""
    result = rate_limited_call(ball.holders, stock_code)
    return process_data(result, {'simplify': 'holders'})


@mcp.tool()
def org_holding_change(stock_code: str = "SZ000002") -> dict:
    """获取F10机构持仓数据"""
    result = rate_limited_call(ball.org_holding_change, stock_code)
    return process_data(result, {'simplify': 'org_holding'})


@mcp.tool()
def bonus(stock_code: str = "SZ000002", page: int = 1, size: int = 10) -> dict:
    """获取F10分红融资数据
    
    Args:
        stock_code: 股票代码
        page: 第几页 默认1
        size: 每页含有多少数据 默认10
    """
    result = rate_limited_call(ball.bonus, stock_code, page=page, size=size)
    return process_data(result, {'simplify': 'bonus'})


@mcp.tool()
def industry_compare(stock_code: str = "SZ000002") -> dict:
    """获取F10行业对比数据"""
    result = rate_limited_call(ball.industry_compare, stock_code)
    return process_data(result, {'simplify': 'industry_compare'})


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
    return process_data(result, {'simplify': 'convertible_bond'})


@mcp.tool()
def index_basic_info(index_code: str = "SZ000002") -> dict:
    """获取指数基本信息
    
    Args:
        index_code: 指数代码
    """
    result = rate_limited_call(ball.index_basic_info, index_code)
    return process_data(result, {'simplify': 'index_basic'})


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
    return process_data(result, {'simplify': 'index_weight'})


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
    return process_data(result, {'simplify': 'fund_detail'})


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
    return process_data(result, {'simplify': 'fund_nav'})


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
