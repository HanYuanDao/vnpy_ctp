import struct
import sys
import traceback
import pytz
from datetime import datetime
from time import sleep
from typing import Dict, List, Set, Tuple
from pathlib import Path
import pymysql
import re

from vnpy.event import EventEngine
from vnpy.trader.constant import (
    Direction,
    Offset,
    Exchange,
    OrderType,
    Product,
    Status,
    OptionType
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
)
from vnpy.trader.utility import get_folder_path
from vnpy.trader.event import EVENT_TIMER

from ..api import (
    TdApi,
    THOST_FTDC_OAS_Submitted,
    THOST_FTDC_OAS_Accepted,
    THOST_FTDC_OAS_Rejected,
    THOST_FTDC_OST_NoTradeQueueing,
    THOST_FTDC_OST_PartTradedQueueing,
    THOST_FTDC_OST_AllTraded,
    THOST_FTDC_OST_Canceled,
    THOST_FTDC_D_Buy,
    THOST_FTDC_D_Sell,
    THOST_FTDC_PD_Long,
    THOST_FTDC_PD_Short,
    THOST_FTDC_OPT_LimitPrice,
    THOST_FTDC_OPT_AnyPrice,
    THOST_FTDC_OF_Open,
    THOST_FTDC_OFEN_Close,
    THOST_FTDC_OFEN_CloseYesterday,
    THOST_FTDC_OFEN_CloseToday,
    THOST_FTDC_PC_Futures,
    THOST_FTDC_PC_Options,
    THOST_FTDC_PC_SpotOption,
    THOST_FTDC_PC_Combination,
    THOST_FTDC_CP_CallOptions,
    THOST_FTDC_CP_PutOptions,
    THOST_FTDC_HF_Speculation,
    THOST_FTDC_CC_Immediately,
    THOST_FTDC_FCC_NotForceClose,
    THOST_FTDC_TC_GFD,
    THOST_FTDC_VC_AV,
    THOST_FTDC_TC_IOC,
    THOST_FTDC_VC_CV,
    THOST_FTDC_AF_Delete
)

import socket
import threading

# 委托状态映射
STATUS_CTP2VT: Dict[str, Status] = {
    THOST_FTDC_OAS_Submitted: Status.SUBMITTING,
    THOST_FTDC_OAS_Accepted: Status.SUBMITTING,
    THOST_FTDC_OAS_Rejected: Status.REJECTED,
    THOST_FTDC_OST_NoTradeQueueing: Status.NOTTRADED,
    THOST_FTDC_OST_PartTradedQueueing: Status.PARTTRADED,
    THOST_FTDC_OST_AllTraded: Status.ALLTRADED,
    THOST_FTDC_OST_Canceled: Status.CANCELLED
}

# 多空方向映射
DIRECTION_VT2CTP: Dict[Direction, str] = {
    Direction.LONG: THOST_FTDC_D_Buy,
    Direction.SHORT: THOST_FTDC_D_Sell
}
DIRECTION_CTP2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2CTP.items()}
DIRECTION_CTP2VT[THOST_FTDC_PD_Long] = Direction.LONG
DIRECTION_CTP2VT[THOST_FTDC_PD_Short] = Direction.SHORT

# 委托类型映射
ORDERTYPE_VT2CTP: Dict[OrderType, Tuple] = {
    OrderType.LIMIT: (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV),
    OrderType.MARKET: (THOST_FTDC_OPT_AnyPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV),
    OrderType.FAK: (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_IOC, THOST_FTDC_VC_AV),
    OrderType.FOK: (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_IOC, THOST_FTDC_VC_CV),
}
ORDERTYPE_CTP2VT: Dict[Tuple, OrderType] = {v: k for k, v in ORDERTYPE_VT2CTP.items()}

# 开平方向映射
OFFSET_VT2CTP: Dict[Offset, str] = {
    Offset.OPEN: THOST_FTDC_OF_Open,
    Offset.CLOSE: THOST_FTDC_OFEN_Close,
    Offset.CLOSETODAY: THOST_FTDC_OFEN_CloseToday,
    Offset.CLOSEYESTERDAY: THOST_FTDC_OFEN_CloseYesterday,
}
OFFSET_CTP2VT: Dict[str, Offset] = {v: k for k, v in OFFSET_VT2CTP.items()}

# 交易所映射
EXCHANGE_CTP2VT: Dict[str, Exchange] = {
    "CFFEX": Exchange.CFFEX,
    "SHFE": Exchange.SHFE,
    "CZCE": Exchange.CZCE,
    "DCE": Exchange.DCE,
    "INE": Exchange.INE,
    "GFEX": Exchange.CZCE,
}

# 产品类型映射
PRODUCT_CTP2VT: Dict[str, Product] = {
    THOST_FTDC_PC_Futures: Product.FUTURES,
    THOST_FTDC_PC_Options: Product.OPTION,
    THOST_FTDC_PC_SpotOption: Product.OPTION,
    THOST_FTDC_PC_Combination: Product.SPREAD
}

# 期权类型映射
OPTIONTYPE_CTP2VT: Dict[str, OptionType] = {
    THOST_FTDC_CP_CallOptions: OptionType.CALL,
    THOST_FTDC_CP_PutOptions: OptionType.PUT
}

# 其他常量
MAX_FLOAT = sys.float_info.max  # 浮点数极限值
CHINA_TZ = pytz.timezone("Asia/Shanghai")  # 中国时区

# 合约数据全局缓存字典
symbol_contract_map: Dict[str, ContractData] = {}


class VtpGateway(BaseGateway):
    """
    vn.py用于对接期货CTP柜台的交易接口。
    """

    default_setting: Dict[str, str] = {
        "用户名": "",
        "密码": "",
        "经纪商代码": "",
        "交易服务器": "",
        "行情服务器地址": "",
        "行情服务器端口": 0,
        "产品名称": "",
        "授权编码": ""
    }

    exchanges: List[str] = list(EXCHANGE_CTP2VT.values())

    def __init__(self, event_engine: EventEngine, gateway_name: str = "CTP") -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.md_api: "VtpMdApi" = VtpMdApi(self)
        self.td_api: "CtpTdApi" = CtpTdApi(self)

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        userid: str = setting["用户名"]
        password: str = setting["密码"]
        brokerid: str = setting["经纪商代码"]
        td_address: str = setting["交易服务器"]
        md_address: str = setting["行情服务器地址"]
        md_port: int = setting["行情服务器端口"]
        appid: str = setting["产品名称"]
        auth_code: str = setting["授权编码"]

        if (
                (not td_address.startswith("tcp://"))
                and (not td_address.startswith("ssl://"))
        ):
            td_address = "tcp://" + td_address

        self.td_api.connect(td_address, userid, password, brokerid, auth_code, appid)
        self.md_api.connect(md_address, md_port)

        self.init_query()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        self.td_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        self.td_api.query_position()

    def close(self) -> None:
        """关闭接口"""
        self.td_api.close()
        self.md_api.close()

    def write_error(self, msg: str, error: dict) -> None:
        """输出错误信息日志"""
        error_id: int = error["ErrorID"]
        error_msg: str = error["ErrorMsg"]
        msg = f"{msg}，代码：{error_id}，信息：{error_msg}"
        self.write_log(msg)

    def process_timer_event(self, event) -> None:
        """定时事件处理"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

        self.md_api.update_date()

    def init_query(self) -> None:
        """初始化查询任务"""
        self.count: int = 0
        self.query_functions: list = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)


class VtpMdApi():
    """"""
    CMD_ECHO = 1
    CMD_RTN_MARKETDATA = 11
    CMD_RTN_DECODECFFEX = 12
    CMD_RSP_ERROR = 100
    CMD_REQ_SUB = 101
    CMD_RSP_SUB = 201
    CMD_REQ_UNSUB = 102
    CMD_RSP_UNSUB = 202

    def __init__(self, gateway: VtpGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: VtpGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.reqid: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False
        self.subscribed: Set = set()

        self.address: str = ""
        self.port: int = 0

        self.socket_client: socket.socket = None

        self.msg_size = 1024
        self.msg_head_size = 8

        self.current_date: str = datetime.now().strftime("%Y%m%d")

    def onFrontConnected(self) -> None:
        """服务器连接成功回报"""
        self.gateway.write_log("行情服务器连接成功")
        self.login()

    def onFrontDisconnected(self, reason: int) -> None:
        """服务器连接断开回报"""
        self.login_status = False
        self.gateway.write_log(f"行情服务器连接断开，原因{reason}")

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """用户登录请求回报"""
        if not error["ErrorID"]:
            self.login_status = True
            self.gateway.write_log("行情服务器登录成功")

            for symbol in self.subscribed:
                self.subscribeMarketData(symbol)
        else:
            self.gateway.write_error("行情服务器登录失败", error)

    def onRspError(self, error: dict, reqid: int, last: bool) -> None:
        """请求报错回报"""
        self.gateway.write_error("行情接口报错", error)

    def onRspSubMarketData(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """订阅行情回报"""
        if not error or not error["ErrorID"]:
            return

        self.gateway.write_error("行情订阅失败", error)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        if self.login_status:
            self.subscribeMarketData(req.symbol)
        self.subscribed.add(req.symbol)

    def doConnect(self) -> None:
        try:
            self.socket_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
            # c = s.connect(address, port)
            self.socket_client.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            self.socket_client.ioctl(socket.SIO_KEEPALIVE_VALS,
                                     (1,
                                      5 * 1000,
                                      3 * 1000)
                                     )
            self.socket_client.connect((self.address, self.port))

            for symbol in self.subscribed:
                self.subscribeMarketData(symbol)
        except:
            self.gateway.write_log("行情服务器连接时异常")
            sleep(5)
            self.doConnect()

    def connect(self, address: str, port: int) -> None:
        """连接服务器"""
        self.address = address
        self.port = port

        # 禁止重复发起连接，会导致异常崩溃
        if not self.connect_status:
            self.doConnect()

            self.connect_status = True
            self.login_status = True
            self.gateway.write_log("行情服务器登录成功")

            t = threading.Thread(target=self.receive)
            t.start()

    def close(self) -> None:
        """关闭连接"""
        self.gateway.write_log("行情服务器连接关闭")
        if self.connect_status:
            self.exit()

    def update_date(self) -> None:
        """更新当前日期"""
        self.current_date = datetime.now().strftime("%Y%m%d")

    def subscribeMarketData(self, symbol: str):
        req = bytes()
        req += struct.pack("<l", 36)
        req += struct.pack("<l", VtpMdApi.CMD_REQ_SUB)
        self.reqid += 1
        req += struct.pack("<l", self.reqid)
        req += struct.pack("<32s", bytes(symbol, encoding = "ISO-8859-1"))
        self.socket_client.send(req)

    def receive(self) -> None:
        data_buffer = bytes()

        while True:
            try :
                data = self.socket_client.recv(self.msg_size)
                if data:
                    data_buffer += data
                    while True:
                        if len(data_buffer) < self.msg_head_size:
                            break

                        head_pack = struct.unpack('<2l', data_buffer[:self.msg_head_size])
                        body_size = head_pack[0]
                        cmd_id = head_pack[1]

                        # buffer区长度少于消息体长度则退出
                        if len(data_buffer) < self.msg_head_size + body_size:
                            break

                        # 根据消息长度获取到完整的消息
                        body = data_buffer[0:self.msg_head_size + body_size]

                        # 数据处理
                        self.parse(cmd_id, body)

                        # 将已处理掉的数据清除出buffer区
                        data_buffer = data_buffer[self.msg_head_size + body_size:]
            except:
                self.gateway.write_log("行情服务器连接出现异常")
                self.doConnect()

    def parse(self, cmd_id: int, msg_body):
        try:
            VtpMdApi.parse_data[cmd_id](self, msg_body)
        except Exception:
            self.output(traceback.format_exc())

    def parse_tick(self, msg_body: bytes) -> None:
        trading_day = re.sub(u"([^\u4e00-\u9fa5\u0030-\u005a\u0061-\u007a])", "",
                             str(msg_body[8:17], encoding="ISO-8859-1"))
        instrument_id = re.sub(u"([^\u4e00-\u9fa5\u0030-\u005a\u0061-\u007a])", "",
                               str(msg_body[17:48], encoding="ISO-8859-1"))
        exchange_id = re.sub(u"([^\u4e00-\u9fa5\u0030-\u005a\u0061-\u007a])", "",
                             str(msg_body[48:57], encoding="ISO-8859-1"))
        exchange_inst_id = re.sub(u"([^\u4e00-\u9fa5\u0030-\u005a\u0061-\u007a])", "",
                                  str(msg_body[57:88]))
        last_price = struct.unpack('<d', msg_body[88:96])[0]
        pre_settlement_price = struct.unpack('<d', msg_body[96:104])[0]
        pre_close_price = struct.unpack('<d', msg_body[104:112])[0]
        pre_open_interest = struct.unpack('<d', msg_body[112:120])[0]
        open_price = struct.unpack('<d', msg_body[120:128])[0]
        highest_price = struct.unpack('<d', msg_body[128:136])[0]
        lowest_price = struct.unpack('<d', msg_body[136:144])[0]
        volume = struct.unpack('<l', msg_body[144:148])[0]
        turnover = struct.unpack('<d', msg_body[152:160])[0]
        open_interest = struct.unpack('<d', msg_body[160:168])[0]
        close_price = struct.unpack('<d', msg_body[168:176])[0]
        settlement_price = struct.unpack('<d', msg_body[176:184])[0]
        upper_limit_price = struct.unpack('<d', msg_body[184:192])[0]
        lower_limit_price = struct.unpack('<d', msg_body[192:200])[0]
        pre_delta = struct.unpack('<d', msg_body[200:208])[0]
        curr_delta = struct.unpack('<d', msg_body[208:216])[0]
        update_time = re.sub(u"([^\u4e00-\u9fa5\u0030-\u005a\u0061-\u007a])", "",
                             str(msg_body[216:225], encoding="ISO-8859-1"))
        update_millisec = struct.unpack('<l', msg_body[228:232])[0]
        bid_price_1 = struct.unpack('<d', msg_body[232:240])[0]
        bid_volume_1 = struct.unpack('<l', msg_body[240:244])[0]
        ask_price_1 = struct.unpack('<d', msg_body[248:256])[0]
        ask_volume_1 = struct.unpack('<l', msg_body[256:260])[0]
        bid_price_2 = struct.unpack('<d', msg_body[264:272])[0]
        bid_volume_2 = struct.unpack('<l', msg_body[272:276])[0]
        ask_price_2 = struct.unpack('<d', msg_body[280:288])[0]
        ask_volume_2 = struct.unpack('<l', msg_body[288:292])[0]
        bid_price_3 = struct.unpack('<d', msg_body[296:304])[0]
        bid_volume_3 = struct.unpack('<l', msg_body[304:308])[0]
        ask_price_3 = struct.unpack('<d', msg_body[312:320])[0]
        ask_volume_3 = struct.unpack('<l', msg_body[320:324])[0]
        bid_price_4 = struct.unpack('<d', msg_body[328:336])[0]
        bid_volume_4 = struct.unpack('<l', msg_body[336:340])[0]
        ask_price_4 = struct.unpack('<d', msg_body[344:352])[0]
        ask_volume_4 = struct.unpack('<l', msg_body[352:356])[0]
        bid_price_5 = struct.unpack('<d', msg_body[360:368])[0]
        bid_volume_5 = struct.unpack('<l', msg_body[368:372])[0]
        ask_price_5 = struct.unpack('<d', msg_body[376:384])[0]
        ask_volume_5 = struct.unpack('<l', msg_body[384:388])[0]
        average_price = struct.unpack('<d', msg_body[392:400])[0]
        action_day = re.sub(u"([^\u4e00-\u9fa5\u0030-\u005a\u0061-\u007a])", "",
                            str(msg_body[400:409], encoding="ISO-8859-1"))

        """行情数据推送"""
        # 过滤没有时间戳的异常行情数据
        if not update_time:
            return

        # 过滤还没有收到合约数据前的行情推送
        symbol: str = instrument_id
        contract: ContractData = symbol_contract_map.get(symbol, None)
        if not contract:
            return

        # 对大商所的交易日字段取本地日期
        if contract.exchange == Exchange.DCE:
            date_str: str = self.current_date
        else:
            date_str: str = action_day

        timestamp: str = f"{date_str} {update_time}.{update_millisec}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S.%f")
        dt: datetime = CHINA_TZ.localize(dt)

        tick: TickData = TickData(
            symbol=symbol,
            exchange=contract.exchange,
            datetime=dt,
            name=contract.name,
            volume=volume,
            turnover=turnover,
            open_interest=open_interest,
            last_price=adjust_price(last_price),
            limit_up=upper_limit_price,
            limit_down=lower_limit_price,
            open_price=adjust_price(open_price),
            high_price=adjust_price(highest_price),
            low_price=adjust_price(lowest_price),
            pre_close=adjust_price(pre_close_price),
            bid_price_1=adjust_price(bid_price_1),
            ask_price_1=adjust_price(ask_price_1),
            bid_volume_1=bid_volume_1,
            ask_volume_1=ask_volume_1,
            actionDay="",
            tradDay="",
            gateway_name=self.gateway_name
        )

        tick.bid_price_2 = adjust_price(bid_price_2)
        tick.bid_price_3 = adjust_price(bid_price_3)
        tick.bid_price_4 = adjust_price(bid_price_4)
        tick.bid_price_5 = adjust_price(bid_price_5)

        tick.ask_price_2 = adjust_price(ask_price_2)
        tick.ask_price_3 = adjust_price(ask_price_3)
        tick.ask_price_4 = adjust_price(ask_price_4)
        tick.ask_price_5 = adjust_price(ask_price_5)

        tick.bid_volume_2 = bid_volume_2
        tick.bid_volume_3 = bid_volume_3
        tick.bid_volume_4 = bid_volume_4
        tick.bid_volume_5 = bid_volume_5

        tick.ask_volume_2 = ask_volume_2
        tick.ask_volume_3 = ask_volume_3
        tick.ask_volume_4 = ask_volume_4
        tick.ask_volume_5 = ask_volume_5

        self.gateway.on_tick(tick)

    def parse_cipher_tick(self, msg_body: bytes):
        # 解析获取加密字符
        byte_key = msg_body[8]

        parse_tick = bytes()
        for index in range(9, 9+298):
            parse_tick += bytes([msg_body[index] ^ byte_key])

        instrument_id = re.sub(u"([^\u4e00-\u9fa5\u0030-\u005a\u0061-\u007a])", "",
                               str(parse_tick[0:31], encoding="ISO-8859-1"))
        last_price = struct.unpack('<d', parse_tick[31:39])[0]
        volume = struct.unpack('<l', parse_tick[39:43])[0]
        bid_price_1 = struct.unpack('<d', parse_tick[43:51])[0]
        bid_price_2 = struct.unpack('<d', parse_tick[51:59])[0]
        bid_price_3 = struct.unpack('<d', parse_tick[59:67])[0]
        bid_price_4 = struct.unpack('<d', parse_tick[67:75])[0]
        bid_price_5 = struct.unpack('<d', parse_tick[75:83])[0]
        bid_volume_1 = struct.unpack('<l', parse_tick[83:87])[0]
        bid_volume_2 = struct.unpack('<l', parse_tick[87:91])[0]
        bid_volume_3 = struct.unpack('<l', parse_tick[91:95])[0]
        bid_volume_4 = struct.unpack('<l', parse_tick[95:99])[0]
        bid_volume_5 = struct.unpack('<l', parse_tick[99:103])[0]
        ask_price_1 = struct.unpack('<d', parse_tick[103:111])[0]
        ask_price_2 = struct.unpack('<d', parse_tick[111:119])[0]
        ask_price_3 = struct.unpack('<d', parse_tick[119:127])[0]
        ask_price_4 = struct.unpack('<d', parse_tick[127:135])[0]
        ask_price_5 = struct.unpack('<d', parse_tick[135:143])[0]
        ask_volume_1 = struct.unpack('<l', parse_tick[143:147])[0]
        ask_volume_2 = struct.unpack('<l', parse_tick[147:151])[0]
        ask_volume_3 = struct.unpack('<l', parse_tick[151:155])[0]
        ask_volume_4 = struct.unpack('<l', parse_tick[155:159])[0]
        ask_volume_5 = struct.unpack('<l', parse_tick[159:163])[0]

        highest_price = struct.unpack('<d', parse_tick[163:171])[0]
        lowest_price = struct.unpack('<d', parse_tick[171:179])[0]

        upper_limit_price = struct.unpack('<d', parse_tick[179:187])[0]
        lower_limit_price = struct.unpack('<d', parse_tick[187:195])[0]

        pre_settlement_price = struct.unpack('<d', parse_tick[195:203])[0]
        pre_close_price = struct.unpack('<d', parse_tick[203:211])[0]
        pre_open_interest = struct.unpack('<d', parse_tick[211:219])[0]

        pre_delta = struct.unpack('<d', parse_tick[219:227])[0]
        curr_delta = struct.unpack('<d', parse_tick[227:235])[0]

        open_price = struct.unpack('<d', parse_tick[235:243])[0]
        close_price = struct.unpack('<d', parse_tick[243:251])[0]
        settlement_price = struct.unpack('<d', parse_tick[251:259])[0]
        turnover = struct.unpack('<d', parse_tick[259:267])[0]
        open_interest = struct.unpack('<d', parse_tick[267:275])[0]
        trading_day = re.sub(u"([^\u4e00-\u9fa5\u0030-\u005a\u0061-\u007a])", "",
                             str(parse_tick[275:284], encoding="ISO-8859-1"))
        update_time = re.sub(u"([^\u4e00-\u9fa5\u0030-\u005a\u0061-\u007a])", "",
                             str(parse_tick[284:293], encoding="ISO-8859-1"))
        update_millisec = struct.unpack('<l', parse_tick[293:297])[0]

        """行情数据推送"""
        # 过滤没有时间戳的异常行情数据
        if not update_time:
            return

        # 过滤还没有收到合约数据前的行情推送
        symbol: str = instrument_id
        contract: ContractData = symbol_contract_map.get(symbol, None)
        if not contract:
            return

        # 对大商所的交易日字段取本地日期
        if contract.exchange == Exchange.DCE:
            date_str: str = self.current_date
        else:
            date_str: str = trading_day

        timestamp: str = f"{date_str} {update_time}.{update_millisec}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S.%f")
        dt: datetime = CHINA_TZ.localize(dt)

        tick: TickData = TickData(
            symbol=symbol,
            exchange=contract.exchange,
            datetime=dt,
            name=contract.name,
            volume=volume,
            turnover=turnover,
            open_interest=open_interest,
            last_price=adjust_price(last_price),
            limit_up=upper_limit_price,
            limit_down=lower_limit_price,
            open_price=adjust_price(open_price),
            high_price=adjust_price(highest_price),
            low_price=adjust_price(lowest_price),
            pre_close=adjust_price(pre_close_price),
            bid_price_1=adjust_price(bid_price_1),
            ask_price_1=adjust_price(ask_price_1),
            bid_volume_1=bid_volume_1,
            ask_volume_1=ask_volume_1,
            actionDay="",
            tradDay="",
            gateway_name=self.gateway_name
        )

        tick.bid_price_2 = adjust_price(bid_price_2)
        tick.bid_price_3 = adjust_price(bid_price_3)
        tick.bid_price_4 = adjust_price(bid_price_4)
        tick.bid_price_5 = adjust_price(bid_price_5)

        tick.ask_price_2 = adjust_price(ask_price_2)
        tick.ask_price_3 = adjust_price(ask_price_3)
        tick.ask_price_4 = adjust_price(ask_price_4)
        tick.ask_price_5 = adjust_price(ask_price_5)

        tick.bid_volume_2 = bid_volume_2
        tick.bid_volume_3 = bid_volume_3
        tick.bid_volume_4 = bid_volume_4
        tick.bid_volume_5 = bid_volume_5

        tick.ask_volume_2 = ask_volume_2
        tick.ask_volume_3 = ask_volume_3
        tick.ask_volume_4 = ask_volume_4
        tick.ask_volume_5 = ask_volume_5

        self.gateway.on_tick(tick)

    def parse_defind(self, msg_body: bytes):
        pass

    parse_data = {
        CMD_ECHO: parse_defind,
        CMD_RTN_MARKETDATA: parse_tick,
        CMD_RTN_DECODECFFEX: parse_cipher_tick,
        CMD_RSP_ERROR: parse_defind,
        CMD_REQ_SUB: parse_defind,
        CMD_RSP_SUB: parse_defind,
        CMD_REQ_UNSUB: parse_defind,
        CMD_RSP_UNSUB: parse_defind,
    }


class CtpTdApi(TdApi):
    """"""

    def __init__(self, gateway: VtpGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: VtpGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.reqid: int = 0
        self.order_ref: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False
        self.auth_status: bool = False
        self.login_failed: bool = False
        self.auth_failed: bool = False
        self.contract_inited: bool = False

        self.userid: str = ""
        self.password: str = ""
        self.brokerid: str = ""
        self.auth_code: str = ""
        self.appid: str = ""

        self.frontid: int = 0
        self.sessionid: int = 0
        self.order_data: List[dict] = []
        self.trade_data: List[dict] = []
        self.positions: Dict[str, PositionData] = {}
        self.sysid_orderid_map: Dict[str, str] = {}

    def onFrontConnected(self) -> None:
        """服务器连接成功回报"""
        self.gateway.write_log("交易服务器连接成功")

        if self.auth_code:
            self.authenticate()
        else:
            self.login()

    def onFrontDisconnected(self, reason: int) -> None:
        """服务器连接断开回报"""
        self.login_status = False
        self.gateway.write_log(f"交易服务器连接断开，原因{reason}")

    def onRspAuthenticate(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """用户授权验证回报"""
        if not error['ErrorID']:
            self.auth_status = True
            self.gateway.write_log("交易服务器授权验证成功")
            self.login()
        else:
            self.auth_failed = True

            self.gateway.write_error("交易服务器授权验证失败", error)

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """用户登录请求回报"""
        if not error["ErrorID"]:
            self.frontid = data["FrontID"]
            self.sessionid = data["SessionID"]
            self.login_status = True
            self.gateway.write_log("交易服务器登录成功")

            # 自动确认结算单
            ctp_req: dict = {
                "BrokerID": self.brokerid,
                "InvestorID": self.userid
            }
            self.reqid += 1
            self.reqSettlementInfoConfirm(ctp_req, self.reqid)
        else:
            self.login_failed = True

            self.gateway.write_error("交易服务器登录失败", error)

    def onRspOrderInsert(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """委托下单失败回报"""
        order_ref: str = data["OrderRef"]
        orderid: str = f"{self.frontid}_{self.sessionid}_{order_ref}"

        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map[symbol]

        order: OrderData = OrderData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            direction=DIRECTION_CTP2VT[data["Direction"]],
            offset=OFFSET_CTP2VT.get(data["CombOffsetFlag"], Offset.NONE),
            price=data["LimitPrice"],
            volume=data["VolumeTotalOriginal"],
            status=Status.REJECTED,
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)

        self.gateway.write_error("交易委托失败", error)

    def onRspOrderAction(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """委托撤单失败回报"""
        self.gateway.write_error("交易撤单失败", error)

    def onRspSettlementInfoConfirm(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """确认结算单回报"""
        self.gateway.write_log("结算信息确认成功")

        # 由于流控，单次查询可能失败，通过while循环持续尝试，直到成功发出请求
        while True:
            self.reqid += 1
            n: int = self.reqQryInstrument({}, self.reqid)

            if not n:
                break
            else:
                sleep(1)

    def onRspQryInvestorPosition(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """持仓查询回报"""
        if not data:
            return

        # 必须已经收到了合约信息后才能处理
        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map.get(symbol, None)

        if contract:
            # 获取之前缓存的持仓数据缓存
            key: str = f"{data['InstrumentID'], data['PosiDirection']}"
            position: PositionData = self.positions.get(key, None)
            if not position:
                position: PositionData = PositionData(
                    symbol=data["InstrumentID"],
                    exchange=contract.exchange,
                    direction=DIRECTION_CTP2VT[data["PosiDirection"]],
                    gateway_name=self.gateway_name
                )
                self.positions[key] = position

            # 对于上期所昨仓需要特殊处理
            if position.exchange in [Exchange.SHFE, Exchange.INE]:
                if data["YdPosition"] and not data["TodayPosition"]:
                    position.yd_volume = data["Position"]
            # 对于其他交易所昨仓的计算
            else:
                position.yd_volume = data["Position"] - data["TodayPosition"]

            # 获取合约的乘数信息
            size: int = contract.size

            # 计算之前已有仓位的持仓总成本
            cost: float = position.price * position.volume * size

            # 累加更新持仓数量和盈亏
            position.volume += data["Position"]
            position.pnl += data["PositionProfit"]

            # 计算更新后的持仓总成本和均价
            if position.volume and size:
                cost += data["PositionCost"]
                position.price = cost / (position.volume * size)

            # 更新仓位冻结数量
            if position.direction == Direction.LONG:
                position.frozen += data["ShortFrozen"]
            else:
                position.frozen += data["LongFrozen"]

        if last:
            for position in self.positions.values():
                self.gateway.on_position(position)

            self.positions.clear()

    def onRspQryTradingAccount(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """资金查询回报"""
        if "AccountID" not in data:
            return

        account: AccountData = AccountData(
            accountid=data["AccountID"],
            balance=data["Balance"],
            frozen=data["FrozenMargin"] + data["FrozenCash"] + data["FrozenCommission"],
            gateway_name=self.gateway_name
        )
        account.available = data["Available"]

        self.gateway.on_account(account)

    def onRspQryInstrument(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """合约查询回报"""
        product: Product = PRODUCT_CTP2VT.get(data["ProductClass"], None)
        if product:
            contract: ContractData = ContractData(
                symbol=data["InstrumentID"],
                exchange=EXCHANGE_CTP2VT[data["ExchangeID"]],
                name=data["InstrumentName"],
                product=product,
                size=data["VolumeMultiple"],
                pricetick=data["PriceTick"],
                long_margin_ratio=data["LongMarginRatio"],
                short_margin_ratio=data["ShortMarginRatio"],
                gateway_name=self.gateway_name
            )

            # 期权相关
            if contract.product == Product.OPTION:
                # 移除郑商所期权产品名称带有的C/P后缀
                if contract.exchange == Exchange.CZCE:
                    contract.option_portfolio = data["ProductID"][:-1]
                else:
                    contract.option_portfolio = data["ProductID"]

                contract.option_underlying = data["UnderlyingInstrID"]
                contract.option_type = OPTIONTYPE_CTP2VT.get(data["OptionsType"], None)
                contract.option_strike = data["StrikePrice"]
                contract.option_index = str(data["StrikePrice"])
                contract.option_listed = datetime.strptime(data["OpenDate"], "%Y%m%d")
                contract.option_expiry = datetime.strptime(data["ExpireDate"], "%Y%m%d")

            self.gateway.on_contract(contract)

            symbol_contract_map[contract.symbol] = contract

        if last:
            self.contract_inited = True
            self.gateway.write_log("合约信息查询成功")

            for data in self.order_data:
                self.onRtnOrder(data)
            self.order_data.clear()

            for data in self.trade_data:
                self.onRtnTrade(data)
            self.trade_data.clear()

    def onRtnOrder(self, data: dict) -> None:
        """委托更新推送"""
        if not self.contract_inited:
            self.order_data.append(data)
            return

        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map[symbol]

        frontid: int = data["FrontID"]
        sessionid: int = data["SessionID"]
        order_ref: str = data["OrderRef"]
        orderid: str = f"{frontid}_{sessionid}_{order_ref}"

        timestamp: str = f"{data['InsertDate']} {data['InsertTime']}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        dt: datetime = CHINA_TZ.localize(dt)

        tp = (data["OrderPriceType"], data["TimeCondition"], data["VolumeCondition"])

        order: OrderData = OrderData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            type=ORDERTYPE_CTP2VT[tp],
            direction=DIRECTION_CTP2VT[data["Direction"]],
            offset=OFFSET_CTP2VT[data["CombOffsetFlag"]],
            price=data["LimitPrice"],
            volume=data["VolumeTotalOriginal"],
            traded=data["VolumeTraded"],
            status=STATUS_CTP2VT[data["OrderStatus"]],
            datetime=dt,
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)

        self.sysid_orderid_map[data["OrderSysID"]] = orderid

    def onRtnTrade(self, data: dict) -> None:
        """成交数据推送"""
        if not self.contract_inited:
            self.trade_data.append(data)
            return

        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map[symbol]

        orderid: str = self.sysid_orderid_map[data["OrderSysID"]]

        timestamp: str = f"{data['TradeDate']} {data['TradeTime']}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        dt: datetime = CHINA_TZ.localize(dt)

        trade: TradeData = TradeData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            tradeid=data["TradeID"],
            direction=DIRECTION_CTP2VT[data["Direction"]],
            offset=OFFSET_CTP2VT[data["OffsetFlag"]],
            price=data["Price"],
            volume=data["Volume"],
            datetime=dt,
            gateway_name=self.gateway_name
        )
        self.gateway.on_trade(trade)

    def onRspForQuoteInsert(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """询价请求回报"""
        if not error["ErrorID"]:
            symbol: str = data["InstrumentID"]
            msg: str = f"{symbol}询价请求发送成功"
            self.gateway.write_log(msg)
        else:
            self.gateway.write_error("询价请求发送失败", error)

    def connect(
            self,
            address: str,
            userid: str,
            password: str,
            brokerid: int,
            auth_code: str,
            appid: str
    ) -> None:
        """连接服务器"""
        self.userid = userid
        self.password = password
        self.brokerid = brokerid
        self.auth_code = auth_code
        self.appid = appid

        if not self.connect_status:
            path: Path = get_folder_path(self.gateway_name.lower())
            self.createFtdcTraderApi((str(path) + "\\Td").encode("GBK"))

            self.subscribePrivateTopic(0)
            self.subscribePublicTopic(0)

            self.registerFront(address)
            self.init()

            self.connect_status = True
        else:
            self.authenticate()

    def authenticate(self) -> None:
        """发起授权验证"""
        if self.auth_failed:
            return

        ctp_req: dict = {
            "UserID": self.userid,
            "BrokerID": self.brokerid,
            "AuthCode": self.auth_code,
            "AppID": self.appid
        }

        self.reqid += 1
        self.reqAuthenticate(ctp_req, self.reqid)

    def login(self) -> None:
        """用户登录"""
        if self.login_failed:
            return

        ctp_req: dict = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid,
            "AppID": self.appid
        }

        self.reqid += 1
        self.reqUserLogin(ctp_req, self.reqid)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        if req.offset not in OFFSET_VT2CTP:
            self.gateway.write_log("请选择开平方向")
            return ""

        if req.type not in ORDERTYPE_VT2CTP:
            self.gateway.write_log(f"当前接口不支持该类型的委托{req.type.value}")
            return ""

        self.order_ref += 1

        tp = ORDERTYPE_VT2CTP[req.type]
        price_type, time_condition, volume_condition = tp

        ctp_req: dict = {
            "InstrumentID": req.symbol,
            "ExchangeID": req.exchange.value,
            "LimitPrice": req.price,
            "VolumeTotalOriginal": int(req.volume),
            "OrderPriceType": price_type,
            "Direction": DIRECTION_VT2CTP.get(req.direction, ""),
            "CombOffsetFlag": OFFSET_VT2CTP.get(req.offset, ""),
            "OrderRef": str(self.order_ref),
            "InvestorID": self.userid,
            "UserID": self.userid,
            "BrokerID": self.brokerid,
            "CombHedgeFlag": THOST_FTDC_HF_Speculation,
            "ContingentCondition": THOST_FTDC_CC_Immediately,
            "ForceCloseReason": THOST_FTDC_FCC_NotForceClose,
            "IsAutoSuspend": 0,
            "TimeCondition": time_condition,
            "VolumeCondition": volume_condition,
            "MinVolume": 1
        }

        self.reqid += 1
        self.reqOrderInsert(ctp_req, self.reqid)

        orderid: str = f"{self.frontid}_{self.sessionid}_{self.order_ref}"
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        frontid, sessionid, order_ref = req.orderid.split("_")

        ctp_req: dict = {
            "InstrumentID": req.symbol,
            "ExchangeID": req.exchange.value,
            "OrderRef": order_ref,
            "FrontID": int(frontid),
            "SessionID": int(sessionid),
            "ActionFlag": THOST_FTDC_AF_Delete,
            "BrokerID": self.brokerid,
            "InvestorID": self.userid
        }

        self.reqid += 1
        self.reqOrderAction(ctp_req, self.reqid)

    def query_account(self) -> None:
        """查询资金"""
        self.reqid += 1
        self.reqQryTradingAccount({}, self.reqid)

    def query_position(self) -> None:
        """查询持仓"""
        if not symbol_contract_map:
            return

        ctp_req: dict = {
            "BrokerID": self.brokerid,
            "InvestorID": self.userid
        }

        self.reqid += 1
        self.reqQryInvestorPosition(ctp_req, self.reqid)

    def close(self) -> None:
        """关闭连接"""
        if self.connect_status:
            self.exit()


def adjust_price(price: float) -> float:
    """将异常的浮点数最大值（MAX_FLOAT）数据调整为0"""
    if price == MAX_FLOAT:
        price = 0
    return price