import datetime
from ctfix.message import Base, Types
from ctfix.field import *


PROTOCOL = 'FIX.4.4'
SOH = chr(1)


class LogonMessage(Base):
    def __init__(self, username, password, heartbeat=3, session=None):
        super().__init__([
            (EncryptMethod, 0),
            (HeartBtInt, heartbeat),
            (ResetSeqNum, 'Y'),
            (Username, username),
            (Password, password)
        ], session)
        self.msg_type = Types.Logon


class HeartbeatMessage(Base):
    def __init__(self, session=None):
        super().__init__([], session)
        self.msg_type = Types.Heartbeat


class TestResponseMessage(Base):
    def __init__(self, text, session=None):
        super().__init__([(TestReqID, text)], session)
        self.msg_type = Types.Heartbeat


class MarketDataRequestMessage(Base):
    def __init__(self, request_id, symbol, unsubscribe=False, refresh=False, session=None):
        super().__init__([
            (MDReqID, request_id),
            (SubscriptionRequestType, 2 if unsubscribe else 1),
            (MarketDepth, 0 if refresh else 1),
            (MDUpdateType, 1),
            (NoRelatedSym, 1),
            (Symbol, symbol),
            (NoMDEntryTypes, 2),
            (MDEntryType, 0),
            (MDEntryType, 1),
        ], session)
        self.msg_type = Types.MarketDataRequest


class CreateOrder(Base):
    def __init__(self, order_id, symbol, side, size, session=None):
        super().__init__([
            (ClOrdID, order_id),
            (Symbol, symbol),
            (Side, side),
            (TransactTime, get_time()),
            (OrderQty, size),
            (OrdType, 1),
            (TimeInForce, 3),
        ], session)
        self.msg_type = Types.NewOrder


class CreateLimitOrder(Base):
    def __init__(self, order_id, symbol, side, size, price, expiry, session=None):
        super().__init__([
            (ClOrdID, order_id),
            (Symbol, symbol),
            (Side, side),
            (TransactTime, get_time()),
            (OrderQty, size),
            (OrdType, 2),
            (Price, price),
            (TimeInForce, 6),
            (ExpireTime, expiry)
        ], session)
        self.msg_type = Types.NewOrder


def calculate_spread(bid: str, ask: str, pip_position: int) -> int:
    spread = float(ask) - float(bid)
    spread = '{:.{}f}'.format(spread, pip_position + 1)
    return int(spread.replace('.', ''))


def calculate_pip_value(price: str, size: int, pip_position: int) -> str:
    pip = (pow(1 / 10, pip_position) * size) / float(price)
    pip = '{:.5f}'.format(pip)
    return pip


def calculate_commission(size=10000, rate=1, commission=0.000030):
    # can't handle different size/rate for now
    return (size * commission) * rate * 2


def get_time(add_seconds=None):
    if add_seconds:
        return (datetime.datetime.utcnow() + datetime.timedelta(0, add_seconds)).strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
    else:
        return datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
