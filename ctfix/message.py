import datetime
import ctfix.field


class Message:
    PROTOCOL = 'FIX.4.4'

    class TYPES:
        Logon = 'A'
        Heartbeat = '0'
        TestRequest = '1'
        Logout = '5'
        ResendRequest = '2'
        Reject = '3'
        SequenceReset = '4'
        MarketDataRequest = 'V'
        MarketDataSnapshot = 'W'
        MarketDataRefresh = 'X'
        NewOrder = 'D'
        OrderStatus = 'H'
        ExecutionReport = '8'
        MessageReject = 'j'
        PositionRequest = 'AN'
        PositionReport = 'AP'

    default_session = None
    current_session = None

    def __init__(self, fields=None, session=None):
        self.msg_type = None
        self.fields = []

        if fields is None:
            fields = []

        for pair in fields:
            self.set_field(pair)

        self.length = None
        self.string = None

        if not session and not self.default_session:
            raise RuntimeError('Session must be provided if default session is not set')

        if not session:
            self.current_session = self.default_session
        else:
            self.current_session = session

    def __getitem__(self, item):
        return self.get_field(item)

    def __setitem__(self, key, value):
        return self.set_field((key, value))

    def get_type(self):
        return self.msg_type

    def get_field(self, field_type):
        for pair in self.fields:
            if str(pair[0]) == str(field_type):
                return pair[1]

    def set_field(self, pair):
        if str(pair[0]) == str(ctfix.field.MsgType):
            self.msg_type = pair[1]

        self.fields.append(pair)
        self.length = None
        self.string = None
        return self

    def get_all_by(self, field_type):
        result = []
        for pair in self.fields:
            if str(pair[0]) == str(field_type):
                result.append(pair[1])
        return result

    def get_group(self, group):
        result = []
        for field_id in group:
            values = self.get_all_by(field_id)
            for i, value in enumerate(values):
                if len(result) - 1 < i:
                    result.append({})
                result[i][field_id] = value
        return result

    def __len__(self):
        if self.length is None:
            self.build_message()

        return self.length

    def __str__(self):
        if self.string is None:
            self.build_message()

        return self.string

    def __bytes__(self):
        if self.string is None:
            self.build_message()

        return bytes(self.string, 'ASCII')

    def build_header(self):
        header = [
            (ctfix.field.MsgSeqNum, self.current_session.next_sequence_number()),
            (ctfix.field.SenderCompID, self.current_session.sender_id),
            (ctfix.field.SendingTime, self.get_time()),
            (ctfix.field.TargetCompID, self.current_session.target_id),
        ]

        if self.current_session.target_sub is not None:
            header.append((ctfix.field.TargetSubID, self.current_session.target_sub))
        if self.current_session.sender_sub is not None:
            header.append((ctfix.field.SenderSubID, self.current_session.sender_sub))

        return header

    def build_message(self):
        header = self.build_header()
        self.length = len(header)

        body = ''
        for pair in sorted(header):
            body += self.make_pair(pair)
        for pair in self.fields:
            body += Message.make_pair(pair)

        self.length += len(body)

        msg_str = self.make_pair((ctfix.field.BeginString, Message.PROTOCOL))
        msg_str += self.make_pair((ctfix.field.BodyLength, self.length))
        msg_str += self.make_pair((ctfix.field.MsgType, self.msg_type))
        msg_str += body
        msg_str += self.build_checksum(msg_str)

        self.string = msg_str

    @staticmethod
    def get_time(add_seconds=None):
        if add_seconds:
            return (datetime.datetime.utcnow() + datetime.timedelta(0, add_seconds)).strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
        else:
            return datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]

    @staticmethod
    def make_pair(pair: tuple):
        return str(pair[0]) + "=" + str(pair[1]) + ctfix.field.SEPARATOR

    @classmethod
    def build_checksum(cls, message: str):
        checksum = sum([ord(i) for i in list(message)]) % 256
        return cls.make_pair((10, str(checksum).zfill(3)))

    @classmethod
    def from_string(cls, string, session=None):
        result = cls([], session)
        for pair in string.split(ctfix.field.SEPARATOR):
            if len(pair):
                values = pair.split('=')
                if len(values) == 2:
                    result.set_field((values[0], values[1]))
        result.string = string
        return result


class LogonMessage(Message):
    def __init__(self, username, password, heartbeat=3, session=None):
        super().__init__([
            (ctfix.field.EncryptMethod, 0),
            (ctfix.field.HeartBtInt, heartbeat),
            (ctfix.field.ResetSeqNum, 'Y'),
            (ctfix.field.Username, username),
            (ctfix.field.Password, password)
        ], session)
        self.msg_type = Message.TYPES.Logon


class HeartbeatMessage(Message):
    def __init__(self, session=None):
        super().__init__([], session)
        self.msg_type = Message.TYPES.Heartbeat


class TestResponseMessage(Message):
    def __init__(self, text, session=None):
        super().__init__([(ctfix.field.TestReqID, text)], session)
        self.msg_type = Message.TYPES.Heartbeat


class MarketDataRequestMessage(Message):
    def __init__(self, request_id, symbol, unsubscribe=False, refresh=False, session=None):
        super().__init__([
            (ctfix.field.MDReqID, request_id),
            (ctfix.field.SubscriptionRequestType, 2 if unsubscribe else 1),
            (ctfix.field.MarketDepth, 0 if refresh else 1),
            (ctfix.field.MDUpdateType, 1),
            (ctfix.field.NoRelatedSym, 1),
            (ctfix.field.Symbol, symbol),
            (ctfix.field.NoMDEntryTypes, 2),
            (ctfix.field.MDEntryType, 0),
            (ctfix.field.MDEntryType, 1),
        ], session)
        self.msg_type = Message.TYPES.MarketDataRequest


class CreateOrder(Message):
    def __init__(self, order_id, symbol, side, size, session=None):
        super().__init__([
            (ctfix.field.ClOrdID, order_id),
            (ctfix.field.Symbol, symbol),
            (ctfix.field.Side, side),
            (ctfix.field.TransactTime, self.get_time()),
            (ctfix.field.OrderQty, size),
            (ctfix.field.OrdType, 1),
            (ctfix.field.TimeInForce, 3),
        ], session)
        self.msg_type = Message.TYPES.NewOrder


class CreateLimitOrder(Message):
    def __init__(self, order_id, symbol, side, size, price, expiry, session=None):
        super().__init__([
            (ctfix.field.ClOrdID, order_id),
            (ctfix.field.Symbol, symbol),
            (ctfix.field.Side, side),
            (ctfix.field.TransactTime, self.get_time()),
            (ctfix.field.OrderQty, size),
            (ctfix.field.OrdType, 2),
            (ctfix.field.Price, price),
            (ctfix.field.TimeInForce, 6),
            (ctfix.field.ExpireTime, expiry)
        ], session)
        self.msg_type = Message.TYPES.NewOrder
