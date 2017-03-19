import Field
import FIX44
import datetime


class Types:
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


def build_checksum(message: str):
    checksum = sum([ord(i) for i in list(message)]) % 256
    return make_pair((10, str(checksum).zfill(3)))


def make_pair(pair: tuple):
    return str(pair[0]) + "=" + str(pair[1]) + FIX44.SOH


class Base:
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
        if str(pair[0]) == str(Field.MsgType):
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
            (Field.MsgSeqNum, self.current_session.next_sequence_number()),
            (Field.SenderCompID, self.current_session.sender_id),
            (Field.SendingTime, datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S.%f")[:-3]),
            (Field.TargetCompID, self.current_session.target_id),
        ]

        if self.current_session.target_sub is not None:
            header.append((Field.TargetSubID, self.current_session.target_sub))
        if self.current_session.sender_sub is not None:
            header.append((Field.SenderSubID, self.current_session.sender_sub))

        return header

    def build_message(self):
        header = self.build_header()
        self.length = len(header)

        body = ''
        for pair in sorted(header):
            body += make_pair(pair)
        for pair in self.fields:
            body += make_pair(pair)

        self.length += len(body)

        msg_str = make_pair((Field.BeginString, FIX44.PROTOCOL))
        msg_str += make_pair((Field.BodyLength, self.length))
        msg_str += make_pair((Field.MsgType, self.msg_type))
        msg_str += body
        msg_str += build_checksum(msg_str)

        self.string = msg_str


def from_string(string):
    result = Base()
    for pair in string.split(FIX44.SOH):
        if len(pair):
            values = pair.split('=')
            if len(values) == 2:
                result.set_field((values[0], values[1]))
    return result
