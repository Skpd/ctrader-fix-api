import Field
import Message
from Message import Base as BaseMessage
import Symbol
import asyncore
import socket
import logging
import logging.handlers
import datetime
import sys


PROTOCOL = 'FIX.4.4'
SOH = chr(1)


class Session:
    def __init__(self, sender_id: str, target_id: str, target_sub=None, sender_sub=None):
        self.sender_id = sender_id
        self.target_sub = target_sub
        self.target_id = target_id
        self.sender_sub = sender_sub

        self.__sequence_number = 0

        if sender_id.split('.')[0] in Symbol.SETTINGS:
            self.symbol_table = Symbol.SETTINGS[sender_id.split('.')[0]]
        else:
            self.symbol_table = Symbol.SETTINGS['default']

    def next_sequence_number(self):
        self.__sequence_number += 1
        return self.__sequence_number

    def reset_sequence(self):
        self.__sequence_number = 0


class LogonMessage(BaseMessage):
    def __init__(self, username, password, heartbeat=3, session=None):
        BaseMessage.__init__(self, [
            (Field.EncryptMethod, 0),
            (Field.HeartBtInt, heartbeat),
            (Field.ResetSeqNum, 'Y'),
            (Field.Username, username),
            (Field.Password, password)
        ], session)
        self.msg_type = Message.Types.Logon


class HeartbeatMessage(BaseMessage):
    def __init__(self, session=None):
        BaseMessage.__init__(self, [], session)
        self.msg_type = Message.Types.Heartbeat


class TestResponseMessage(BaseMessage):
    def __init__(self, text, session=None):
        BaseMessage.__init__(self, [(Field.TestReqID, text)], session)
        self.msg_type = Message.Types.Heartbeat


class MarketDataRequestMessage(BaseMessage):
    def __init__(self, request_id, symbol, unsubscribe=False, refresh=False, session=None):
        BaseMessage.__init__(self, [
            (Field.MDReqID, request_id),
            (Field.SubscriptionRequestType, 2 if unsubscribe else 1),
            (Field.MarketDepth, 0 if refresh else 1),
            (Field.MDUpdateType, 1),
            (Field.NoRelatedSym, 1),
            (Field.Symbol, symbol),
            (Field.NoMDEntryTypes, 2),
            (Field.MDEntryType, 0),
            (Field.MDEntryType, 1),
        ], session)
        self.msg_type = Message.Types.MarketDataRequest


class CreateOrder(BaseMessage):
    def __init__(self, order_id, symbol, side, size, session=None):
        BaseMessage.__init__(self, [
            (Field.ClOrdID, order_id),
            (Field.Symbol, symbol),
            (Field.Side, side),
            (Field.TransactTime, get_time()),
            (Field.OrderQty, size),
            (Field.OrdType, 1),
            (Field.TimeInForce, 3),
        ], session)
        self.msg_type = Message.Types.NewOrder


class CreateLimitOrder(BaseMessage):
    def __init__(self, order_id, symbol, side, size, price, expiry, session=None):
        BaseMessage.__init__(self, [
            (Field.ClOrdID, order_id),
            (Field.Symbol, symbol),
            (Field.Side, side),
            (Field.TransactTime, get_time()),
            (Field.OrderQty, size),
            (Field.OrdType, 2),
            (Field.Price, price),
            (Field.TimeInForce, 6),
            (Field.ExpireTime, expiry)
        ], session)
        self.msg_type = Message.Types.NewOrder


class Client(asyncore.dispatcher):
    logging_level = logging.INFO
    authorized = False
    commission = 0.000030
    buffer = ''
    address = None

    def __init__(self, address: tuple, user, password, session, log_file=None):
        asyncore.dispatcher.__init__(self)
        self.session = session
        self.user = user
        self.password = password
        self.address = address

        self.symbol_requests = []
        self.market_last_request = 1

        if log_file is None:
            log_file = 'messages_' + self.session.sender_id + '.log'

        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=self.logging_level)
        self.logger = logging.getLogger('fix-client.' + self.session.sender_id)

        log_handler = logging.handlers.TimedRotatingFileHandler(log_file, 'h', 1)
        log_handler.setFormatter(logging.Formatter('%(asctime)s: %(message)s'))
        self.message_logger = logging.getLogger('fix-messages.' + self.session.sender_id)
        self.message_logger.addHandler(log_handler)
        self.message_logger.info("\nNEW SESSION\n")

        self.handlers = {
            Message.Types.Logon: [self.logon_handler],
            Message.Types.Logout: [self.logout_handler],
            Message.Types.Heartbeat: [self.heartbeat_handler],
            Message.Types.TestRequest: [self.test_request_handled],
            Message.Types.Reject: [self.reject_handler],
            Message.Types.MessageReject: [self.reject_handler],
            Message.Types.MarketDataSnapshot: [self.market_data_snapshot_handler],
            Message.Types.MarketDataRefresh: [self.market_data_refresh_handler],
            Message.Types.ExecutionReport: [self.execution_report_handler],
        }

        self.do_connect()

    def do_connect(self):
        self.session.reset_sequence()
        self.authorized = False
        self.buffer = ''
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(self.address)

    def add_handler(self, h_type, h_callback):
        if h_type not in self.handlers:
            self.handlers[h_type] = []
        self.handlers[h_type].insert(0, h_callback)

    def set_handler(self, h_type, h_callback):
        self.handlers[h_type] = [h_callback]

    def send(self, data):
        if data is None:
            return

        if not isinstance(data, BaseMessage):
            data = bytes(data, 'ASCII')

        sent = 0
        while sent < len(data):
            sent_m = asyncore.dispatcher.send(self, bytes(data)[sent:])
            if sent_m is None:
                return
            else:
                sent += sent_m

        self.message_logger.debug('OUT >>> ' + str(data).replace('\x01', '|'))
        return sent

    def handle_connect(self):
        if not self.authorized:
            self.send(LogonMessage(self.user, self.password, 3, self.session))

    def handle_close(self):
        self.close()

    def handle_read(self):
        self.buffer += self.recv(2048).decode('ASCII')

        if len(self.buffer) == 0:
            self.logger.info(self.session.sender_id + ' disconnected.')
            self.close()
            sys.exit(1)
            return

        while True:
            checksum_point = self.buffer.find(SOH + '10=')

            if len(self.buffer) == 0 or checksum_point == -1 or self.buffer[checksum_point + 7:][:1] != SOH:
                break

            message = Message.from_string(self.buffer[:checksum_point + 8], self.session)
            self.handle_message(message)
            self.buffer = self.buffer[checksum_point + 8:]

    def handle_message(self, message):
        handlers = self.get_message_handler(message)

        if message.get_field(Field.CheckSum) is None:
            self.logger.critical("Incomplete message: {0}".format(message))
            return

        if handlers is not None:
            for h in handlers:
                h(message)

    def get_message_handler(self, message: BaseMessage):
        if message.get_type() is None:
            self.logger.warn("Can't handle message: can't find message type")
            return None
        elif message.get_type() in self.handlers:
            return self.handlers[message.get_type()]
        else:
            self.logger.warn("Can't handle message: handler {0} not registered".format(message.get_type()))
            return None

    def writable(self):
        return self.connecting

    def logon_handler(self, message: BaseMessage):
        self.authorized = True
        self.logger.info("Logged in at {0} as {1}".format(message.get_field(Field.SendingTime), self.session.sender_id))

    def heartbeat_handler(self, message: BaseMessage):
        self.logger.debug(
            "Received heartbeat, sending it back. Server time: {0}.".format(message.get_field(Field.SendingTime))
        )
        self.send(HeartbeatMessage(self.session))

    def test_request_handled(self, message: BaseMessage):
        self.send(TestResponseMessage(message.get_field(Field.TestReqID), self.session))

    def reject_handler(self, message: BaseMessage):
        self.logger.warn("MESSAGE REJECTED: {0}".format(message.get_field(Field.Text)))

    def market_data_snapshot_handler(self, message: BaseMessage):
        prices = message.get_group(Field.Groups.MDEntry_Snapshot)

        if len(prices) < 2 or Field.MDEntryPx not in prices[0] or Field.MDEntryPx not in prices[1]:
            self.logger.warn("No ask or bid in price update.")
            return

        ask_idx = 1 if prices[0][Field.MDEntryType] == '0' else 0
        bid_idx = (ask_idx + 1) % 2 
        spread = calculate_spread(
            prices[bid_idx][Field.MDEntryPx],
            prices[ask_idx][Field.MDEntryPx],
            self.session.symbol_table[int(message.get_field(Field.Symbol))]['pip_position']
        )
        name = self.session.symbol_table[int(message.get_field(Field.Symbol))]['name']
        self.logger.info("Symbol: {0: <7}\tBID: {1: <10}\tASK: {2: <10}\tSPREAD: {3}\t\tBID_VOL: {4}\tASK_VOL: {5}".format(
            name,
            prices[bid_idx][Field.MDEntryPx],
            prices[ask_idx][Field.MDEntryPx],
            spread,
            int(self.session.symbol_table[int(message.get_field(Field.Symbol))]['bid_volume'] / 1000000),
            int(self.session.symbol_table[int(message.get_field(Field.Symbol))]['ask_volume'] / 1000000),
        ))

    def market_data_refresh_handler(self, message: BaseMessage):
        results = message.get_group(Field.Groups.MDEntry_Refresh)
        actions = {'0': 'New', '2': 'Delete'}
        types = {'0': 'BID', '1': 'ASK'}
        
        message = "Price Update:"
        for r in results:
            if actions[r[Field.MDUpdateAction]] == 'New':
                if Field.MDEntryPx in r:
                    name = self.session.symbol_table[int(r[Field.Symbol])]['name']
                    message += "\n\t\t\tSymbol: {0: <7}, Type: {1}, ID: {2}, Price: {3: <10}, Size: {4}, Action: {5}".format(
                        name, types[r[Field.MDEntryType]], r[Field.MDEntryID], r[Field.MDEntryPx], r[Field.MDEntrySize],
                        actions[r[Field.MDUpdateAction]]
                    )
            else:
                message += "\n\t\t\tSymbol: {0: <7}, ID: {1}, Action: {2}".format(
                    'none', r[Field.MDEntryID], actions[r[Field.MDUpdateAction]]
                )
        
        self.logger.info(message)

    def execution_report_handler(self, message: Message):
        statuses = {'0': 'New', '1': 'Partial', '2': 'Filled', '4': 'Cancelled', '8': 'Rejected', 'C': 'Expired'}
        self.logger.warning("ORDER {0} {1} status: {2}, Time: {3}. {4}".format(
            message.get_field(Field.OrderID), message.get_field(Field.ClOrdID),
            statuses[message.get_field(Field.OrdStatus)], message.get_field(Field.TransactTime),
            message.get_field(Field.Text)
        ))

    def logout_handler(self, message: BaseMessage):
        self.logger.critical('Logout reason: {0}'.format(message.get_field(Field.Text)))
        self.close()

    def symbol_subscribe(self, symbol_id, refresh=False):
        self.market_last_request += 1
        self.symbol_requests.append({'symbol': symbol_id, 'request_id': self.market_last_request})
        self.send(MarketDataRequestMessage(self.market_last_request, symbol_id, False, refresh, self.session))

    def symbol_unsubscribe(self, symbol_id):
        try:
            symbol_idx = next(i for (i, d) in enumerate(self.symbol_requests) if d['symbol'] == symbol_id)
        except StopIteration:
            self.logger.warn("Can't find subscription for symbol {0}".format(Symbol.NAME[symbol_id]))
            return

        self.send(MarketDataRequestMessage(
            self.symbol_requests[symbol_idx]['request_id'],
            symbol_id,
            True,
            False,
            self.session
        ))


def run():
    asyncore.loop(3)


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
