import Field
import Message
from Message import Base as BaseMessage
import Symbol
import asyncore
import socket
import logging
import logging.handlers
from decimal import Decimal


PROTOCOL = 'FIX.4.4'
SOH = chr(1)


class Session:
    def __init__(self, sender_id: str, target_id: str, target_sub=None, sender_sub=None):
        self.sender_id = sender_id
        self.target_sub = target_sub
        self.target_id = target_id
        self.sender_sub = sender_sub

        self.__sequence_number = 0

    def next_sequence_number(self):
        self.__sequence_number += 1
        return self.__sequence_number


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


class Client(asyncore.dispatcher):
    logging_level = logging.INFO

    def __init__(self, address: tuple, user, password, session, log_file=None):
        asyncore.dispatcher.__init__(self)
        self.session = session
        self.user = user
        self.password = password

        self.authorized = False
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
        }

        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(address)

    def add_handler(self, h_type, h_callback):
        if h_type not in self.handlers:
            self.handlers[h_type] = []
        self.handlers[h_type].append(h_callback)

    def send(self, data: BaseMessage):
        # self.logger.debug('Initiated request')
        result = asyncore.dispatcher.send(self, data=bytes(data))
        # self.logger.debug('Request complete, sent {0} bytes'.format(result))
        self.message_logger.debug('OUT >>> ' + str(data).replace('\x01', '|'))
        return result

    def handle_connect(self):
        self.send(LogonMessage(self.user, self.password, 3, self.session))

    def handle_close(self):
        self.close()

    def handle_read(self):
        message_str = self.recv(1048576).decode('ASCII')

        if len(message_str) == 0:
            self.logger.info(self.session.sender_id + ' disconnected.')
            return

        header = '8=' + PROTOCOL

        if message_str.find(header) == -1:
            self.logger.warn('Unknown message: ' + message_str)
            return

        parts = message_str.split(header)[1:]

        for msg in parts:
            msg = header + msg
            self.message_logger.debug('IN <<< ' + msg.replace('\x01', '|'))

            message = Message.from_string(msg, self.session)
            handlers = self.get_message_handler(message)

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

        if len(prices) < 2:
            self.logger.warn("No ask or bid in price update.")
            return

        ask_idx = 1 if prices[0][Field.MDEntryType] == '0' else 0
        bid_idx = (ask_idx + 1) % 2 
        spread = calculate_spread(prices[bid_idx][Field.MDEntryPx], prices[ask_idx][Field.MDEntryPx])
        name = Symbol.NAME[int(message.get_field(Field.Symbol))] if int(message.get_field(Field.Symbol)) in Symbol.NAME else int(message.get_field(Field.Symbol))
        self.logger.info("Symbol: {0: <7}\tServer Time: {1}\tBID: {2: <10}\tASK: {3: <10}\tSPREAD: {4}".format(
            name,
            message.get_field(Field.SendingTime),
            prices[bid_idx][Field.MDEntryPx],
            prices[ask_idx][Field.MDEntryPx],
            spread
        ))

    def market_data_refresh_handler(self, message: BaseMessage):
        results = message.get_group(Field.Groups.MDEntry_Refresh)
        actions = {'0': 'New', '2': 'Delete'}
        types = {'0': 'BID', '1': 'ASK'}
        
        message = "Price Update:"
        for r in results:
            name = Symbol.NAME[int(r[Field.Symbol])] if int(r[Field.Symbol]) in Symbol.NAME else r[Field.Symbol]
            if actions[r[Field.MDUpdateAction]] == 'New':
                message += "\n\t\t\tSymbol: {0: <7}, Type: {5}, ID: {1}, Price: {2 <10}, Size: {3}, Action: {4}".format(
                    name, r[Field.MDEntryID], r[Field.MDEntryPx], r[Field.MDEntrySize],
                    actions[r[Field.MDUpdateAction]], types[r[Field.MDEntryType]]
                )
            else:
                message += "\n\t\t\tSymbol: {0: <7}, ID: {1}, Action: {2}".format(
                    name, r[Field.MDEntryID], actions[r[Field.MDUpdateAction]]
                )
        
        self.logger.info(message)

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


def calculate_spread(bid: str, ask: str):
    spread = Decimal(ask) - Decimal(bid)
    return int(spread.to_eng_string().replace('.', ''))
