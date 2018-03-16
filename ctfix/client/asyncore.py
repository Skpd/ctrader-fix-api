import sys
import socket
import asyncore
import logging
import logging.handlers
from ctfix.message import Message, LogonMessage, HeartbeatMessage, TestResponseMessage, MarketDataRequestMessage
from ctfix.math import calculate_spread
from ctfix.field import *


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
            Message.TYPES.Logon: [self.logon_handler],
            Message.TYPES.Logout: [self.logout_handler],
            Message.TYPES.Heartbeat: [self.heartbeat_handler],
            Message.TYPES.TestRequest: [self.test_request_handled],
            Message.TYPES.Reject: [self.reject_handler],
            Message.TYPES.MessageReject: [self.reject_handler],
            Message.TYPES.MarketDataSnapshot: [self.market_data_snapshot_handler],
            Message.TYPES.MarketDataRefresh: [self.market_data_refresh_handler],
            Message.TYPES.ExecutionReport: [self.execution_report_handler],
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

        if not isinstance(data, Message):
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

        while True:
            checksum_point = self.buffer.find(SEPARATOR + '10=')

            if len(self.buffer) == 0 or checksum_point == -1 or self.buffer[checksum_point + 7:][:1] != SEPARATOR:
                break

            message = Message.from_string(self.buffer[:checksum_point + 8], self.session)
            self.handle_message(message)
            self.buffer = self.buffer[checksum_point + 8:]

    def handle_message(self, message):
        handlers = self.get_message_handler(message)

        if message.get_field(CheckSum) is None:
            self.logger.critical("Incomplete message: {0}".format(message))
            return

        if handlers is not None:
            for h in handlers:
                h(message)

    def get_message_handler(self, message: Message):
        if message.get_type() is None:
            self.logger.warning("Can't handle message: can't find message type")
            return None
        elif message.get_type() in self.handlers:
            return self.handlers[message.get_type()]
        else:
            self.logger.warning("Can't handle message: handler {0} not registered".format(message.get_type()))
            return None

    def writable(self):
        return self.connecting

    def logon_handler(self, message: Message):
        self.authorized = True
        self.logger.info("Logged in at {0} as {1}".format(message.get_field(SendingTime), self.session.sender_id))

    def heartbeat_handler(self, message: Message):
        self.logger.debug(
            "Received heartbeat, sending it back. Server time: {0}.".format(message.get_field(SendingTime))
        )
        self.send(HeartbeatMessage(self.session))

    def test_request_handled(self, message: Message):
        self.send(TestResponseMessage(message.get_field(TestReqID), self.session))

    def reject_handler(self, message: Message):
        self.logger.warning("MESSAGE REJECTED: {0}".format(message.get_field(Text)))

    def market_data_snapshot_handler(self, message: Message):
        prices = message.get_group(Groups.MDEntry_Snapshot)

        if len(prices) < 2 or MDEntryPx not in prices[0] or MDEntryPx not in prices[1]:
            self.logger.warning("No ask or bid in price update.")
            return

        ask_idx = 1 if prices[0][MDEntryType] == '0' else 0
        bid_idx = (ask_idx + 1) % 2
        spread = calculate_spread(
            prices[bid_idx][MDEntryPx],
            prices[ask_idx][MDEntryPx],
            self.session.symbol_table[int(message.get_field(Symbol))]['pip_position']
        )
        name = self.session.symbol_table[int(message.get_field(Symbol))]['name']
        self.logger.info(
            "Symbol: {0: <7}\tBID: {1: <10}\tASK: {2: <10}\tSPREAD: {3}\t\tBID_VOL: {4}\tASK_VOL: {5}".format(
                name,
                prices[bid_idx][MDEntryPx],
                prices[ask_idx][MDEntryPx],
                spread,
                int(self.session.symbol_table[int(message.get_field(Symbol))]['bid_volume'] / 1000000),
                int(self.session.symbol_table[int(message.get_field(Symbol))]['ask_volume'] / 1000000),
            )
        )

    def market_data_refresh_handler(self, message: Message):
        results = message.get_group(Groups.MDEntry_Refresh)
        actions = {'0': 'New', '2': 'Delete'}
        types = {'0': 'BID', '1': 'ASK'}

        message = "Price Update:"
        for r in results:
            if actions[r[MDUpdateAction]] == 'New':
                if MDEntryPx in r:
                    name = self.session.symbol_table[int(r[Symbol])]['name']
                    message += "\n\t\t\tSymbol: {0: <7}, Type: {1}, ID: {2}, Price: {3: <10}, Size: {4}, Action: {5}"\
                        .format(
                            name, types[r[MDEntryType]], r[MDEntryID], r[MDEntryPx],
                            r[MDEntrySize], actions[r[MDUpdateAction]]
                        )
            else:
                message += "\n\t\t\tSymbol: {0: <7}, ID: {1}, Action: {2}".format(
                    'none', r[MDEntryID], actions[r[MDUpdateAction]]
                )

        self.logger.info(message)

    def execution_report_handler(self, message: Message):
        statuses = {'0': 'New', '1': 'Partial', '2': 'Filled', '4': 'Cancelled', '8': 'Rejected', 'C': 'Expired'}
        self.logger.warning("ORDER {0} {1} status: {2}, Time: {3}. {4}".format(
            message.get_field(OrderID), message.get_field(ClOrdID),
            statuses[message.get_field(OrdStatus)], message.get_field(TransactTime),
            message.get_field(Text)
        ))

    def logout_handler(self, message: Message):
        self.logger.critical('Logout reason: {0}'.format(message.get_field(Text)))
        self.close()

    def symbol_subscribe(self, symbol_id, refresh=False):
        self.market_last_request += 1
        self.symbol_requests.append({'symbol': symbol_id, 'request_id': self.market_last_request})
        self.send(MarketDataRequestMessage(self.market_last_request, symbol_id, False, refresh, self.session))

    def symbol_unsubscribe(self, symbol_id):
        try:
            symbol_idx = next(i for (i, d) in enumerate(self.symbol_requests) if d['symbol'] == symbol_id)
        except StopIteration:
            self.logger.warning("Can't find subscription for symbol {0}".format(symbol_id))
            return

        self.send(MarketDataRequestMessage(
            self.symbol_requests[symbol_idx]['request_id'],
            symbol_id,
            True,
            False,
            self.session
        ))

    @staticmethod
    def run():
        asyncore.loop(3)
