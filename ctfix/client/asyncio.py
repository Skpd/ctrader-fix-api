import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from ctfix import message
from ctfix import field
from ctfix.session import Session
from ctfix.fix44 import TestResponseMessage, MarketDataRequestMessage, HeartbeatMessage, LogonMessage, SOH
from ctfix.fix44 import calculate_spread


class Client:
    session = None
    writer = None
    reader = None
    logger = None
    loop = None
    executor = None
    buffer = b''
    handlers = {}

    def __init__(self, loop, username, password, broker=None, max_threads=None):
        self.session = Session(
            sender_id='{}.{}'.format(broker, username) if broker else username,
            target_id='CSERVER',
            target_sub='QUOTE',
            username=username,
            password=password
        )
        self.loop = loop
        self.executor = ThreadPoolExecutor(
            max_workers=max_threads if max_threads else len(self.session.symbol_table)
        )
        logging.basicConfig(
            format='%(asctime)s %(threadName)s %(levelname)s: %(message)s'
        )
        self.logger = logging.getLogger('fix-client.' + self.session.sender_id)
        self.handlers = {
            message.Types.Logon: [self.on_logon],
            message.Types.Heartbeat: [self.on_heartbeat],
            message.Types.MarketDataSnapshot: [self.on_market_data],
            message.Types.TestRequest: [self.on_test]
        }

    async def connect(self, host=None, port=None):
        logging.info('Connecting ')
        self.writer = None
        self.reader = None
        self.buffer = b''
        self.session.reset_sequence()
        (self.reader, self.writer) = await asyncio.open_connection(host, port, loop=self.loop)
        self.on_connect()

    def on_connect(self):
        logging.info('Connected')
        self.write(LogonMessage(self.session.username, self.session.password, 3, self.session))

    def on_logon(self):
        logging.critical('Signed in as {}'.format(self.session.sender_id))
        # subscribe to all symbols
        for symbol_id, symbol in self.session.symbol_table.items():
            self.write(MarketDataRequestMessage(
                symbol=symbol_id, request_id=symbol_id, session=self.session, refresh=False
            ))

    def on_test(self, msg: message.Base):
        self.write(TestResponseMessage(msg.get_field(field.TestReqID), self.session))

    def on_heartbeat(self):
        self.write(HeartbeatMessage(self.session))

    def on_market_data(self, msg: message.Base):
        prices = msg.get_group(field.Groups.MDEntry_Snapshot)

        if len(prices) < 2 or field.MDEntryPx not in prices[0] or field.MDEntryPx not in prices[1]:
            self.logger.error("No ask or bid in price update.")
            return

        ask_idx = 1 if prices[0][field.MDEntryType] == '0' else 0
        bid_idx = (ask_idx + 1) % 2
        spread = calculate_spread(
            prices[bid_idx][field.MDEntryPx],
            prices[ask_idx][field.MDEntryPx],
            self.session.symbol_table[int(msg.get_field(field.Symbol))]['pip_position']
        )
        name = self.session.symbol_table[int(msg.get_field(field.Symbol))]['name']

        self.logger.info("\t{0: <10}\tSPREAD: {1}\tBID: {2: <10}\tASK: {3: <10}".format(
            name, spread, prices[bid_idx][field.MDEntryPx], prices[ask_idx][field.MDEntryPx],
        ))

    def process(self, buffer):
        self.logger.debug('<<< IN {}'.format(buffer))

        msg = message.from_string(buffer.decode(), self.session)

        if msg.get_type() in self.handlers:
            if type(self.handlers[msg.get_type()]) is not list:
                self.handlers[msg.get_type()] = [self.handlers[msg.get_type()]]

            for handler in self.handlers[msg.get_type()]:
                handler(message)
        else:
            self.logger.warning('No handler for message type "{}"'.format(msg.get_type()))

    def feed(self, data):
        self.buffer += data

        header, value = data.split(b'=')
        if header == b'10':
            self.logger.debug('Submitting task to execute')
            self.executor.submit(self.process, self.buffer)
            self.logger.debug('Submitted')
            self.buffer = b''

    def write(self, msg: message.Base):
        self.logger.debug('>>> OUT {}'.format(bytes(msg)))
        self.loop.call_soon_threadsafe(self.writer.write, bytes(msg))

    async def run(self, host, port):
        await self.connect(host, port)

        while self.loop.is_running():
            try:
                data = await self.reader.readuntil(bytes(SOH, 'ASCII'))
                self.feed(data)
            except asyncio.streams.IncompleteReadError:
                self.logger.critical('!Disconnected!')
                self.logger.info('Trying to reconnect')
                await self.connect(host, port)
