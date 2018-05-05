import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from ctfix.session import Session
from ctfix.message import *
from ctfix.math import *
from ctfix.field import *


class Client:
    TYPE_QUOTE = 'QUOTE'
    TYPE_TRADE = 'TRADE'
    session = None
    writer = None
    reader = None
    logger = None
    loop = None
    executor = None
    buffer = b''
    handlers = {}
    client_type = None

    def __init__(self, loop, session: Session, max_threads=None, client_type=None):
        self.client_type = self.TYPE_TRADE if client_type == self.TYPE_TRADE else self.TYPE_QUOTE
        self.session = session
        self.loop = loop
        self.executor = ThreadPoolExecutor(
            max_workers=max_threads if max_threads else len(self.session.symbol_table)
        )
        logging.basicConfig(
            format='%(asctime)s %(threadName)s %(name)s %(levelname)s: %(message)s'
        )
        self.logger = logging.getLogger('{} {}'.format(self.session.sender_id, self.client_type))
        self.handlers = {
            Message.TYPES.Logon: [self.on_logon],
            Message.TYPES.Heartbeat: [self.on_heartbeat],
            Message.TYPES.MarketDataSnapshot: [self.on_market_data],
            Message.TYPES.TestRequest: [self.on_test]
        }

    async def connect(self, host=None, port=None):
        self.logger.info('Connecting ')
        self.writer = None
        self.reader = None
        self.buffer = b''
        self.session.reset_sequence()
        (self.reader, self.writer) = await asyncio.open_connection(host, port, loop=self.loop)
        self.on_connect()

    def on_connect(self):
        self.logger.info('Connected')
        self.write(LogonMessage(self.session.username, self.session.password, 30, self.session))

    def on_logon(self):
        self.logger.critical('Signed in as {}'.format(self.session.sender_id))

    def on_test(self, msg: Message):
        self.write(TestResponseMessage(msg.get_field(TestReqID), self.session))

    def on_heartbeat(self):
        self.write(HeartbeatMessage(self.session))

    def on_market_data(self, msg: Message):
        prices = msg.get_group(Groups.MDEntry_Snapshot)

        if len(prices) < 2 or MDEntryPx not in prices[0] or MDEntryPx not in prices[1]:
            self.logger.error("No ask or bid in price update.")
            return

        ask_idx = 1 if prices[0][MDEntryType] == '0' else 0
        bid_idx = (ask_idx + 1) % 2
        spread = calculate_spread(
            prices[bid_idx][MDEntryPx],
            prices[ask_idx][MDEntryPx],
            self.session.symbol_table[int(msg.get_field(Symbol))]['pip_position']
        )
        name = self.session.symbol_table[int(msg.get_field(Symbol))]['name']

        self.logger.info("\t{0: <10}\tSPREAD: {1}\tBID: {2: <10}\tASK: {3: <10}".format(
            name, spread, prices[bid_idx][MDEntryPx], prices[ask_idx][MDEntryPx],
        ))

    def process(self, buffer):
        self.logger.debug('<<< IN {}'.format(buffer.decode().split(SEPARATOR)))

        msg = Message.from_string(buffer.decode(), self.session)

        if msg.get_type() in self.handlers:
            if type(self.handlers[msg.get_type()]) is not list:
                self.handlers[msg.get_type()] = [self.handlers[msg.get_type()]]

            for handler in self.handlers[msg.get_type()]:
                self.logger.debug('Executing {} for message type {}'.format(handler.__name__, msg.get_type()))
                handler(msg)
                self.logger.debug('{} done'.format(handler.__name__))
        else:
            self.logger.warning('No handler for message type "{}"'.format(msg.get_type()))

    def feed(self, data):
        self.buffer += data

        header, value = data.split(b'=')
        if header == b'10':
            self.logger.debug('Submitting task to execute')
            self.loop.call_soon_threadsafe(self.executor.submit, self.process, self.buffer)
            self.logger.debug('Submitted')
            self.buffer = b''

    def write(self, msg: Message):
        self.logger.debug('>>> OUT {}'.format(bytes(msg).replace(b'\x01', b'|')))
        self.loop.call_soon_threadsafe(self.writer.write, bytes(msg))

    async def run(self, host, port):
        await self.connect(host, port)

        max_attempts = 2
        attempts = max_attempts

        while attempts and self.loop.is_running():
            try:
                data = await self.reader.readuntil(bytes(SEPARATOR, 'ASCII'))
                self.feed(data)
                attempts = max_attempts
            except asyncio.streams.IncompleteReadError:
                self.logger.critical('!Disconnected!')
                self.logger.info('Trying to reconnect')
                attempts -= 1
                await self.connect(host, port)

        self.logger.critical('Giving up after {} attempts'.format(max_attempts))
        self.loop.stop()
