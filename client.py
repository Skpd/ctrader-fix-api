#!./venv/bin/python
import asyncio
import uvloop
from concurrent.futures import ThreadPoolExecutor
from FIX44 import *
import Message
import logging


class Client:
    session = None
    writer = None
    reader = None
    logger = None
    loop = None
    executor = None
    buffer = b''

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

    def on_test(self, message: Message.Base):
        self.write(TestResponseMessage(message.get_field(Field.TestReqID), self.session))

    def on_heartbeat(self):
        self.write(HeartbeatMessage(self.session))

    def on_market_data(self, message: Message.Base):
        prices = message.get_group(Field.Groups.MDEntry_Snapshot)

        if len(prices) < 2 or Field.MDEntryPx not in prices[0] or Field.MDEntryPx not in prices[1]:
            self.logger.error("No ask or bid in price update.")
            return

        ask_idx = 1 if prices[0][Field.MDEntryType] == '0' else 0
        bid_idx = (ask_idx + 1) % 2
        spread = calculate_spread(
            prices[bid_idx][Field.MDEntryPx],
            prices[ask_idx][Field.MDEntryPx],
            self.session.symbol_table[int(message.get_field(Field.Symbol))]['pip_position']
        )
        name = self.session.symbol_table[int(message.get_field(Field.Symbol))]['name']

        self.logger.info("\t{0: <10}\tSPREAD: {1}\tBID: {2: <10}\tASK: {3: <10}".format(
            name, spread, prices[bid_idx][Field.MDEntryPx], prices[ask_idx][Field.MDEntryPx],
        ))

    def process(self, buffer):
        self.logger.debug('<<< IN {}'.format(buffer))

        message = Message.from_string(buffer.decode(), self.session)

        if message.get_type() == Message.Types.Heartbeat:
            self.on_heartbeat()
        elif message.get_type() == Message.Types.Logon:
            self.on_logon()
        elif message.get_type() == Message.Types.TestRequest:
            self.on_test(message)
        elif message.get_type() == Message.Types.MarketDataSnapshot:
            self.on_market_data(message)

    def feed(self, data):
        self.buffer += data

        header, value = data.split(b'=')
        if header == b'10':
            self.logger.debug('Submitting task to execute')
            self.executor.submit(self.process, self.buffer)
            self.logger.debug('Submitted')
            self.buffer = b''

    def write(self, message: Message.Base):
        self.logger.debug('>>> OUT {}'.format(bytes(message)))
        self.loop.call_soon_threadsafe(self.writer.write, bytes(message))

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


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='CTrader FIX async client.')
    parser.add_argument('--version', action='version', version='%(prog)s v0.21')
    parser.add_argument('-b', '--broker', required=True, help='Broker name, usually first part of sender id.')
    parser.add_argument('-u', '--username', required=True, help='Account number.')
    parser.add_argument('-p', '--password', required=True, help='Account password.')
    parser.add_argument('-s', '--server', required=True, help='Host, ex hXX.p.ctrader.com.')
    parser.add_argument('-v', '--verbose', action='count', help='Increase verbosity level. '
                                                                '-v to something somewhat useful, '
                                                                '-vv to full debug')
    parser.add_argument('-t', '--max-threads', help='Thread limit in thread pool. Default to symbol table length.')
    args = parser.parse_args()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    base_loop = asyncio.get_event_loop()
    client = Client(loop=base_loop, broker=args.broker, username=args.username, password=args.password)
    verbose = min(2, args.verbose) if args.verbose else 0
    client.logger.setLevel(logging.WARNING - (verbose * 10))
    asyncio.ensure_future(client.run(args.server, 5201))
    base_loop.run_forever()
