import logging
import sys
from ctfix.client.asyncore import Client as AsyncoreClient
from ctfix.message import Message
from ctfix.session import Session


def main():
    global client
    session = Session('sender.id', 'CSERVER', 'QUOTE')
    Message.default_session = session
    client = AsyncoreClient(('ip.ad.dr.ess', 5201), 'login', 'password', session)
    client.add_handler(Message.TYPES.Logon, subscribe)
    client.add_handler(Message.TYPES.Heartbeat, funky_print)
    AsyncoreClient.run()


def subscribe():
    global client
    client.symbol_subscribe(1)


def funky_print():
    print("Do you hear my heart beat?")


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == '--debug':
        AsyncoreClient.logging_level = logging.DEBUG

    main()
