import sys
import logging
sys.path.append('../')
import FIX44
import Message
import Symbol


def main():
    global client
    session = FIX44.Session('sender.id', 'CSERVER', 'QUOTE')
    FIX44.BaseMessage.default_session = session
    client = FIX44.Client(('ip.ad.dr.ess', 5201), 'login', 'password', session)
    client.add_handler(Message.Types.Logon, subscribe)
    client.add_handler(Message.Types.Heartbeat, funky_print)
    client.run()


def subscribe(message):
    global client
    FIX44.Client.symbol_subscribe(client, Symbol.EUR_USD)


def funky_print(message):
    print("Do you hear my heart beat?")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--debug':
        FIX44.Client.logging_level = logging.DEBUG
    main()
