__all__ = ['Session']

from ctfix.symbol import SETTINGS


class Session:
    __sequence_number = 0
    symbol_table = None

    def __init__(self, sender_id: str, target_id: str, **kwargs):
        self.sender_id = sender_id
        self.target_id = target_id

        self.password = kwargs.get('password', None)
        self.username = kwargs.get('username', None)
        self.target_sub = kwargs.get('target_sub', None)
        self.sender_sub = kwargs.get('sender_sub', None)

        self.set_symbol_table(kwargs.get('symbol_table_ref', None))
        self.reset_sequence()

    def set_symbol_table(self, symbol_table_ref=None):
        if not symbol_table_ref and self.sender_id and self.sender_id.split('.')[0] in SETTINGS:
            self.symbol_table = SETTINGS.get(self.sender_id.split('.')[0])
        elif symbol_table_ref and symbol_table_ref in SETTINGS:
            self.symbol_table = SETTINGS.get(symbol_table_ref)
        else:
            self.symbol_table = SETTINGS.get('default')

    def next_sequence_number(self):
        self.__sequence_number += 1
        return self.__sequence_number

    def reset_sequence(self):
        self.__sequence_number = 0
