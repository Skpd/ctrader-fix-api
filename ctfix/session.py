from ctfix.symbol import SETTINGS


class Session:
    def __init__(self, sender_id: str, target_id: str, target_sub=None, sender_sub=None, username=None, password=None):
        self.password = password
        self.username = username
        self.sender_id = sender_id
        self.target_sub = target_sub
        self.target_id = target_id
        self.sender_sub = sender_sub

        self.__sequence_number = 0

        if sender_id.split('.')[0] in SETTINGS:
            self.symbol_table = SETTINGS[sender_id.split('.')[0]]
        else:
            self.symbol_table = SETTINGS['default']

    def next_sequence_number(self):
        self.__sequence_number += 1
        return self.__sequence_number

    def reset_sequence(self):
        self.__sequence_number = 0