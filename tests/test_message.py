import pytest
from ctfix.session import Session
from ctfix.message import *


def test_message_init():
    with pytest.raises(RuntimeError):
        Message()

    session1 = Session('sender', 'target')
    session2 = Session('sender', 'target')

    assert session1 != session2

    Message.default_session = session1
    msg = Message()
    assert msg.current_session == msg.default_session == session1

    msg = Message(session=session2)
    assert msg.current_session == session2
    assert msg.default_session != session2

    msg = Message([(1, 2)])
    assert msg.fields[0] == (1, 2)


def test_add_field():
    msg = Message()
    msg.add_field((1, 2))
    assert msg.fields[0] == (1, 2)

    msg.add_field(3, 4)
    assert msg.fields[1] == (3, 4)

    msg.length = 123
    msg.add_field(5, 6)
    assert msg.length is None
    assert msg.string is None

    assert msg.add_field(7, 8) == msg




