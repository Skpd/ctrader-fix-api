import pytest
from unittest import mock
from ctfix.session import Session


def test_session_requires_sender_and_target_id():
    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        Session()


def test_session_accept_optional_args():
    session = Session(
        'sender', 'target',
        username='username', password='password', sender_sub='sender_sub', target_sub='target_sub'
    )

    assert session.username == 'username'
    assert session.password == 'password'
    assert session.sender_sub == 'sender_sub'
    assert session.target_sub == 'target_sub'


@mock.patch.dict('ctfix.session.SETTINGS', {'default': {1: 2}, 'something': {2: 3}})
def test_session_set_symbol_table():
    session = Session('something.sender', 'target')
    assert 2 in session.symbol_table

    session = Session('sender', 'target')
    assert 1 in session.symbol_table

    session = Session('sender', 'target', symbol_table_ref='something')
    assert 2 in session.symbol_table


def test_session_sequence():
    session = Session('sender', 'target')
    # noinspection PyProtectedMember,PyUnresolvedReferences
    assert 0 == session._Session__sequence_number
    assert 1 == session.next_sequence_number()
    session.reset_sequence()
    # noinspection PyProtectedMember,PyUnresolvedReferences
    assert 0 == session._Session__sequence_number

