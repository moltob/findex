import pathlib
from unittest import mock

import pytest

from findex.fs import safe_file_access


def test__safe_file_access__ok():
    func = mock.Mock(return_value=mock.sentinel.RETURN_VALUE)
    assert safe_file_access(mock.sentinel.PATH, func) is mock.sentinel.RETURN_VALUE
    func.assert_called_once_with(mock.sentinel.PATH)


@mock.patch('findex.fs.os.chdir')
def test__safe_file_access__retry_ok(mock_chdir):
    path = pathlib.Path('a', 'b', 'c.txt')
    func = mock.Mock(side_effect=[OSError(), mock.sentinel.RETURN_VALUE])

    assert safe_file_access(path, func) is mock.sentinel.RETURN_VALUE
    assert mock_chdir.mock_calls[:-1] == [mock.call('a'), mock.call('b')]


@mock.patch('findex.fs.os.chdir')
def test__safe_file_access__retry_failed(mock_chdir):
    path = pathlib.Path('a', 'b', 'c.txt')
    exc_expected = OSError(mock.sentinel.REASON)
    func = mock.Mock(side_effect=[OSError(), exc_expected])

    with pytest.raises(OSError) as exc:
        safe_file_access(path, func)

    assert exc.value is exc_expected
