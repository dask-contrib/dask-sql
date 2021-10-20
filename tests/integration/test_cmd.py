import pytest
from mock import MagicMock, patch
from prompt_toolkit.application import create_app_session
from prompt_toolkit.input import create_pipe_input
from prompt_toolkit.output import DummyOutput
from prompt_toolkit.shortcuts import PromptSession

from dask_sql.cmd import _meta_commands


@pytest.fixture(autouse=True, scope="function")
def mock_prompt_input():
    pipe_input = create_pipe_input()
    try:
        with create_app_session(input=pipe_input, output=DummyOutput()):
            yield pipe_input
    finally:
        pipe_input.close()


def _feed_cli_with_input(
    text,
    editing_mode=None,
    clipboard=None,
    history=None,
    multiline=False,
    check_line_ending=True,
    key_bindings=None,
):
    """
    Create a Prompt, feed it with the given user input and return the CLI
    object.
    This returns a (result, Application) tuple.
    """
    # If the given text doesn't end with a newline, the interface won't finish.
    if check_line_ending:
        assert text.endswith("\r")

    inp = create_pipe_input()

    try:
        inp.send_text(text)
        session = PromptSession(
            input=inp,
            output=DummyOutput(),
            editing_mode=editing_mode,
            history=history,
            multiline=multiline,
            clipboard=clipboard,
            key_bindings=key_bindings,
        )

        result = session.prompt()
        return session.default_buffer.document, session.app

    finally:
        inp.close()


def test_meta_commands(c, client, capsys):
    _meta_commands("?", context=c, client=client)
    captured = capsys.readouterr()
    assert "Commands" in captured.out

    _meta_commands("help", context=c, client=client)
    captured = capsys.readouterr()
    assert "Commands" in captured.out

    _meta_commands("\\d?", context=c, client=client)
    captured = capsys.readouterr()
    assert "Commands" in captured.out

    _meta_commands("\\l", context=c, client=client)
    captured = capsys.readouterr()
    assert "Schemas" in captured.out

    _meta_commands("\\dt", context=c, client=client)
    captured = capsys.readouterr()
    assert "Tables" in captured.out

    _meta_commands("\\dm", context=c, client=client)
    captured = capsys.readouterr()
    assert "Models" in captured.out

    _meta_commands("\\df", context=c, client=client)
    captured = capsys.readouterr()
    assert "Functions" in captured.out

    _meta_commands("\\de", context=c, client=client)
    captured = capsys.readouterr()
    assert "Experiments" in captured.out

    c.create_schema("test_schema")
    _meta_commands("\\dss test_schema", context=c, client=client)
    assert c.schema_name == "test_schema"

    _meta_commands("\\dss not_exists", context=c, client=client)
    captured = capsys.readouterr()
    assert "Schema not_exists not available\n" == captured.out

    with pytest.raises(
        OSError,
        match="Timed out during handshake while "
        "connecting to tcp://localhost:8787 after 5 s",
    ):
        client = _meta_commands("\\dsc localhost:8787", context=c, client=client)
        assert client.scheduler.__dict__["addr"] == "localhost:8787"


def test_connection_info(c, client, capsys):
    dummy_client = MagicMock()
    dummy_client.scheduler.__dict__["addr"] = "somewhereonearth:8787"
    dummy_client.cluster.worker = ["worker1", "worker2"]

    _meta_commands("\\conninfo", context=c, client=dummy_client)
    captured = capsys.readouterr()
    assert "somewhereonearth" in captured.out


def test_quit(c, client, capsys):
    with patch("sys.exit", return_value=lambda: "exit"):
        _meta_commands("quit", context=c, client=client)
        captured = capsys.readouterr()
        assert captured.out == "Quitting dask-sql ...\n"


def test_non_meta_commands(c, client, capsys):
    _meta_commands("\\x", context=c, client=client)
    captured = capsys.readouterr()
    assert (
        "The meta command \\x not available, please use commands from below list"
        in captured.out
    )

    res = _meta_commands("Select 42 as answer", context=c, client=client)
    captured = capsys.readouterr()
    assert res is False
