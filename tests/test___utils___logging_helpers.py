import logging
import os
from pathlib import Path
import pytest
from alex_leontiev_toolbox_python.utils.logging_helpers import (
    make_log_format,
    get_configured_logger,
)


def test_make_log_format():
    # Test with a list of strings
    log_format_list = ["name", "levelname", "message"]
    expected_format_str = "%(name)s - %(levelname)s - %(message)s"
    assert make_log_format(log_format_list) == expected_format_str

    # Test with a string
    log_format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    assert make_log_format(log_format_str) == log_format_str

    # Test with an unsupported type
    with pytest.raises(NotImplementedError):
        make_log_format(123)


def test_get_configured_logger_basic(capsys):
    logger = get_configured_logger("test_logger_basic")
    logger.info("This is an info message.")
    captured = capsys.readouterr()
    assert "test_logger_basic" in captured.err
    assert "INFO" in captured.err
    assert "This is an info message." in captured.err


def test_get_configured_logger_levels(capsys):
    logger = get_configured_logger("test_logger_levels", level="WARNING")
    logger.info("This should not be logged.")
    logger.warning("This should be logged.")
    captured = capsys.readouterr()
    assert "This should not be logged." not in captured.err
    assert "This should be logged." in captured.err


def test_get_configured_logger_file_logging(tmp_path: Path):
    log_file = tmp_path / "test.log"
    logger = get_configured_logger(
        "test_logger_file", log_to_file=str(log_file), file_log_level="DEBUG"
    )
    logger.debug("This is a debug message for the file.")
    logger.warning("This is a warning for both.")

    with open(log_file, "r") as f:
        log_content = f.read()

    assert "This is a debug message for the file." in log_content
    assert "This is a warning for both." in log_content


def test_get_configured_logger_pre_clean():
    logger = logging.getLogger("test_logger_pre_clean")
    dummy_handler = logging.StreamHandler()
    logger.addHandler(dummy_handler)
    assert len(logger.handlers) == 1

    get_configured_logger("test_logger_pre_clean", is_pre_clean=True)
    # The default configuration adds one handler
    assert len(logger.handlers) == 1
    assert logger.handlers[0] != dummy_handler


def test_get_configured_logger_no_pre_clean():
    logger = logging.getLogger("test_logger_no_pre_clean")
    dummy_handler = logging.StreamHandler()
    logger.addHandler(dummy_handler)
    assert len(logger.handlers) == 1

    get_configured_logger("test_logger_no_pre_clean", is_pre_clean=False)
    # The default configuration adds one handler, so we should have 2 total
    assert len(logger.handlers) == 2


def test_get_configured_logger_propagation():
    logger = get_configured_logger("test_logger_propagate", is_propagate=True)
    assert logger.propagate is True

    logger_no_propagate = get_configured_logger(
        "test_logger_no_propagate", is_propagate=False
    )
    assert logger_no_propagate.propagate is False


def test_get_configured_logger_custom_format(capsys):
    custom_format = "%(levelname)s:%(name)s:%(message)s"
    logger = get_configured_logger(
        "test_logger_custom_format", log_format=custom_format
    )
    logger.error("Custom format test.")
    captured = capsys.readouterr()
    assert "ERROR:test_logger_custom_format:Custom format test." in captured.err
