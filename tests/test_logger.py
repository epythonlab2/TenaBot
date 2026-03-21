from src.utils.logger import get_logger


def test_logger_creation():
    logger = get_logger("test_module")

    assert logger.name == "test_module"