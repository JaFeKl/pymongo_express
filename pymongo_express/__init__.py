import logging
from .client import Client


LOGGER = logging.getLogger(__name__)


handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))

LOGGER.addHandler(handler)
LOGGER.setLevel(logging.WARNING)
