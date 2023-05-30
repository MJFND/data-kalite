import logging

logger = logging
logger.basicConfig(
    format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
    level=logging.INFO,
)
