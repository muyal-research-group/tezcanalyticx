
import logging
from http_logging import HttpHost
from http_logging.handler import AsyncHttpHandler

log_handler = AsyncHttpHandler(http_host=HttpHost(name='your-domain.com'))

logger = logging.getLogger()
logger.addHandler(log_handler)

# Works with simple log messages like:
logger.info('Some useful information...')

# Can also handle extra fields:
logger.warning('You\'ve been warned!', extra={'foo': 'bar'})

# And, of course, captures exception with full stack-trace
try:
    1/0
except Exception as exc:
    logger.error('Ooops!', exc_info=exc)