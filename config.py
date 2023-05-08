import logging

NEW_GPU_DISPLAY_ORDER = ['v100s', 'rtx6k', 'rtx8k', 'a4500', 'a40', 'a6000'][::-1]
OLD_GPU_DISPLAY_ORDER = ['m40', 'p40'][::-1]
LOGGER_PREFIX = 'vggbot'
LOGGER_OUTPUT = 'logs'
LOGGER_LEVEL = logging.INFO
