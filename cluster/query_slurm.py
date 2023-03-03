import pyslurm

from utils.log import get_logger
from utils.utils import cache_for_n_seconds

logger = get_logger(__name__)


@cache_for_n_seconds(seconds=2)
def get_slum_node_dict():
    try:
        return pyslurm.node().get()
    except ValueError as e:
        logger.error(f"Error - {e.args[0]}")
        return {}


@cache_for_n_seconds(seconds=2)
def get_slum_job_dict():
    try:
        return pyslurm.job().get()
    except ValueError as e:
        logger.error(f"Error - {e.args[0]}")
        return {}


@cache_for_n_seconds(seconds=2)
def get_slum_statistics_dict():
    try:
        return pyslurm.statistics().get()
    except ValueError as e:
        logger.error(f"Error - {e.args[0]}")
        return {}
