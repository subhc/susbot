import re
import pyslurm

from collections import defaultdict
from types import SimpleNamespace

from utils.log import get_logger
from utils.utils import sizeof_fmt, cache_for_n_seconds

logger = get_logger(__name__)

gpu_order = ['m40', 'p40', 'v100s', 'rtx6k', 'rtx8k', 'a4500', 'a40', 'a6000']


def extract_useful_node_info(value_dict):
    node_info_dict = {
        "gpu_total": int(value_dict["gres"][0].split(":")[2].split("(")[0]),
        "gpu_used": int(value_dict["gres_used"][0].split(":")[2].split("(")[0]),
        "cpu_total": int(value_dict["cpus"]),
        "cpu_used": int(value_dict["alloc_cpus"]),
        "mem_total": sizeof_fmt(value_dict["real_memory"], with_unit=False),
        "mem_used": sizeof_fmt(value_dict["alloc_mem"], with_unit=False),
        "mem_unit": sizeof_fmt(value_dict["real_memory"], with_unit=True)[1],
        "gmem": re.findall("gmem\d+?G", value_dict["features"])
    }
    node_info_dict.update({
        "gpu_free": node_info_dict["gpu_total"] - node_info_dict["gpu_used"],
        "cpu_free": node_info_dict["cpu_total"] - node_info_dict["cpu_used"],
        "mem_free": node_info_dict["mem_total"] - node_info_dict["mem_used"],
    })

    return SimpleNamespace(**node_info_dict)


def format_node_info_into_blocks(node_dict):
    if node_dict:
        node_dict_gpu_grouped = defaultdict(dict)
        for key, value_dict in node_dict.items():
            if len(value_dict['gres']) == 0:  # no gpu in the node
                continue
            else:
                if len(value_dict['gres']) > 1:
                    logger.warning(f"gres length > 1 {value_dict['gres']}")

                node_type = value_dict['gres'][0].split(":")[1]
                node_dict_gpu_grouped[node_type][key] = extract_useful_node_info(value_dict)

        blocks = []
        for node_type in sorted(set(node_dict_gpu_grouped.keys()).difference(gpu_order)) + gpu_order:
            node_dict = node_dict_gpu_grouped[node_type]
            res = "```"
            gmem = None

            res += f"         Free GPU      Free CPU          Free MEM\n"
            for key, value in sorted(node_dict.items(), key=lambda x: (-x[1].gpu_free, x[0])):
                res += f"{key}     "
                res += f'{value.gpu_free:>1} / {value.gpu_total:>1}     '
                res += f'{value.cpu_free:>3} / {value.cpu_total:>3}     '
                res += f'{value.mem_free:>3} / {value.mem_total:>3} {value.mem_unit}\n'
                if not gmem:
                    gmem = value.gmem[0][4:] if len(value.gmem) > 0 else None
            res += "```"

            free_stats = f"{sum(v.gpu_free for v in node_dict.values())}/{sum(v.gpu_total for v in node_dict.values())}"

            blocks.append({
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*{node_type}*" + (f" [{gmem}]" if gmem else "")
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Free*: {free_stats}"
                    }
                ]
            })
            blocks.append({
                "type": "context",
                "elements": [{
                    "type": "mrkdwn",
                    "text": res,
                }],
            })
        return blocks


@cache_for_n_seconds(seconds=2)
def get_pyslum_node_dict():
    return pyslurm.node().get()


def get_node_info_blocks():
    try:
        nodes_dict = get_pyslum_node_dict()
        if nodes_dict:
            res = format_node_info_into_blocks(nodes_dict)
        else:
            logger.warning("No Nodes found!")
            res = []
        return res

    except ValueError as e:
        logger.error(f"Error - {e.args[0]}")
        return []
