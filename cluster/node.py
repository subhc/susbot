import re
import pwd

from collections import defaultdict
from types import SimpleNamespace

from cluster.query_slurm import get_slum_node_dict, get_slum_job_dict
from utils.log import get_logger
from utils.utils import sizeof_fmt

logger = get_logger(__name__)

gpu_order = ['m40', 'p40', 'v100s', 'rtx6k', 'rtx8k', 'a4500', 'a40', 'a6000'][::-1]


def extract_useful_node_info(value_dict):
    gmem = re.findall(r"gmem\d+?G", value_dict["features"])
    gmem = gmem[0][4:] if len(gmem) > 0 else None
    node_info_dict = {
        "gpu_total": int(value_dict["gres"][0].split(":")[2].split("(")[0]),
        "gpu_used": int(value_dict["gres_used"][0].split(":")[2].split("(")[0]),
        "cpu_total": int(value_dict["cpus"]),
        "cpu_used": int(value_dict["alloc_cpus"]),
        "mem_total": sizeof_fmt(value_dict["real_memory"], with_unit=False),
        "mem_used": sizeof_fmt(value_dict["alloc_mem"], with_unit=False),
        "mem_unit": sizeof_fmt(value_dict["real_memory"], with_unit=True)[1],
        "gmem": gmem

    }
    node_info_dict.update({
        "gpu_free": node_info_dict["gpu_total"] - node_info_dict["gpu_used"],
        "cpu_free": node_info_dict["cpu_total"] - node_info_dict["cpu_used"],
        "mem_free": node_info_dict["mem_total"] - node_info_dict["mem_used"],
    })

    return SimpleNamespace(**node_info_dict)


def extract_useful_node_info_dict(node_dict):
    node_dict_gpu_grouped = defaultdict(dict)
    for key, value_dict in node_dict.items():
        if len(value_dict['gres']) == 0:  # no gpu in the node
            continue
        else:
            if len(value_dict['gres']) > 1:
                logger.warning(f"gres length > 1 {value_dict['gres']}")

            node_type = value_dict['gres'][0].split(":")[1]
            node_dict_gpu_grouped[node_type][key] = extract_useful_node_info(value_dict)
    return node_dict_gpu_grouped


def get_node_info_blocks(ignore_full_node=False):
    if node_dict := get_slum_node_dict():
        node_dict_gpu_grouped = extract_useful_node_info_dict(node_dict)

        blocks = []
        job_dict = get_slum_job_dict()
        node_user_dict = defaultdict(set)
        for job_id, job_info in job_dict.items():
            if job_info["job_state"] == "RUNNING":
                node_user_dict[job_info["batch_host"]].add(pwd.getpwuid(job_info["user_id"]).pw_name)

        cluster_summary_dict = defaultdict(dict)
        for node_type in sorted(set(node_dict_gpu_grouped.keys()).difference(gpu_order)) + gpu_order:
            node_dict = node_dict_gpu_grouped[node_type]
            gmem = None
            cluster_summary_dict[node_type] = {}
            rows = []
            for key, value in sorted(node_dict.items(), key=lambda x: (-x[1].gpu_free, x[0])):
                if gmem is None:
                    gmem = value.gmem
                if ignore_full_node and value.gpu_free == 0:
                    continue
                res = f"{key}       "
                res += f'{value.gpu_free:>1}/{value.gpu_total:>1}    '
                res += f'{value.cpu_free:>3}/{value.cpu_total:>3}    '
                res += f'{value.mem_free:>3}/{value.mem_total:>3} {value.mem_unit}    '
                res += f"{','.join(sorted(node_user_dict[key]))}" if len(node_user_dict[key]) > 0 else "--"
                rows.append(res)

            cluster_summary_dict[node_type]['table_rows'] = rows
            cluster_summary_dict[node_type]['gmem'] = gmem
            free_stats = f"{sum(v.gpu_free for v in node_dict.values())}/{sum(v.gpu_total for v in node_dict.values())}"
            cluster_summary_dict[node_type]['free_stats'] = free_stats

        width_rows = max([len(row) for summary_dict in cluster_summary_dict.values() for row in summary_dict['table_rows']]) + 4

        for node_type, node_type_summary_dict in cluster_summary_dict.items():
            gmem, free_stats = node_type_summary_dict['gmem'], node_type_summary_dict['free_stats']
            res = "```"
            res += f"         free_gpu   free_cpu       free_mem    users".ljust(width_rows) + "\n"
            res += "\n".join(f"{row}".ljust(width_rows) for row in node_type_summary_dict['table_rows'])
            res += "```"
            blocks.append({
                "type": "context",
                "elements": [
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
    else:
        logger.warning("No Nodes found!")
        return []


def get_node_user_blocks():
    if job_dict := get_slum_job_dict():
        node_dict = get_slum_node_dict()
        node_dict_gpu_grouped = extract_useful_node_info_dict(node_dict)
        node2nodeinfo = {node_name: {"gpu_name": node_type, "gpu_mem": node_info.gmem} for node_type, node_dict in node_dict_gpu_grouped.items() for node_name, node_info in node_dict.items()}
        gpu2gmem = {node_type: node_info.gmem for node_type, node_dict in node_dict_gpu_grouped.items() for node_name, node_info in node_dict.items()}

        node_dict_user_grouped = defaultdict(lambda: defaultdict(int))
        for job_id, job_info in job_dict.items():

            if job_info["job_state"] == "RUNNING" and job_info["partition"] != "compute":
                node_dict_user_grouped[pwd.getpwuid(job_info["user_id"]).pw_name]["total"] += 1
                if job_info["batch_flag"] == 0:
                    node_dict_user_grouped[pwd.getpwuid(job_info["user_id"]).pw_name]["shell"] += 1
                if job_info["run_time"] < 8 * 60 * 60:
                    node_dict_user_grouped[pwd.getpwuid(job_info["user_id"]).pw_name]["hrs8"] += 1
                node_dict_user_grouped[pwd.getpwuid(job_info["user_id"]).pw_name][node2nodeinfo[job_info["batch_host"]]['gpu_name']] += 1

        unknown_gpus = set([node2nodeinfo[k]['gpu_name'] for k in node_dict.keys() if k in node2nodeinfo]).difference(gpu_order)
        new_gpus = sorted(unknown_gpus) + gpu_order[:5]
        all_gpus = sorted(unknown_gpus) + gpu_order
        g48_gpus = {k for k in all_gpus if gpu2gmem[k] == "48G"}

        blocks = []
        res = "```"
        len_user = max([len(user) for user in node_dict_user_grouped.keys()])
        for user, value in node_dict_user_grouped.items():
            new = sum([value[gpu] for gpu in new_gpus])
            g48 = sum([value[gpu] for gpu in g48_gpus])
            value['new'] = new
            value['g48'] = g48

        for user, value in sorted(node_dict_user_grouped.items(), key=lambda x: (-x[1]["total"], -x[1]["g48"], -x[1]["new"], -x[1]["shell"], x[1]["hrs8"], x[0])):
            row = f"{user}".ljust(len_user + 1)
            row += f' | total={value["total"]:<2} | new={value["new"]:<2} | 48g={value["g48"]:<2} | shell={value["shell"]:<2} | <8h={value["new"]:<2} | '
            row += " ".join([f'{node_type}={value[node_type]}' for node_type in all_gpus if value[node_type] > 0])
            res += f"{row}   \n"
        res += "```"
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": res,
            }],
        })
        return blocks
    else:
        logger.warning("No Nodes found!")
        return []


def get_user_jobs_blocks(unix_user_name, state="RUNNING"):
    if job_dict := get_slum_job_dict():
        node_dict_gpu_grouped = extract_useful_node_info_dict(get_slum_node_dict())
        node2nodeinfo = {node_name: (node_type, node_info.gmem) for node_type, node_dict in node_dict_gpu_grouped.items() for node_name, node_info in node_dict.items()}

        blocks = []
        table = []
        table += [f"job_id partition job_name user run_time tot_time priority gpu type gmem nodes(reason)".split()]
        for job_id, job_info in job_dict.items():
            if job_info["job_state"] == state and job_info["partition"] != "compute":
                if unix_user_name is None or pwd.getpwuid(job_info["user_id"]).pw_name == unix_user_name:
                    num_gpus = job_info["tres_req_str"].split(",")[-1].split("=")[-1]
                    reason = "" if job_info["state_reason"] == 'None' else f"({job_info['state_reason']})"
                    table += [[str(job_id),
                               job_info['partition'],
                               job_info['name'][:20],
                               pwd.getpwuid(job_info["user_id"]).pw_name,
                               job_info['run_time_str'],
                               job_info['time_limit_str'],
                               str(job_info['priority']),
                               str(num_gpus),
                               node2nodeinfo.get(job_info['batch_host'], ['']*2)[0],
                               node2nodeinfo.get(job_info['batch_host'], ['']*2)[1],
                               f"{'' if job_info['batch_host'] is None else job_info['batch_host']} {reason}"]]

        table = zip(*table)
        table = [x for x in table if any(x[1:])]
        table = list(zip(*table))
        if len(table) > 1:
            table[1:] = sorted(table[1:], key=lambda x: (x[1], -int(x[6]), int(x[0])))
            padding = [max(map(len, col)) for col in zip(*table)]
            res = '```'
            padded_table_t = [[f"{x.rjust(l)}" for x in col] for col, l in zip(zip(*table), padding)]
            res += "\n".join(["   ".join(row) for row in zip(*padded_table_t)])
            res += '```'
        else:
            res = f"No {state.lower()} jobs!"
        blocks.extend([{
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"*{state.title()} Jobs*"
                }
            ]
        }, {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": res,
            }],
        }])

        return blocks
    else:
        logger.warning("job_dict is empty!")
        return []
