import re
import pwd
import time

from collections import defaultdict
from types import SimpleNamespace

from cluster.query_slurm import get_slum_node_dict, get_slum_job_dict
from config import NEW_GPU_DISPLAY_ORDER, OLD_GPU_DISPLAY_ORDER
from utils.log import get_logger
from utils.utils import sizeof_fmt

logger = get_logger(__name__)


def extract_useful_node_info(value_dict):
    gmem = re.findall(r"gmem\d+?G", value_dict["features"])
    gmem = gmem[0][4:] if len(gmem) > 0 else None
    partitions = []
    for p in ["ddp-4way", "ddp-2way", "gpu", "low-prio-gpu"]:
        if p in value_dict["partitions"]:
            partitions.append(p.replace("low-prio-gpu", "lp").replace("-4way", "4").replace("-2way", "2"))

    state = value_dict["state"].lower()
    state = "+".join([(x[:3].replace("dra", "drn").replace("all", "aloc") if len(x) > 3 else x) for x in state.split("+")])

    node_info_dict = {
        "gpu_total": int(value_dict["gres"][0].split(":")[2].split("(")[0]),
        "gpu_used": int(value_dict["gres_used"][0].split(":")[2].split("(")[0]),
        "cpu_total": int(value_dict["cpus"]),
        "cpu_used": int(value_dict["alloc_cpus"]),
        "state": state[:8],
        "partitions": ",".join(partitions),
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


def extract_useful_node_info_dict(node_dict, lp_node_info):
    node_dict_gpu_grouped = defaultdict(dict)
    for key, value_dict in node_dict.items():
        if len(value_dict['gres']) == 0:  # no gpu in the node
            continue
        else:
            if len(value_dict['gres']) > 1:
                logger.warning(f"gres length > 1 {value_dict['gres']}")

            node_type = value_dict['gres'][0].split(":")[1]
            node_dict_gpu_grouped[node_type][key] = extract_useful_node_info(value_dict)

            node_dict_gpu_grouped[node_type][key].gpu_lp = lp_node_info[key]['gpu']
            node_dict_gpu_grouped[node_type][key].cpu_lp = lp_node_info[key]['cpu']
            node_dict_gpu_grouped[node_type][key].mem_lp = sizeof_fmt(lp_node_info[key]['mem'], suffix=node_dict_gpu_grouped[node_type][key].mem_unit, with_unit=False)

    return node_dict_gpu_grouped

def extract_cpu_info(job_info, cpu):
    return {'gpu': sum([int(req_str.split("=")[-1]) for req_str in job_info["tres_req_str"].split(",") if req_str.startswith("gres/gpu")]),
     'cpu': cpu,
     'mem_per_cpu': job_info['mem_per_cpu'],
     'min_memory_cpu': job_info['min_memory_cpu'],
     'mem_per_node': job_info['mem_per_node'],
     'min_memory_node': job_info['min_memory_node']
     }

def get_lp_node_info():
    lp_node_info = defaultdict(lambda: defaultdict(int))
    if job_dict := get_slum_job_dict():
        lp_nodes = [{k1: extract_cpu_info(v, v1) for k1, v1 in
                     v['cpus_allocated'].items()} for k, v in job_dict.items() if v['partition'] == 'low-prio-gpu']

        for lp_node_dict in lp_nodes:
            for node_name, node in lp_node_dict.items():
                for k in ['cpu', 'gpu']:
                    lp_node_info[node_name][k] += node[k]
                if node['mem_per_cpu']:
                    lp_node_info[node_name]['mem'] += node['min_memory_cpu'] * node['gpu']
                elif node['mem_per_node']:
                    lp_node_info[node_name]['mem'] += node['min_memory_node']
    return lp_node_info


def get_node_info_blocks(ignore_full_node=False):
    if node_dict := get_slum_node_dict():
        lp_node_info = get_lp_node_info()
        node_dict_gpu_grouped = extract_useful_node_info_dict(node_dict, lp_node_info)
        blocks = []
        job_dict = get_slum_job_dict()
        node_user_dict = defaultdict(set)
        for job_id, job_info in job_dict.items():
            if job_info["job_state"] == "RUNNING":
                node_user_dict[job_info["batch_host"]].add(pwd.getpwuid(job_info["user_id"]).pw_name)

        cluster_summary_dict = defaultdict(dict)
        gpu_display_order = [gpu for gpu in NEW_GPU_DISPLAY_ORDER + OLD_GPU_DISPLAY_ORDER if gpu in node_dict_gpu_grouped]

        for node_type in sorted(set(node_dict_gpu_grouped.keys()).difference(gpu_display_order)) + gpu_display_order:
            node_dict = node_dict_gpu_grouped[node_type]
            gmem = None
            cluster_summary_dict[node_type] = {}
            rows = []
            prev_partition = None
            for key, value in sorted(node_dict.items(), key=lambda x: ([-ord(c) for c in x[1].partitions], -x[1].gpu_free, -x[1].gpu_lp, x[0])):
                if gmem is None:
                    gmem = value.gmem
                if ignore_full_node and value.gpu_free == 0:
                    continue
                res = f"---\n" if prev_partition and prev_partition != value.partitions else ""
                res += f"{key}    "
                res += f'{value.partitions:>7}       '
                res += f'{value.gpu_free:>1}/{value.gpu_total:>1}     '
                res += f'{value.gpu_lp:>1}/{value.gpu_total:>1}    '
                res += f'{value.cpu_free:>3}/{value.cpu_total:>3}  '
                res += f'{value.cpu_lp:>3}/{value.cpu_total:>3}   '
                res += f'{value.mem_free:>3}/{value.mem_total:>3}{value.mem_unit}  '
                res += f'{value.mem_lp:>3}/{value.mem_total:>3}{value.mem_unit}  '
                res += f'{value.state:>8}   '
                res += f"{','.join(sorted(node_user_dict[key]))}" if len(node_user_dict[key]) > 0 else "--"
                rows.append(res)
                prev_partition = value.partitions

            cluster_summary_dict[node_type]['table_rows'] = rows
            cluster_summary_dict[node_type]['gmem'] = gmem
            free_stats = f"{sum(v.gpu_free for v in node_dict.values())}/{sum(v.gpu_total for v in node_dict.values())}"
            cluster_summary_dict[node_type]['free_stats'] = free_stats
            lp_stats = f"{sum(v.gpu_lp for v in node_dict.values())}/{sum(v.gpu_total for v in node_dict.values())}"
            cluster_summary_dict[node_type]['lp_stats'] = lp_stats

        width_rows = max([len(row) for summary_dict in cluster_summary_dict.values() for row in summary_dict['table_rows']]) + 4

        for node_type, node_type_summary_dict in cluster_summary_dict.items():
            gmem, free_stats, lp_stats = node_type_summary_dict['gmem'], node_type_summary_dict['free_stats'], node_type_summary_dict['lp_stats']
            res = "```"
            res += f"         partition  free_gpu  lp_gpu   free_cpu   lp_cpu   free_mem    lp_mem     state   users".ljust(width_rows) + "\n"
            rows = [f"{row}".ljust(width_rows) for row in node_type_summary_dict['table_rows']]
            res += "\n".join(rows)
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
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Low Priority*: {lp_stats}"
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


def get_node_user_blocks(title, ignore_partition=("compute"), limit=40):
    if job_dict := get_slum_job_dict():
        node_dict = get_slum_node_dict()
        lp_node_info = get_lp_node_info()
        node_dict_gpu_grouped = extract_useful_node_info_dict(node_dict, lp_node_info)
        node2nodeinfo = {node_name: {"gpu_name": node_type, "gpu_mem": node_info.gmem} for node_type, node_dict in node_dict_gpu_grouped.items() for node_name, node_info in node_dict.items()}
        gpu2gmem = {node_type: node_info.gmem for node_type, node_dict in node_dict_gpu_grouped.items() for node_name, node_info in node_dict.items()}

        node_dict_user_grouped = defaultdict(lambda: defaultdict(int))
        for job_id, job_info in job_dict.items():
            if job_info["job_state"] == "RUNNING" and job_info["partition"] not in ignore_partition:

                num_gpus = sum([int(req_str.split("=")[-1]) for req_str in job_info["tres_req_str"].split(",") if req_str.startswith("gres/gpu")])
                node_dict_user_grouped[pwd.getpwuid(job_info["user_id"]).pw_name]["total"] += num_gpus
                if job_info["batch_flag"] == 0:
                    node_dict_user_grouped[pwd.getpwuid(job_info["user_id"]).pw_name]["shell"] += num_gpus
                if job_info["run_time"] >= 24 * 60 * 60:
                    node_dict_user_grouped[pwd.getpwuid(job_info["user_id"]).pw_name]["hrs24"] += num_gpus
                node_dict_user_grouped[pwd.getpwuid(job_info["user_id"]).pw_name][node2nodeinfo[job_info["batch_host"]]['gpu_name']] += num_gpus
        new_gpu_display_order = [gpu for gpu in NEW_GPU_DISPLAY_ORDER if gpu in gpu2gmem]
        gpu_display_order = [gpu for gpu in NEW_GPU_DISPLAY_ORDER + OLD_GPU_DISPLAY_ORDER if gpu in gpu2gmem]
        unknown_gpus = set([node2nodeinfo[k]['gpu_name'] for k in node_dict.keys() if k in node2nodeinfo]).difference(gpu_display_order)
        new_gpus = sorted(unknown_gpus) + new_gpu_display_order
        all_gpus = sorted(unknown_gpus) + gpu_display_order
        g48_gpus = {k for k in all_gpus if gpu2gmem[k] == "48G"}
        blocks = []
        res = ""
        len_user = max([len(user) + (0 if value["total"] <= limit else 4) for user, value in node_dict_user_grouped.items()], default=15)
        for user, value in node_dict_user_grouped.items():
            new = sum([value[gpu] for gpu in new_gpus])
            g48 = sum([value[gpu] for gpu in g48_gpus])
            value['new'] = new
            value['g48'] = g48

        for user, value in sorted(node_dict_user_grouped.items(), key=lambda x: (-x[1]["total"], -x[1]["g48"], -x[1]["new"], -x[1]["shell"], x[1]["hrs24"], x[0])):
            row = f"{user}".ljust(len_user + 1)
            row = row if value["total"] <= limit else f"{row[:-4]} <!>"
            row += f' | total={value["total"]:<2} | newer={value["new"]:<2} | 48g={value["g48"]:<2} | shell={value["shell"]:<2} | ≥24h={value["hrs24"]:<2} | '
            row += " ".join([f'{node_type}={value[node_type]}' for node_type in all_gpus if value[node_type] > 0])
            res += f"{row}   \n"
        res += ""
        blocks.extend([

            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"*{title}* _(limit={limit})_"
                    }
                ]
            },
            {
                "type": "context",
                "elements": [{
                    "type": "mrkdwn",
                    "text": "```" + res + "```" if res else "No running jobs\n",
                }],
            }])
        return blocks
    else:
        logger.warning("No Nodes found!")
        return []


def get_job_usage(job_info):
    node = [extract_cpu_info(job_info, v1) for k1, v1 in job_info['cpus_allocated'].items()]
    usage = {}
    if len(node) > 0:
        for k in ['cpu', 'gpu']:
            usage[k] = str(node[0][k])
        if node[0]['mem_per_cpu']:
            usage['mem'] = node[0]['min_memory_cpu'] * node[0]['gpu']
        elif node[0]['mem_per_node']:
            usage['mem'] = node[0]['min_memory_node']
        usage['mem'] = sizeof_fmt(usage['mem'], with_unit=True)
        usage['mem'] = f"{usage['mem'][0]}{usage['mem'][1]}"
    else:
        usage = {"cpu": None, "gpu": None, "mem": None}
    return usage

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def get_user_jobs_blocks(unix_user_name, state="RUNNING"):
    if job_dict := get_slum_job_dict():
        node_dict_gpu_grouped = extract_useful_node_info_dict(get_slum_node_dict(), get_lp_node_info())
        node2nodeinfo = {node_name: (node_type, node_info.gmem) for node_type, node_dict in node_dict_gpu_grouped.items() for node_name, node_info in node_dict.items()}

        blocks = []
        rows = []
        for job_id, job_info in job_dict.items():
            if job_info["job_state"] == state and job_info["partition"] != "compute":
                if unix_user_name is None or pwd.getpwuid(job_info["user_id"]).pw_name == unix_user_name:
                    num_gpus = sum([int(req_str.split("=")[-1]) for req_str in job_info["tres_req_str"].split(",") if req_str.startswith("gres/gpu")])
                    reason = "" if job_info["state_reason"] == 'None' else f"({job_info['state_reason']})"[:20]

                    if job_info["start_time"] != 0:
                        start_time = time.strftime("%d %b %H:%M", time.gmtime(job_info["start_time"]))
                        end_time = time.strftime("%d %b %H:%M", time.gmtime(job_info["end_time"]))
                    else:
                        start_time = "N/A"
                        end_time = "N/A"
                    node_info = node2nodeinfo.get(job_info['batch_host'], [''] * 2)
                    job_usage = get_job_usage(job_info)
                    rows.append({
                        "job_id": str(job_id),
                        "part": job_info['partition'].replace("low-prio", "lp"),
                        "job_name": job_info['name'][:20],
                        "user": pwd.getpwuid(job_info["user_id"]).pw_name,
                        "total_time": job_info['time_limit_str'],
                        "run_time": job_info['run_time_str'],
                        "start_time": start_time,
                        "end_time": end_time,
                        "prio": str(job_info['priority']), # 8
                        "gpu": str(num_gpus),
                        "type": node_info[0],
                        "gmem": node_info[1],
                        "cpu": job_usage['cpu'],
                        "mem": job_usage['mem'],
                        "nodes(reason)": f"{'' if job_info['batch_host'] is None else job_info['batch_host']} {reason}"
                    })

        if len(rows) > 0:
            # remove empty columns
            header_keys = set([k for row in rows for k, v in row.items() if v])
            header_keys = [k for k in rows[0].keys() if k in header_keys]

            rows = sorted(rows, key=lambda x: (x['part'], -int(x['prio']), int(x['job_id'])))
            padding = {k: max(map(len, v)) for k, v in {k: [k] + [dic[k] for dic in rows] for k in header_keys}.items()}
            table_rows_all = [[f"{row[k].rjust(padding[k])}" for k in header_keys] for row in rows]
            header_keys = [k.rjust(padding[k]) for k in header_keys]

            chunk_size = 75
            res_list = []
            for new_chunk_rows in chunks(table_rows_all, chunk_size):
                table_rows = [header_keys] + new_chunk_rows
                res = '```'
                res += "\n".join(["  ".join(row) for row in table_rows])
                res += '```'
                res_list.append(res)
        else:
            res_list = [f"No {state.lower()} jobs!"]
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"*{state.title()} Jobs*"
                    }
                ]
            }
        )
        blocks.extend([{
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": res,
            }],
        } for res in res_list])

        return blocks
    else:
        logger.warning("job_dict is empty!")
        return []
