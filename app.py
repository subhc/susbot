import os
import traceback
from datetime import datetime

from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_bolt.app import App

import config
from utils.log import setup_logger
from cluster.node import get_node_info_blocks, get_node_user_blocks, get_user_jobs_blocks
from utils.slack2unix import get_slack2unix_map

logger = setup_logger(output=config.LOGGER_OUTPUT, level=config.LOGGER_LEVEL)

app = App(token=os.environ["SLACK_BOT_TOKEN"],
          signing_secret=os.environ["SLACK_APP_TOKEN"],
          logger=logger)


def get_home_tab_blocks(user_id):
    try:
        unix_user = get_slack2unix_map().get(user_id, None)
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Hi <@{user_id}>* :wave: ",
                }
            }, {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Last updated: {datetime.now().strftime('%m/%d/%Y, %H:%M:%S')} \n"
                },
                "accessory": {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Refresh",
                        "emoji": True
                    },
                    "value": "refresh_home",
                    "action_id": "action_refresh_home"
                }
            }, {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*GPU Cluster Summary:*",
                }
            }, *get_node_info_blocks()]

        if unix_user:
            blocks += [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*User Summary:*",
                    }
                }, *get_node_user_blocks("All GPUs", limit=52),
                *get_node_user_blocks("Non-preemptible GPUs", ignore_partition=["compute", "low-prio-gpu"], limit=12),
                *get_node_user_blocks("Preemptible GPUs", ignore_partition=["compute", "ddp-4way", "ddp-2way", "gpu"], limit=40),
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Your Jobs ({unix_user}):*\n",
                    }
                }, *get_user_jobs_blocks(unix_user, state='RUNNING')
                , *get_user_jobs_blocks(unix_user, state='PENDING'),
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Waiting in Cluster:*\n",
                    }
                }, *get_user_jobs_blocks(unix_user_name=None, state='PENDING'),
            ]
        else:
            blocks.extend(get_no_account_found_blocks())
        blocks.extend([
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": " ",
                },
                "accessory": {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Notes",
                        "emoji": True
                    },
                    "value": "readme",
                    "action_id": "action_readme",
                }
            }]
        )
        return blocks
    except Exception:
        logger.error(f"Failed to get home tab blocks")
        traceback.print_exc()
        return []


@app.action("action_refresh_home")
def refresh_home(ack, body, client):
    user_id = body["user"]["id"]
    ack()
    client.views_update(
        view_id=body["view"]["id"],
        view={
            "type": "home",
            "blocks": get_home_tab_blocks(user_id)
        }
    )


@app.event("app_home_opened")
def update_home_tab(client, event, logger):
    try:
        user_id = event["user"]
        # Call views.publish with the built-in client
        client.views_publish(
            # The user that opened your app's app home
            user_id=user_id,
            # The view object that appears in the app home
            view={
                "type": "home",
                "blocks": get_home_tab_blocks(user_id)
            }
        )
    except Exception:
        logger.error(f"Failed to publish home tab")
        traceback.print_exc()


def command_cluster_stats(user_id):
    unix_user = get_slack2unix_map().get(user_id, None)
    blocks = []
    if unix_user:
        return [{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Hi <@{user_id}>* :wave:\nHere is the GPU availability summary ({datetime.now().strftime('%m/%d/%Y, %H:%M:%S')})",
            }
        }, *get_node_info_blocks()]
    else:

        return get_no_account_found_blocks()


def get_no_account_found_blocks():
    return [{
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*Error:*",
        }
    }, {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"You do not seem to have a cluster account.\n"
                        f"Reason: Cannot find any account matching your Slack full name (not display name) in tritons's `/etc/passwd` database.\n"
                        f"Remedy: Run `getent passwd $USER` to look for your full name in triton and set the same on Slack."
            }
        ]
    }]


@app.command("/cluster")
def scan_cluster(ack, body):
    ack(blocks=command_cluster_stats(body["user_id"]))


@app.message("cluster")
def say_hello_regex(message, say):
    # logger.debug(message['text'])
    say(blocks=command_cluster_stats(message["user"]))


# Listen for a shortcut invocation
@app.action("action_readme")
def open_modal(ack, body, client):
    # Acknowledge the command request
    ack()
    # Call views_open with the built-in client
    client.views_open(
        # Pass a valid trigger_id within 3 seconds of receiving it
        trigger_id=body["trigger_id"],
        # View payload
        view={
            "type": "modal",
            # View identifier
            "callback_id": "readme",
            "title": {
                "type": "plain_text",
                "text": "Notes"
            },
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "1. The code is here: <https://github.com/subhc/susbot|github link>"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "2. If the app is not refreshing the server most likely is down. On clicking the  `Refresh` button if an exclamation mark appears beside it the server is down. Ping me. \nIf you are curious run `ls -ltra "
                                "/work/subha/apps/VGGBot/logs` and check the latest log "
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "3. If your jobs don't show up or the displayed linux username is wrong then the app has failed to find any account matching your Slack full name (not display name) in tritons's `/etc/passwd` database. \nTo fix, "
                                "run `getent passwd $USER` to look for your full name in triton and set the same on Slack.\nTake a look at the account matching logic <https://github.com/subhc/susbot/blob/main/utils/slack2unix.py|here> "
                    }
                }
            ],
            "type": "modal"
        }
    )


if __name__ == "__main__":
    SocketModeHandler(app).start()
