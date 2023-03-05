import os
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
        }, *get_node_info_blocks(),
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*User Summary:*",
            }
        }, *get_node_user_blocks(),
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Waiting in Cluster:*\n",
            }
        }, *get_user_jobs_blocks(unix_user_name=None, state='PENDING'),
    ]
    if unix_user:
        blocks.extend([{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Your Jobs ({unix_user}):*\n",
            }
        }, *get_user_jobs_blocks(unix_user, state='RUNNING')
            , *get_user_jobs_blocks(unix_user, state='PENDING')
        ])
    else:
        blocks.extend([{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Your Jobs:*",
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
        }])
    return blocks


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
    except Exception as e:
        logger.error(f"Failed to publish home tab: {e}")


def command_cluster_stats(user_id):
    return [{
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*Hi <@{user_id}>* :wave:\nHere is the GPU availability summary ({datetime.now().strftime('%m/%d/%Y, %H:%M:%S')})",
        }
    }, *get_node_info_blocks()]


@app.command("/cluster")
def scan_cluster(ack, body):
    ack(blocks=command_cluster_stats(body["user_id"]))


@app.message("cluster")
def say_hello_regex(message, say):
    # logger.debug(message['text'])
    say(blocks=command_cluster_stats(message["user"]))


if __name__ == "__main__":
    SocketModeHandler(app).start()
