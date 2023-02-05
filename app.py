import os
from datetime import datetime

from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_bolt.app import App

from utils.log import setup_logger
from cluster.node import get_node_info_blocks

logger = setup_logger()

app = App(token=os.environ["SLACK_BOT_TOKEN"],
          signing_secret=os.environ["SLACK_APP_TOKEN"],
          logger=logger)


@app.command("/cluster")
def scan_cluster(ack, body):
    user_id = body["user_id"]
    ack(
        blocks=[{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Hi <@{user_id}>* :wave:\nHere is the GPU availability summary ({datetime.now().strftime('%m/%d/%Y, %H:%M:%S')})",
            }
        }, *get_node_info_blocks()]
    )


if __name__ == "__main__":
    SocketModeHandler(app).start()
