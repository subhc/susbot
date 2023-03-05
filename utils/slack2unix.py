import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from thefuzz import fuzz
from unidecode import unidecode
import pwd

from utils.log import get_logger
from utils.utils import cache_for_n_seconds

logger = get_logger(__name__)


def get_slack_users():
    client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))

    try:
        # Call the users.list method using the WebClient
        # users.list requires the users:read scope
        result = client.users_list()["members"]
        return result

    except SlackApiError as e:
        logger.error("Error creating conversation: {}".format(e))
        return []


@cache_for_n_seconds(seconds=24 * 60 * 60)
def get_slack2unix_map():
    slack2unix_map = {}
    slack_users = get_slack_users()
    for linux_user in pwd.getpwall():
        if linux_user.pw_uid < 100 or linux_user.pw_name.startswith('.'):
            continue
        dic = {}
        match = (None, (None, None))
        for slack_user in slack_users:
            if 'real_name' in slack_user:
                slack = unidecode(slack_user['real_name'].lower())
                linux = unidecode(linux_user.pw_gecos.lower())
                linux_fn = linux.rpartition(' ')[0]
                linux_sn = linux.rpartition(' ')[-1]
                slack_fn = slack.rpartition(' ')[0]
                slack_sn = slack.rpartition(' ')[-1]
                score = fuzz.partial_ratio(linux, slack)
                score += fuzz.partial_ratio(linux_fn, slack_fn) / 4
                score += fuzz.partial_ratio(linux_sn, slack_sn) / 10
                dic.update({slack_user['real_name']: (slack_user['id'], score)})
        if len(dic) > 0:
            match_ = sorted(dic.items(), key=lambda x: -x[1][1])[0]
            if match_[1][1] > 75:
                match = match_
        slack2unix_map.update({match[1][0]: linux_user.pw_name})
    return slack2unix_map
