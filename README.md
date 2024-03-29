#  <img style="padding-top:10px" height=35 src="https://static.wikia.nocookie.net/among-us-wiki/images/c/c7/Red_old_design.png"></img> `susbot` : Slurm and other Utilities Slack Bot </span>

[WIP] Provides helper commands related to an ML research Slack group. Mostly related to a slurm cluster. 

Available commands:
- `/cluster` - Get summary of the nodes in the cluster


## Getting Started

### Prerequisites
Create a Slack bot app with appropriate scope (`chat:write`, `command`, `im:history`, `users:read`) and get the Slack tokens. Subscribe to events `app_home_opened`, `message.im`. You can find more information on how to do this [here](https://api.slack.com/start/building/bolt-python).


`SLACK_BOT_TOKEN` and `SLACK_SIGNING_SECRET` environment variables are required to run the app. 
- For `SLACK_BOT_TOKEN` copy the **Bot User OAuth Access Token** under the **OAuth & Permissions** sidebar. 
- `SLACK_SIGNING_SECRET` is available in your app's **Basic Information** page under **App Credentials**.
```commandline
export SLACK_BOT_TOKEN=xoxb-your-token
export SLACK_SIGNING_SECRET=your-signing-secret
```

### Installing dependencies
```commandline
conda env create -f environment.yaml
```
#### Install PySlurm
Follow the instructions [here](https://github.com/PySlurm/pyslurm)
```commandline
export SLURM_INCLUDE_DIR=/usr/include
export SLURM_LIB_DIR=/usr/lib64
git clone https://github.com/PySlurm/pyslurm.git && cd pyslurm
git checkout v23.2.0
pip install .
```

### Running the app

Run it on a node with access to the cluster.
```commandline 
python app.py
```

### Acknowledgments
- The cluster GUI is shameless rip-off of [slurm_web](https://github.com/TengdaHan/slurm_web). If you are looking for a web GUI for cluster profiling, check it out.
- [slurm_gpustat](https://github.com/albanie/slurm_gpustat)
