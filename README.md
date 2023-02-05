# `susbot`: Slurm and other Utilities Slack Bot
[WIP] Provides helper commands related to an ML research Slack group. Mostly related to a slurm cluster. 

Available commands:
- `/cluster` - Get summary of the nodes in the cluster


## Getting Started

### Prerequisites
Create a Slack bot app with appropriate permissions (`chat:write`, `command`) and get the Slack tokens. You can find more information on how to do this [here](https://api.slack.com/start/building/bolt-python).


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
export SLURM_INCLUDE_DIR=/opt/slurm/include
export SLURM_LIB_DIR=/opt/slurm/lib64
git clone https://github.com/PySlurm/pyslurm.git && cd pyslurm
git checkout 21.08
pip install .
```

### Running the app
```commandline 
python app.py
```