# OpenAlex API Extractor

## Clone this repo

Type following command in you terminal:

```bash
# clone this repo in your machine
git clone https://github.com/hussainmahmood/openalex.git

# move to project folder
cd openalex
```

## Make your own environment variables file

Create a file named .env in root folder.

Copy the contents of .env-smaple to your newly created .env file and update the variables according to your need.

## Setup project

Once you've cloned this project, create a virtual environment and install dependencies:

```bash
# create a virtual environment
virtualenv .venv

# source your virtual environment
# Linux
source .venv/bin/activate

# Windows Powershell
.venv/Scripts/activate

# Command prompt
call .venv/Scripts/activate

# install dependencies
pip install -r requirements.txt
```

## Run extractor

```bash
# run extraction script
python extractor.py

```

