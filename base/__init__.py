from json import load, dump
from dotenv import load_dotenv, find_dotenv
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler, BaseRotatingHandler
from packaging.version import Version, parse as parse_version
import time
import os
import ast
import requests
import zipfile
import io
import shutil
import regex
import subprocess
import schedule
import psutil
import sys
import threading
import glob
import re
import random
import zipfile
import platform
import fnmatch
import signal
from colorlog import ColoredFormatter
from ruamel.yaml import YAML


load_dotenv(find_dotenv('./config/.env'))

                    
def load_secret_or_env(secret_name, default=None):
    secret_file = f'/run/secrets/{secret_name}'
    try:
        with open(secret_file, 'r') as file:
            return file.read().strip()
    except IOError:
        return os.getenv(secret_name.upper(), default)

PLEXDEBRID = os.getenv("PD_ENABLED")
PDLOGLEVEL = os.getenv("PD_LOG_LEVEL")
PLEXUSER = load_secret_or_env('plex_user')
PLEXTOKEN = load_secret_or_env('plex_token')
JFADD = load_secret_or_env('jf_address')
JFAPIKEY = load_secret_or_env('jf_api_key')
RDAPIKEY = load_secret_or_env('rd_api_key')
ADAPIKEY = load_secret_or_env('ad_api_key')
GHTOKEN = load_secret_or_env('GITHUB_TOKEN')
SEERRAPIKEY = load_secret_or_env('seerr_api_key')
SEERRADD = load_secret_or_env('seerr_address')
PLEXADD = load_secret_or_env('plex_address')
ZURGUSER = load_secret_or_env('zurg_user')
ZURGPASS = load_secret_or_env('zurg_pass')
SHOWMENU = os.getenv('SHOW_MENU')
LOGFILE = os.getenv('PD_LOGFILE')
PDUPDATE = os.getenv('PD_UPDATE')
PDREPO = os.getenv('PD_REPO')
DUPECLEAN = os.getenv('DUPLICATE_CLEANUP')
CLEANUPINT = os.getenv('CLEANUP_INTERVAL')
RCLONEMN = os.getenv("RCLONE_MOUNT_NAME")
RCLONELOGLEVEL = os.getenv("RCLONE_LOG_LEVEL")
ZURG = os.getenv("ZURG_ENABLED")
ZURGVERSION = os.getenv("ZURG_VERSION")
ZURGLOGLEVEL = os.getenv("ZURG_LOG_LEVEL")
ZURGUPDATE = os.getenv('ZURG_UPDATE')
PLEXREFRESH = os.getenv('PLEX_REFRESH')
PLEXMOUNT = os.getenv('PLEX_MOUNT_DIR')
NFSMOUNT = os.getenv('NFS_ENABLED')
NFSPORT = os.getenv('NFS_PORT')
ZURGPORT = os.getenv('ZURG_PORT')
