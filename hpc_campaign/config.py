#!/usr/bin/env python3

from dataclasses import dataclass
from os.path import expanduser

import yaml

ACA_VERSION = "0.6"
# 0.2 added key encryption (added table key, modfified table bpdataset)
# 0.3 generate UUID for each bpdataset (modified table bpdataset)
# 0.4 added h5dataset table
# 0.5 reorganized to "dataset" table, plus text and images, replicas, archives
# 0.6 redefines "archive" as TAR files, throws away 0.5's archive concept

REDIS_PORT = 6379


@dataclass
class Config:
    """User config and Hosts config file parser"""

    def __init__(self):
        self.campaign_store_path: str = ""
        self.host_name: str = ""
        self.cache_path: str = ""
        self.verbose: int = 0
        path = expanduser("~/.config/hpc-campaign/config.yaml")
        try:
            doc = {}
            with open(path, encoding="utf-8") as f:
                doc = yaml.safe_load(f)
            camp = doc.get("Campaign")
            if isinstance(camp, dict):
                for key, value in camp.items():
                    if key == "campaignstorepath":
                        self.campaign_store_path = expanduser(value)
                    if key == "hostname":
                        self.host_name = value
                    if key == "cachepath":
                        self.cache_path = value
                    if key == "verbose":
                        self.verbose = value
        except FileNotFoundError:
            pass  # An empty except block

    def read_host_config(self) -> dict:
        path = expanduser("~/.config/hpc-campaign/hosts.yaml")
        doc = {}
        try:
            with open(path, encoding="utf-8") as f:
                doc = yaml.safe_load(f)
        except FileNotFoundError:
            pass  # An empty except block
        return doc
