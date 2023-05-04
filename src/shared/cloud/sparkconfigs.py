import os
from abc import ABC
from typing import Optional


class SparkConfig(ABC):

    def __init__(self, key: str, value: Optional[str] = None):
        self.key = key
        self.value = value

    def get_config(self):
        return self.key, self.value

    def set_value(self, value):
        self.value = value


class GoogleAuthKeyfile(SparkConfig):
    """Google json key file.
    """

    def __init__(self, keyfile="/etc/cloudauth/google/keyfile.json"):
        value = os.environ.get('GOOGLE_KEYFILE', keyfile)
        super().__init__(
            key="spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            value=value
        )


class GoogleAuthEnable(SparkConfig):
    def __init__(self):
        super().__init__(
            key="spark.hadoop.google.cloud.auth.service.account.enable",
            value="true"
        )


class GoogleAFS(SparkConfig):
    """Google Abstract File System for Hadoop.
    """

    def __init__(self):
        super().__init__(
            key="spark.hadoop.fs.AbstractFileSystem.gs.impl",
            value="com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        )
