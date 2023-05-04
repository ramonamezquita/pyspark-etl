from typing import List, Tuple, Dict, Union

from . import sparkconfigs


class CloudConfig:
    """CloudConfig objects store spark configuration values to establish
    connection with cloud services.

    Parameters
    ----------
    conf : list
        List of SparkConfigs
    """

    def __init__(self, conf: List[sparkconfigs.SparkConfig]):
        self._conf = conf

    def dict(self) -> Dict:
        d = {}
        for conf in self._conf:
            d[conf.key] = conf.value
        return d

    def pairs(self) -> List[Tuple]:
        pairs = []
        for config in self._conf:
            pairs.append((config.key, config.value))

        return pairs

    def get_from_key(self, key) -> Union[None, sparkconfigs.SparkConfig]:
        for conf in self._conf:
            if key == conf.key:
                return conf


class GoogleConfig(CloudConfig):
    """Google Context.
    """

    def __init__(self):
        conf = [
            sparkconfigs.GoogleAFS(),
            sparkconfigs.GoogleAuthEnable(),
            sparkconfigs.GoogleAuthKeyfile()
        ]
        super().__init__(conf)
