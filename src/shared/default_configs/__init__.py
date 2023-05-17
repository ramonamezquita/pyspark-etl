from typing import List, Dict, Union

from . import sparkconfigs


class _DefaultConfigIter:

    def __init__(self, default_config):
        self.default_config = default_config
        self._conf_size = len(default_config.conf)
        self._current_index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._current_index < self._conf_size:
            current_conf = self.default_config.conf[self._current_index]
            key, value = current_conf.key, current_conf.value
            self._current_index += 1
            return key, value

        raise StopIteration


class DefaultConfig:
    """:class:`DefaultConfig` collects spark configuration values into a single
    portable object.

    Parameters
    ----------
    conf : list
        List of SparkConfigs
    """

    def __init__(self, conf: List[sparkconfigs.SparkConfig]):
        self.conf = conf

    def __iter__(self):
        return _DefaultConfigIter(self)

    def dict(self) -> Dict:
        d = {}
        for conf in self.conf:
            d[conf.key] = conf.value
        return d

    def get(self, key) -> Union[None, sparkconfigs.SparkConfig]:
        for conf in self.conf:
            if key == conf.key:
                return conf


class Google(DefaultConfig):
    """Google Context.
    """

    def __init__(self):
        conf = [
            sparkconfigs.GoogleAFS(),
            sparkconfigs.GoogleAuthEnable(),
            sparkconfigs.GoogleAuthKeyfile()
        ]
        super().__init__(conf)
