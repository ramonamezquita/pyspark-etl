import json
import os
from typing import Union, Dict

from pyspark import SparkFiles


def exists(filepath: str) -> bool:
    """Checks if the file exists.

    Parameters
    ----------
    filepath : str
        The *full* path to the file.

    Returns
    -------
    exists: bool
        True if file path exists, False otherwise.
    """
    return os.path.isfile(filepath)


class FileReader:
    def __init__(self, extension: str, root: str = ""):
        self.extension = extension
        self.root = root

    def get_full_path(self, filepath):
        """Returns full path including the root and extension.

        That is, <root>/<filepath>.<extension>

        Parameters
        ----------
        filepath : str
            The path to the file (relative to ``root``) to load without the
            extension.

        """
        return os.path.join(self.root, filepath) + self.extension

    def read(self, filepath: str) -> Union[bytes, None]:
        """Reads file.

        Parameters
        ----------
        filepath : str
            The path to the file (relative to ``root``) to load without the
            extension.
        """
        full_path = self.get_full_path(filepath)
        if exists(full_path):
            with open(full_path, 'rb') as file:
                return file.read()


class JSONFileLoader:
    """Loads json files from given root.

    Parameters
    ----------
    root : str
        Root file path.
    """

    def __init__(self, root=""):
        self._reader = FileReader(extension='.json', root=root)

    def load_file(self, filepath: str, encoding='utf-8', params=None) -> Dict:
        """
        Parameters
        ----------
        filepath : str
            The path to the file (relative to ``root``) to load without the
            extension.

        encoding : str, default='utf-8'

        params : dict
            Params to be replaced.

        Returns
        -------
        file : dict
            Loaded json file.
        """
        file = self.read(filepath).decode(encoding)

        if params is not None:
            file = self.replace_params(file, params)

        return json.loads(file)

    def replace_params(self, file: str, params: Dict):
        """Replaces params in file.

        Parameters
        ----------
        file : str
            Stringified file (after decoding).

        params : dict
            Params to be replaced.
        """
        for key, val in params.items():
            key = '{' + key + '}'
            file = file.replace(key, val)

        return file

    def read(self, filepath):
        return self._reader.read(filepath)


class SparkJSONFileLoader(JSONFileLoader):

    def __init__(self):
        super().__init__(root=SparkFiles.getRootDirectory())
