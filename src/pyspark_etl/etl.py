from typing import Dict, Union

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession

<<<<<<< Updated upstream:src/shared/etl/etl.py
from . import (
    extractors,
    loaders,
    transformers
)
from . import fileloaders


class ETLService:
    """Entry point for ETL service.
=======
from . import transformers
from .extractors import Extractor
from .loaders import Loader
from .object_factory import SubclassesFactory


class ETLCreator:
    """Factory for :class:`ETLs` instances.
>>>>>>> Stashed changes:src/pyspark_etl/etl.py
    """

    def __init__(self):
        self._file_loader = fileloaders.SparkJSONFileLoader()

<<<<<<< Updated upstream:src/shared/etl/etl.py
    def load_etl_data(self, params=None):
        return self._file_loader.load_file('etl', params=params)

    def create_etl(self, spark: SparkSession, etl_params=None):
        etl_data = self.load_etl_data(etl_params)
        resolved = ETLResolver(spark, etl_data).resolve()
=======
    def get_resolver(self):
        return self._resolver

    def create(self):
        resolved = self._resolver.resolve()
>>>>>>> Stashed changes:src/pyspark_etl/etl.py
        return ETL(**resolved)


class ETLResolver:
    """API for etl_data.

    Parameters
    ----------
    etl_data : dict
    """

    def __init__(self, spark: SparkSession, etl_data: Dict):
        self.spark = spark
        self.etl_data = etl_data
<<<<<<< Updated upstream:src/shared/etl/etl.py
=======

        self._factories = {}
        self._register_factories()
        self._check_etl_data()

    def _check_etl_data(self):
        pass
>>>>>>> Stashed changes:src/pyspark_etl/etl.py

    def get_factory(self, name: str) -> Union[SubclassesFactory, None]:
        """Returns factory.

        Parameters
        ----------
        name : str
            Name of the registered factory

        Returns
        -------
        SubclassesFactory or None
        """
        return self._factories.get(name)

    def _register_factories(self) -> None:
        """Registers factories.

        Once registered, factories are available through private method
        :meth:`_get_factory`.
        """

        base_classes = {
            'extractors': Extractor,
            'transformers': transformers.TransformerBaseClass,
            'loaders': Loader
        }

        for name, cls in base_classes.items():
            self._factories[name] = SubclassesFactory(cls)

    def resolve_extractor(self) -> Extractor:
        """
        """
        factory = self.get_factory('extractors')
        extract_data = self.etl_data['extract']
        return factory(spark=self.spark, **extract_data)

    def resolve_transformer(self) -> Pipeline:
        """
        """
        factory = self.get_factory('transformers')
        transform_data = self.etl_data['transform']
        stages = []

        for stage in transform_data['stages']:
            trans = factory(**stage)
            stages.append(trans)

        return Pipeline(stages=stages)

    def resolve_loader(self) -> Loader:
        factory = self.get_factory('loaders')
        load_data = self.etl_data['load']
        return factory(**load_data)

    def resolve(self) -> Dict:
        """
        """
        extractor = self.resolve_extractor()
        transformer = self.resolve_transformer()
        loader = self.resolve_loader()

        return {
            'extractor': extractor,
            'transformer': transformer,
            'loader': loader
        }


class ETL:
    """ETL class

    All ETL instances may be instantiated from this one.

    The class can be instantiated passing an :class:`Extractor`,
    a :class:`Transformation` and a :class:`Loader` instances,
    as arguments to the constructor:

            etl = ETL(
                extractor=ZIPCSVExctractor(
                        remote_url
                ),
                transformation=RegionCSV2CSVTransformation(),
                loader=CSVLoader(csv_path, label='governi')
            )



    The :meth:`extract()`, :meth:`transform()` and :meth:`load()` methods are
    just wrappers around the same methods of the respective :class:`Extractor`,
    :class:`Transformation` and :class:`Loader` instances.
    """

    def __init__(
            self,
            extractor: Extractor,
            loader: Loader,
            transformer: transformers.TransformerBaseClass =
            transformers.IdentityTransformer()
    ):
        self.extractor = extractor
        self.loader = loader
        self.transformer = transformer

    def execute(self):
        return self.load(self.transform(self.extract()))

    def extract(self):
        """Extracts data using the associated :class:`Extractor`.
        """

        return self.extractor.extract()

    def load(self, data):
        """Loads data using the associated :class:`Loader`.
        """

        return self.loader.load(data)

    def transform(self, data):
        """Transforms the data using the associated :class:`Transformer`.
        """
        return self.transformer.fit(data).transform(data)
