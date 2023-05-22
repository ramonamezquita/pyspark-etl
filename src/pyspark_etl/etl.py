from typing import Dict, Union

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession

from .extractors import Extractor
from .loaders import Loader
from .object_factory import SubclassesFactory
from .transformers import TransformerBaseClass, IdentityTransformer


class PySparkETL:
    """PySparkETL.

    The :meth:`extract()`, :meth:`transform()` and :meth:`load()` methods are
    just wrappers around the same methods of the respective :class:`Extractor`,
    :class:`Transformation` and :class:`Loader` instances.

    Parameters
    ----------
    extractor : Extractor
        Object implementing Extractor interface (:meth:`extract`).

    loader : Loader
        Object implementing Loader interface (:meth:`loader`).

    transformer TransformerBaseClass
        Object implementing Transformer interface (fit/transform methods).
    """

    def __init__(
            self,
            extractor: Extractor,
            loader: Loader,
            transformer: TransformerBaseClass = IdentityTransformer()
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


class ETLCreator:
    """Creates :class:`ETL` instance from config dict.

    Parameters
    ----------
    config : dict
    """

    def __init__(self, config: Dict, spark: SparkSession = None):

        self.config = config
        self._spark = spark

        self._factories = {}
        self._register_factories()
        self._check_config()

    def set_spark(self, spark: SparkSession):
        """Sets spark session.
        """
        self._spark = spark

    def _check_config(self):
        # TODO
        pass

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
            'transformers': TransformerBaseClass,
            'loaders': Loader
        }

        for name, cls in base_classes.items():
            self._factories[name] = SubclassesFactory(cls)

    def create_extractor(self) -> Extractor:
        """
        """
        if self._spark is None:
            raise ValueError(
                'Cannot resolve Extractor without spark session. '
                'Use spark parameter at __init__ or :meth:`set_spark` '
                'to set a spark session.')

        factory = self.get_factory('extractors')
        extract_data = self.config['extract']
        return factory(spark=self._spark, **extract_data)

    def create_transformer(self) -> Pipeline:
        """
        """
        factory = self.get_factory('transformers')
        transform_data = self.config['transform']
        stages = []

        for stage in transform_data['stages']:
            trans = factory(**stage)
            stages.append(trans)

        return Pipeline(stages=stages)

    def create_loader(self) -> Loader:
        factory = self.get_factory('loaders')
        load_data = self.config['load']
        return factory(**load_data)

    def create(self) -> ETL:
        extractor = self.create_extractor()
        transformer = self.create_transformer()
        loader = self.create_loader()
        return ETL(extractor, loader, transformer)
