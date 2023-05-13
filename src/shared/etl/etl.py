from typing import Dict

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession

from . import (
    extractors,
    loaders,
    transformers
)
from . import fileloaders


class ETLService:
    """Entry point for ETL service.
    """

    def __init__(self):
        self._file_loader = fileloaders.SparkJSONFileLoader()

    def load_etl_data(self, params=None):
        return self._file_loader.load_file('etl', params=params)

    def create_etl(self, spark: SparkSession, etl_params=None):
        etl_data = self.load_etl_data(etl_params)
        resolved = ETLResolver(spark, etl_data).resolve()
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

    def resolve_extractor(self) -> extractors.Extractor:
        """
        """
        extract_data = self.etl_data['extract']
        return extractors.factory.create(spark=self.spark, **extract_data)

    def resolve_transformer(self) -> Pipeline:
        """
        """
        transform_data = self.etl_data['transform']
        stages = []

        for stage in transform_data['stages']:
            trans = transformers.factory.create(**stage)
            stages.append(trans)

        return Pipeline(stages=stages)

    def resolve_loader(self) -> loaders.Loader:
        pass

    def resolve(self) -> Dict:
        """
        """
        extractor = self.resolve_extractor()
        transformer = self.resolve_transformer()

        return {
            'extractor': extractor,
            'transformer': transformer
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
            extractor: extractors.Extractor,
            loader: loaders.Loader,
            transformer: transformers.Transformer =
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
        return self.transformer.transform(data)
