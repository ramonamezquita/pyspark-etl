import logging

from . import (
    extractors,
    loaders,
    transformers
)


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
            transformers.IdentityTransformer(),
            log_level=logging.INFO,
            logger=None
    ):
        self.extractor = extractor
        self.loader = loader
        self.transformer = transformer

        if logger:
            self.logger = logger
            self.logger.setLevel(log_level)

    def etl(self, filepath: str, **kwargs):
        return self.load(self.transform(self.extract(filepath, **kwargs)))

    def extract(self, filepath: str, **kwargs):
        """Extracts data using the associated :class:`Extractor`.
        """

        return self.extractor.extract(filepath, **kwargs)

    def load(self, data):
        """Loads data using the associated :class:`Loader`.
        """

        return self.loader.load(data)

    def transform(self, data):
        """Transforms the data using the associated :class:`Transformer`.
        """
        return self.transformer.transform(data)
