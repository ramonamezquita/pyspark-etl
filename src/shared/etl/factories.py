from typing import Dict, Type, Any

from . import (
    extractors,
    transformers
)

EXTRACTORS = {
    'spark_csv': extractors.SparkCSVExtractor,
    'spark_parquet': extractors.SparkParquetExtractor
}

TRANSFORMERS = {
    'column_split': transformers.ColumnSplitTransformer
}


class ObjectFactory:
    def __init__(self, types: Dict[str, Type[Any]]):
        self._types = types

    def create(self, key, **kwargs):
        cls = self._types.get(key)
        if not cls:
            raise ValueError(key)
        return cls(**kwargs)


extractors = ObjectFactory(EXTRACTORS)
transformers



class SparkTransformersFactory(ObjectFactory):
    def __init__(self):
        super().__init__(_SPARK_TRANSFORMERS)
