from abc import abstractmethod

import sparkml_base_classes
from pyspark import keyword_only
from pyspark.sql import functions as F

from . import object_factory


class Transformer(sparkml_base_classes.TransformerBaseClass):
    @keyword_only
    def __init__(self):
        super().__init__()

    @abstractmethod
    def _transform(self, ddf):
        pass


class IdentityTransformer(Transformer):
    @keyword_only
    def __init__(self):
        super().__init__()

    def _transform(self, ddf):
        return ddf


class ColumnNamesTransformer(Transformer):
    """Renames columns.

    Parameters
    ----------
    col_names : dict
        Dict values must be unique (1-to-1). Labels not contained in a dict are 
        left as-is.
    """

    @keyword_only
    def __init__(self, col_names=None):
        super().__init__()

    def _transform(self, ddf):
        for colname, new_colname in self._col_names.items():
            ddf = ddf.withColumnRenamed(colname, new_colname)
        return ddf


class ToDateTransformer(Transformer):
    """Converts columns to date type.

    Parameters
    ----------
    col : str
        Column name to cast to date type.

    format : str
        The strftime to parse time, e.g. "%d/%m/%Y".
    """

    @keyword_only
    def __init__(self, col=None, format=None):
        super().__init__()

    def _transform(self, ddf):
        date_col = F.to_date(F.col(self._col), self._format)
        return ddf.withColumn(self._col, date_col)


class ColumnSplitTransformer(Transformer):
    """Split strings around given separator/delimiter.

    Parameters
    ----------
    input_col : str
        Column to split.

    output_cols : list of str
        Column names after split.

    pattern : str
        String or regular expression to split on.

    drop : bool
        If True, ``input_col`` is drop after split.
    """

    @keyword_only
    def __init__(self, input_col=None, output_cols=None, pattern=None,
                 drop=True):
        super().__init__()

    def _transform(self, ddf):
        split_col = F.split(F.col(self._input_col), self._pattern)
        for i, name in enumerate(self._output_cols):
            ddf = ddf.withColumn(name, split_col[i])

        if self._drop:
            ddf = ddf.drop(self._input_col)

        return ddf


class ColumnTypesTransformer(Transformer):
    @keyword_only
    def __init__(self, column_types=None):
        super().__init__()

    def _transform(self, ddf):
        for colname, type_ in self._column_types.items():
            ddf = ddf.withColumn(colname, F.col(colname).cast(type_))
        return ddf

    def _get_type(self, name):
        try:
            return self.TYPES_MAP[name]
        except KeyError:
            raise


factory = object_factory.ObjectFactory.from_base(Transformer)
