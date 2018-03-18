import pandas as pd
import warnings
from pyspark.sql.functions import row_number, col, count, min, max, sum, avg
from pyspark.sql.functions import explode, array, lit, struct
from pyspark.sql.window import Window
from pyspark.ml.feature import Bucketizer


class OutsideRangeError(ValueError):
    pass


class InvalidArgumentError(ValueError):
    pass


class Zeus:

    def __init__(
            self,
            data=None,
            targetColumn='target',
            idColumn='liveramp_id'
    ):
        if not data:
            raise InvalidArgumentError('Zeus needs a pyspark dataframe to initialize')
        if str(type(data)) != "<class 'pyspark.sql.dataframe.DataFrame'>":
            raise InvalidArgumentError('Zeus only accepts pyspark dataframes')
        self.data = data
        self.targetColumn = targetColumn
        self.idColumn = idColumn
        self.count = data.count()

    def show(self, n=5):
        print('idColumn : ' + self.idColumn)
        print('targetColumn : ' + self.targetColumn )
        print('Number of observations : {a}'.format(a=self.count))
        print('Sample data : ')
        self.data.show(n)

    def dtypes(self):
        return self.data.dtypes()

    def uniVariate(
            self,
            *args
    ):
        if args:
            percentiles = list(args)
        else:
            percentiles = [0.01, 0.25, 0.5, 0.75, 0.99]

        if any(percentile < 0 or percentile > 1 for percentile in percentile):
            raise OutisdeRangeError("all values passed to the function must lie between 0 & 1")

        # Creating a list of variable names for which we would calculate the uni-variates
        colnames = self.data.columns
        colnames.remove(self.idColumn)

        # Describe - covers count, mean, min, max, std dev.
        df_describe = self.data.select(*colnames).describe().toPandas()

        # Calculating percentiles
        quantiles = self.data.approxQuantile(colnames, percentiles, 0.05)
        quantiles_transpose = map(list, zip(*quantiles))
        quantiles_transpose_v2 = list()

        i = 0
        for lists in quantiles_transpose:
            temp = str(int(percentiles[i]*100)) + "_percentile"
            temp = [temp] + lists
            quantiles_transpose_v2.append(temp)
            i = i + 1

        columns = ['summary'] + colnames
        quantile_df = pd.DataFrame(quantiles_transpose_v2, columns=columns)
        uni_variate_df = df_describe.append(quantile_df)
        uni_variate_df_pd = uni_variate_df.set_index('summary').T
        
        return uni_variate_df_pd

    def biVariate(
            self,
            columns=None,
            buckets=5
    ):
        if not columns:
            columns = self.data.drop(self.targetColumn, self.idColumn).columns

        '''Implements functionality of pd.melt; Transforms dataframe from wide to long'''
        # Create and explode an array of (column_name, column_value) structs
        melter = explode(array([
            struct(lit(colnames).alias("key"), col(colnames).alias("val")) for colnames in columns
        ])).alias("kvs")

        long_data = self.data.select(melter, self.targetColumn) \
        .selectExpr(self.targetColumn, "kvs.key AS key", "kvs.val AS val")

        observations = self.count
        split_val = [i/buckets for i in range(buckets, (observations * buckets) + 1, observations - 1)]
        bucketizer = Bucketizer(splits=split_val, inputCol="row", outputCol="bucket")

        biv = bucketizer.transform(
            long_data.select(
                self.targetColumn,
                'key',
                'val',
                row_number().over(Window.partitionBy('key').orderBy('val')).alias('row')
            )
        ) \
        .groupby('key', 'bucket')\
        .agg(
            count('*').alias('num_records'),
            min('val').alias('bucket_min'),
            max('val').alias('bucket_max'),
            sum('target').alias('ones')
        )\
        .withColumn('event_rate', 100 * col('ones') / col('num_records')) \
        .orderBy('key', 'bucket')

        return biv.toPandas()

    def randomSplit(
            self,
            *args,
            **kwargs
    ):
        if not args:
            raise InvalidArgumentError('please pass the ratios to split the data\nSample usage: a,b = data.randomSplit(80,20, seed = 43)')
        elif len(args) < 2:
            raise InvalidArgumentError('please pass at least 2 values to split the data\nSample usage: a,b = data.randomSplit(80,20, seed = 43)')
        else:
            args = list(args)

        if 'seed' in kwargs:
            seed = kwargs['seed']
        else:
            seed = 43
        return [zeus(x, targetColumn=self.targetColumn, idColumn=self.idColumn) for x in self.data.randomSplit(weights=args, seed=seed)]


    def oversample(
            self,
            ratio,
            eventValue=1,
            seed=42
    ):
        observations = self.count
        positives = self.data.filter(col(self.targetColumn) == eventValue)
        events = positives.count()

        base_event_rate = events / float(observations)

        if ratio > base_event_rate:

            sample_ratio = ((ratio / (1 - ratio)) * (float(observations)/events) - 1) - 1

            self.data = self.data.union(
                positives.sample(True, sample_ratio, seed)
            )
            self.count = self.data.count()

        else:
            warnings.warn('base event rate > provided ratio. no oversampling performed')

    def undersample(
            self,
            ratio,
            eventValue=1,
            seed=42
    ):
        observations = self.count
        positives = self.data.filter(col(self.targetColumn) == eventValue)
        events = positives.count()

        base_event_rate = events / float(observations)

        if ratio > base_event_rate:

            sample_ratio = (events / float(observations - events)) * ((1 - ratio) / ratio)

            self.data = positives.union(
                self.data.filter(col('target') != eventValue).sample(False, sample_ratio, seed)
            )
            self.count = self.data.count()

        else:
            warnings.warn('base event rate > provided ratio. no undersampling performed')
