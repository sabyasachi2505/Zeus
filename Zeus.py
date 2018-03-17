import pandas as pd
from pyspark.sql.functions import row_number, col, count, min, max, sum
from pyspark.sql.functions import explode, array, lit, struct
from pyspark.sql.window import Window

class Zeus:

    data = None
    targetColumn = 'target'
    idColumn = 'liveramp_id'

    def __init__(
            self,
            data = None,
            targetColumn = 'target',
            idColumn = 'liveramp_id'
    ):
        self.data = data
        self.targetColumn = targetColumn
        self.idColumn = idColumn


    def univariate(
            self,
            percentiles = [0.01,0.25,0.5,0.75,0.99]
    ):
        # Creating a list of variable names for which we would calculate the uni-variates
        colnames = self.data.columns
        colnames.remove(self.idColumn)
        total_count = self.data.count()

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
        quantile_df = pd.DataFrame(quantiles_transpose_v2, columns = columns)
        uni_variate_df = df_describe.append(quantile_df)
        uni_variate_df_pd = uni_variate_df.set_index('summary').T
        
        return uni_variate_df_pd

    def bivariate(
            self,
            columns = self.data.drop(self.targetColumn, self.idColumn).columns,
            buckets = 5
    ):

        '''Implements functionality of pd.melt; Transforms dataframe from wide to long'''
        # Create and explode an array of (column_name, column_value) structs
        melter = explode(array([
            struct(lit(colnames).alias("key"), col(colnames).alias("val")) for colnames in columns
        ])).alias("kvs")

        long_data = self.data.select(melter, self.targetColumn) \
        .selectExpr(self.targetColumn , "kvs.key AS key", "kvs.val AS val")

        observations = self.data.count()
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
        .withColumn('event_rate', 100 * col('ones') / col('num_records'))
        .orderBy(['key', 'bucket'])

        return biv.toPandas()