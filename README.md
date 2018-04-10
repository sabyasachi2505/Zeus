# Zeus

Zeus is designed to help data scientists in pySpark do the pre-modeling data processing steps without having to write too much code.


## The basic idea

>Zeus class aims to simplify the pre-modeling steps.
Hence, the existence of a target column and an id column is a must.
The generic functions applicable on pyspark dataframes would work on Zeus as well.
Most of the methods work on binary as well as multi-class targets.


## List of Methods available

| Name of Method    |  Function     |
| :-----------------|:------------- |
| columns           |returns a list of column names present in the data|
| show              |shows n sample observations in the data                       |
| dtypes            |returns a list of tuples containing column names <br />and their respective data types|
| drop              |drops specific columns from the Zeus object<br />(Returns a pyspark dataframe in case the id / target column is dropped)|
| keep              |keeps specific columns in the Zeus object and drops the rest<br />(id and target columns are automatically retained)|
| univariate        |returns a pandas DataFrame containing percentile distributions of all<br />the variables along with their count, mean, median etc|
| bivariate         |returns a pandas DataFrame with event rates per <br />percentile buckets of each predictor variable|
| oversample        |increases the event rate of the data by randomly <br />duplicating samples of event occurrences |
| undersample       |increases the event rate of the data by removing <br />randomly sampled non-event occurrences|
| randomSplit       |splits the Zeus object into multiple Zeus objects in the provided ratio |


## Examples

### Initializing a Zeus object
```python
from zeus import Zeus
zdata = Zeus(data, targetColumn = "target", idColumn = "user_id")
zdata.show(2)
```
>Zeus helps us convert a pyspark DataFrame to a Zeus object. Now, we can see how the Zeus data loooks like.

```python
idColumn : user_id
targetColumn : target
Number of Observations : 120324
Sample Data :
```
| user\_id  | amount | timestamp |transaction\_id|item\_id| target |
|:---------:|:-----:|:---------:|:--------------:|:------:|:------:|
|1s3e4452de|10|2018-04-03 12:04:45|18me10052|r1c173fe|1|
|1s3f4329sc|54|2018-05-03 16:04:45|18ch13324|r2c532cz|0|

### Listing the columns present in the data
```python
colnames = zdata.columns()
print (colnames)
```
>.columns returns a list object containing the column names present in the data. The first 2 names in the list represent the idColumn and the targetColumn.

```python
['user_id', 'target', 'amount', 'timestamp', 'transaction_id', 'item_id']
```
### Seeing the datatypes of various columns present in the data
```python
datatypes = zdata.dtypes()
print (datatypes)
```
>.dtypes returns a list of tuples containing column name and their respective type.
```python
[('user_id', 'string'), ('amount','bigint'), ('timestamp','timestamp'), ('transaction_id', 'string'), ('item_id', 'string'), ('target','int')]
```

### Retaining specific columns in the data
```python
zdata.keep('amount', 'transaction_id')
print (zdata.columns)
```
>.keep() lets you retain specific features and deletes the rest. This method modifies the dataset itself. It retains the idColumn and targetColumn by default.
```python
['user_id', 'target', 'amount', 'transaction_id']
```

### Deleting specific columns from the data
```python
# We are assuming that the data contains all the original columns ('user_id', 'target', 'amount', 'timestamp', 'transaction_id', 'item_id')
zdata.drop('amount', 'transaction_id')
print (zdata.columns)
```
>.drop() lets you drop specific features. This method modifies the dataset itself. As retaining the idColumn and targetColumn are essential to Zeus objects, if you accidentally pass these column names into the drop command, the original object is not modified. Instead a pySpark dataframe is returned after deleting the mentioned columns from the data. So, .drop() can be used to either modify (reduce the size of) the Zeus object or extract a certain subset of the data minus the id and/or target columns.

```python
['user_id', 'target', 'timestamp', 'item_id']
```

### Analyzing variable percentile distributions
```python
data_univariate = zdata.univariate(0.03, 0.04, 0.5 0.7, 0.9)
data_univariate
```
>.univariate() returns a pandas dataframe containing the variable distributions of each column. Specific percentiles can be passed as arguements. In case no arguements are passed, these are calculated by default 0.01, 0.25, 0.5. 0.75, 0.99. In addition to these percentiles, the count of observations, the mean, the minimum value, the maximum value and standard deviation are also calculated.

```python
|column_name|count|mean|stddev|min|max|0.03 percentile|0.04 percentile|0.5 percentile|0.7 percentile|0.9 percentile|
|:---------:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|
|target|261437|0.047632891|0.212988666|0|1|0|0|0|1|1|
|amount|261437|0.079648252|0.270748385|0|1|0|0|0|1|1|
```