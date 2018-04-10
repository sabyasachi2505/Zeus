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
from Zeus import Zeus
zdata = Zeus(data, targetColumn = "target", idColumn = "user_id")
zdata.show(5)
```
Passing a pyspark dataframe into Zeus, converts it into a Zeus object. Now, we can see how the Zeus data loooks like.

```python
idColumn : 
targetColumn : 
Number of Observations : 
Sample Data :
```
| user\_id  | spend | timestamp | target |
|:---------:|:-----:|:---------:|:------:|
|1s3e4452de|10|2018-04-03 12:04:45|1|
|1s3f4329sc|54|2018-05-03 16:04:45|0|

### columns
```python
print (data.columns)
```
data.columns returns a list of column names. The first 2 names in the list represent the idColumn and the targetColumn
```python
['id', 'target', 'spend', 'timestamp', 'transactions', 'age_bucket']
```

###keep

```python
data.keep('spend', 'transactions')
print (data.columns)
```
keep lets you retain specific features and deletes the rest. This method modifies the dataset itself.
```python
['id', 'target', 'spend', 'transactions']
```
