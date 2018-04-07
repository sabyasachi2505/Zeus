# Zeus

Zeus is designed to help data scientists in pySpark do the pre-modeling data processing steps without having to write too much code.

## The basic idea

>Zeus class aims to simplify the pre-modeling steps.
Hence, the existence of a target column and an id column is a must.
The generic functions applicable on pyspark dataframes would work on Zeus as well.
Most of the methods work on binary as well as multi-class targets.

## List of Methods available

| Name of Method    |  Function     |
| :-----------------|:-------------:|
| columns           |returns a list of column names present in the data|
| show              |shows n columns in the data                       |
| dtypes            |returns a list of tuples containing column names and their respective data types|
| drop              |drops specific columns from the Zeus object (Returns a pyspark dataframe in case the id / target column is dropped)|
| keep              |keeps specific columns in the Zeus object and drops the rest (id and target columns are automatically retained)|
| univariate        |returns a pandas DataFrame with percentile distributions of all the variables|
| bivariate         |returns a pandas DataFrame with event rates per percentile buckets of each predictor variable|
| oversample        |increases the event rate of the data by duplicating event occurrences |
| undersample       |increases the event rate of the data by removing randomly sampled non-event occurrences|



[columns](#columns) - lets you see all the column names present in the data <br />
[keep](#keep) - lets you keep the selected columns (similar to select) <br />

drop - lets you drop selected columns (similar to drop) <br />
show - shows you a sample of the data and other attributes of the class (similar to show) <br />
dtypes - shows you the datatypes of various columns (simialr to dtypes) <br />

## Advanced functionalities
uniVariate - returns a pandas dataframe containing various percentiles, count, mean, meadian etc for each column <br />
biVariate - returns a pandas dataframe containing the event rate across various percentile buckets of each column <br />
undersample - increases the event rate of the data by removing a random sample of non-events <br />
oversample - increases the event rate of the data by repeating a random sample of events <br />
randomSplit - splits the data into multiple Zeus objects depending on the ratios provided <br />

## All of the above are methods of class Zeus

###columns
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
