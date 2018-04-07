# Zeus

Zeus is designed to help data scientists in pySpark do the pre-modeling data processing steps without having to write too much code. I have implemented the following :

Author : Sabyasachi Mishra


## Basic functionalities of pySpark Dataframes
#[[columns]] - lets you see all the column names present in the data <br />
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
```python
['id', 'target', 'spend', 'transactions']
```
