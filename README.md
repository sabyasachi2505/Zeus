# Zeus

Zeus is designed to help data scientists in pySpark do the pre-modeling data processing steps without having to write too much code. As this is a pet project of mine to help my teammates, I have implemented the following :

## Basic functionalities of pySpark Dataframes
keep - lets you keep the selected columns (similar to select) \\
drop - lets you drop selected columns (similar to drop) \\
show - shows you a sample of the data and other attributes of the class (similar to show) \\
dtypes - shows you the datatypes of various columns (simialr to dtypes)

## Advanced functionalities
uniVariate - returns a pandas dataframe containing various percentiles, count, mean, meadian etc for each column
biVariate - returns a pandas dataframe containing the event rate across various percentile buckets of each column
undersample - increases the event rate of the data by removing a random sample of non-events
oversample - increases the event rate of the data by repeating a random sample of events
randomSplit - splits the data into multiple Zeus objects depending on the ratios provided

## All of the above are methods of class Zeus
