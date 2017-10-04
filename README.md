# csdUpdate
--------------------------
-   Table of Contents    -
--------------------------
A - Program Overview
B - Understanding log.csv
C - Basic requirements
C.1 - Dataset_register.csv
C.2 - Concordance.csv
C.3 - Population.csv


--------------------------
A - Program Overview     -
--------------------------

From time to time, Statistics Canada consolidates smaller Census Subdivions (CSDs) into larger ones.

CSD Updater is designed to allow quick and reliable merging of legacy CSDs. Future plans include implementing the ability to add new data to datasets. This will be particularlily important for datasets such as Population that have over one million rows of data, exceeding the limits of Excel.

This program was written in Python 3.5 by Kyle Lillie.

--------------------------
B - log.csv              -
--------------------------

After processing the dataset files, the program will produce a log detailing the modifications made to the source data.
The log has three columns, 'timestamp', 'dataset', and 'type'.

	- timestamp: the time the dataset was processed
	- dataset: name of the dataset processed
	- type: the type of processing applied to the dataset (see C1). There are a few types:
		- averaged: get the mean value of duplicates.
		- aggregated: simply sum up duplicate values.
		- csd name updated -no duplicates: no duplicate values detected, so only the CSD Name was updated.
		- csd name updated -passed: CSD Name was updated, but the dataset otherwise skipped processing.
		- population weighted: weight the duplicates by population.
		- child CSDs removed: child CSDs removed, because the data shouldn't be merged with the new parent.

--------------------------
C - Basic Requirements   -
--------------------------

If the following files are not found, the program will not run at all:

	.\dataset_register.csv
	.\concordance.csv
	.\datasets\population.csv

More on these below.

--------------------------
C1- dataset_register.csv -
--------------------------

Inside this file you'll find two columns. Column A is a list of datasets, column B describes how they should be treated when combining legacy CSDs. The program looks for one of five numbers:

	0: ignore the dataset, do nothing
	1: weight the duplicates by population
	2: get the mean value of duplicates
	3: simply sum up duplicate values
	4: remove duplicate values, but don't combine values

--------------------------
C2-  concordance.csv     -
--------------------------

The aptly named file contains the concordance of legacy CSDs to their most recent data. This will need to be updated as Statistics Canada updates CSD boundaries.

Ideally, this should be taken from the file, located somewhere on M:\, called '20XX Concordance.xlsx'

--------------------------
C3-   population.csv     -
--------------------------

The secret ingredient to population weighting is having the old population.csv dataset in the datasets folder. To be clear, if you are trying to prepare 2015 datasets to receive 2016 data, you need the 2015 population data. Going forward achrive procedures implemented by this program should greatly help in preserving legacy data in case of the need to do a rollback, or heaven knows what else. 
