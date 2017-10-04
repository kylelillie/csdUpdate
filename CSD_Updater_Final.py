#!/usr/bin/env python
#
#to do: save processed files into new folder called 'processed CSDs'
#		zip up old csvs for backup and safety
#
import os
import pandas as pd
import numpy as np
import multiprocessing
from joblib import Parallel, delayed
import time
from datetime import datetime
import zipfile
import sys

DATASET_PATH = os.getcwd()+'/datasets//'					#set path to the datasets that require conversion
CONCORDANCE_FILE = 'concordance.csv'						#path to the concordance file to use 
POPULATION_FILE = os.getcwd()+'/datasets/population.csv'	#path to population file; required for weighting certain data
REGISTER_FILE = 'dataset_register.csv'						#path to dataset register; used to determine conversion type
EXTENSION = '.csv'											#file type; this shouldn't change
SAVE_DIR = os.getcwd()+'/processed//'						#set the target save directory
NUM_CORES = multiprocessing.cpu_count()						#gets the number of cores available for processing


def sum_weighting (duplicates, header):

	#get a count of the number of child CSDs
	duplicates.reset_index(inplace=True)
	duplicates.drop('index',1,inplace=True)
	
	sums = duplicates.groupby(header[:-1]).sum().reset_index().rename(columns={0:'sums'})
	count = duplicates.groupby(header[:-1]).size().reset_index().rename(columns={0:'count'})
	
	temp = pd.merge(sums,count[['CSDUID','count']], how='left', on=['CSDUID'])
	temp['new'] = temp['Value']/temp['count']
	temp['Value'] = temp['new']
	
	duplicates = temp.drop_duplicates(subset=header[:-1], keep='last').reset_index()
	duplicates = duplicates[header]

	return duplicates

		
def population_weighting (raw_population,old_dataset,num_rows,num_cols,duplicates,header):
		
	#requires base population dataset, as old CSDUIDs are required
	#step 0: slice out old dataset rows that match duplicates. this may not be the most efficient method
	duplicates.loc[:,'index'] = duplicates.index
	old_dataset.loc[:,'index'] = old_dataset.index
	old_dataset.loc[:,'temp'] = duplicates['index']
	base_population = raw_population.copy(deep=True)		#make a copy of population so it can be reused for other datasets
	
	#add population data to old_subset -- need to use MAX Year value....cuz most of these ended in 2013 -- also should be the sum of all child + parent csd
	old_subset = old_dataset[pd.notnull(old_dataset['temp'])] # -- so more like append parent values to the list, then do the things
	old_subset = pd.merge(old_subset, base_population[['CSDUID','Year','Value']], how='left', on=['CSDUID','Year'])
	
	#tack on the CSDUID of the parent -> 'CSDUID_y'
	old_subset = pd.merge(old_subset, duplicates[['CSDUID','index']], how='left', on=['index'])
	
	#append the population of the parent CSD -> 'Value'
	base_population.columns = ['CSDUID_y','Year','Value'] #need to change the column name so the left join will work
	old_subset = pd.merge(old_subset, base_population[['CSDUID_y','Year','Value']], how='left', on=['CSDUID_y','Year'])
	
	#get a sum of population for year - parentCSDUID pairings
	old_subset['total'] = old_subset.groupby(['Year','CSDUID_y'])['Value_y'].transform('sum')
	
	old_subset['weighting'] = old_subset['Value_x']*(old_subset['Value_y']/old_subset['total'])
	old_subset['new_value'] = old_subset.groupby(['Year','CSDUID_y'])['weighting'].transform('sum')
	
	#clean up the dataframe: first set calculated values to the proper place, then trim to original header length
	old_subset['CSDUID_x'] = old_subset['CSDUID_y']
	old_subset['Value_x'] = old_subset['new_value']
	old_subset = old_subset.iloc[:,:len(header)]
	old_subset.rename(columns={'CSDUID_x':'CSDUID','Value_x':'Value'},inplace=True)
	
	weighted_values = old_subset.copy(deep=True)
	weighted_values.reindex(columns=header)
	del base_population

	return weighted_values

	
def get_duplicates (dataset,header,name):
	
	short_header = header[:-1]
	duplicates = dataset[dataset.duplicated(subset=short_header, keep=False)]
	
	return duplicates

	
def sumifs (num_rows,num_cols,dataset,header,n): #parallel For loop

	print('',n, 'duplicate rows processed',end='\r')
	
	#add if else to determine if header is longer than average, and adjust how many col checks are needed
	if (len(header) < 7):
		data_subset = dataset[(dataset[header[0]] == dataset.iloc[n,0]) & (dataset[header[2]] == dataset.iloc[n,2]) & (dataset[header[3]] == dataset.iloc[n,3])]
		
	else:
		data_subset = dataset[(dataset[header[0]] == dataset.iloc[n,0]) & (dataset[header[2]] == dataset.iloc[n,2]) & (dataset[header[3]] == dataset.iloc[n,3]) & (dataset[header[4]] == dataset.iloc[n,4])]
	
	i = data_subset.apply(lambda x: x['Value'], axis=1).sum()
	
	return i #returns list of sumifs

#
# Zip up and archive the old files before converting them #
#	
def zipdir(path, ziph):
    # ziph is zipfile handle
	print('Zipping old datasets.')
	
	for root, path, files in os.walk(path):
		for file in files:
			ziph.write(os.path.join(root, file))
	
if __name__ == '__main__':
	#
	# Do a check for key files first
	#
	if (os.path.isfile(CONCORDANCE_FILE)):
		print ('Concordance file found')
		concordance = pd.read_csv(CONCORDANCE_FILE, delimiter=',',encoding='latin1')
		
	else:
		print ('Concordance.csv not found in root directory.')
		exit()
	
	if (os.path.isfile(POPULATION_FILE)):
		print ('Population file found')
		#read in the file and format it properly by summing by CSD and Year, dropping duplicate values
		base_population = pd.read_csv(POPULATION_FILE, usecols=['CSDUID','Year','Value'], index_col=False, delimiter=',',encoding='utf-8-sig')
		base_population['Value'] = base_population.groupby(['CSDUID','Year'])['Value'].transform('sum')
		base_population = base_population.drop_duplicates(keep='last').reset_index(drop=True)
		
	else:
		print ('Population.csv not found in datasets directory.')
		exit()
	
	if (os.path.isfile(REGISTER_FILE)):
		print ('Register file found')
		register = pd.read_csv(REGISTER_FILE, delimiter=',',encoding='utf-8-sig')
		
	else:
		print ('dataset_register.csv not found in root directory.')
		exit()
	
	#
	# Load up the datasets
	#
	dataset_list = [f for f in os.listdir(DATASET_PATH)]
	
	#
	# Backup the datasets first
	#
	zipf = zipfile.ZipFile((time.strftime("%Y-%m-%d_%H-%M-%S"))+' ARD Datasets.zip', 'w', zipfile.ZIP_DEFLATED) #filename ~ datasets_backup_[today's date].zip
	zipdir('./datasets', zipf)
	zipf.close()
	
	log = []
	
	counter = 0
	
	#
	# Start processing datasets
	#
	
	for f in dataset_list: #loop through datasets
	
		if f.endswith(EXTENSION): #only process valid file types, in this case CSVs
		
			start = time.clock()
			
			# conversion_type == 0: skip the dataset
			# conversion_type == 1: population_weighting
			# conversion_type == 2: sum_weighting
			# conversion_type == 3: sumifs
			try:
				conversion_type = register.loc[register['dataset'] == f, 'dtype'].values[0]						#match the loaded file to the register
			except:
				print('Dataset register missing conversion types.')

			dataset = pd.read_csv(DATASET_PATH+f,low_memory=False, delimiter=',', encoding='latin-1')
			
			dataset_b = dataset.copy(deep=True) 																#keep an copy of the original data for weighting
			header = list(dataset)
			
			if len(header) > 6:
				dataset.iloc[:,4] = dataset.iloc[:,4].astype(str) #prevent errors where it wants to turn strings into integers.
			
			c_nrows = concordance.shape[0]
			c_header = list(concordance)
			
			print('\n Processing: ',f,header, end='\n')
			
			for m in range (c_nrows): #loop through concordance to correlate CSDUIDs. Needs to happen even if there are no duplicates, as names can change.
		
				dataset.loc[dataset['CSDUID'] == concordance.iloc[m,0], 'CSDUID'] = concordance.iloc[m,1] 		#correlate and replace CSDUIDs
				dataset.loc[dataset['CSDUID'] == concordance.iloc[m,1], 'CSD Name'] = concordance.iloc[m,2]		#update CSD Name
						
			duplicates = get_duplicates(dataset,header,f)
			header = list(duplicates)		#column header of the dataset
			num_rows = duplicates.shape[0] 	#number of duplicates in the dataset
			num_cols = len(header)			#size of the datafset
			
			#
			# Check if the dataset even needs to be consolidated, not every dataset will have duplicates
			#	
																					
			if (conversion_type != 0):
				
				if (num_rows > 0):
				
					if (conversion_type == 1): #if it needs to be weighted by population, do this
						print(' Population weighting')
						new_values = population_weighting(base_population,dataset_b,num_rows,num_cols,duplicates,header)
						log.append([(time.strftime("%Y/%m/%d_%H:%M:%S")),f,'population weighted'])
						
						#for m in range (c_nrows): #loop through concordance to correlate CSDUIDs. Needs to happen even if there are no duplicates, as names can change.
						#	dataset.loc[dataset['CSDUID'] == concordance.iloc[m,1], 'CSD Name'] = concordance.iloc[m,2]		#update CSD Name
							
					elif (conversion_type == 2):
						print(' Sum weighting')
						new_values = sum_weighting(duplicates, header)
						log.append([(time.strftime("%Y/%m/%d_%H:%M:%S")),f,'averaged'])
					
					elif (conversion_type == 3): #if dataset is aggregatable, do this
						print(' Aggregating')
						new_values = Parallel(n_jobs=NUM_CORES-1)(delayed(sumifs)(num_rows,num_cols,duplicates,header,i) for i in range (num_rows)) #calculate new values in parallel, total cores less one seems optimal
						log.append([(time.strftime("%Y/%m/%d_%H:%M:%S")),f,'aggregated'])
					
						#format new values into dataframe, and insert them into duplicates df
						nv = pd.DataFrame(new_values)													#turn returned list into a df
						nv.columns = ['Value']															#rename the column
						duplicates = duplicates.reset_index()											#need index to do value replacement
						duplicates.Value = nv.Value														#assign new summed values 
					
					elif (conversion_type == 4): #mainly for municipal tax rates.csv, where you can't really adjust past values, and duplicates should be stricken
						log.append([(time.strftime("%Y/%m/%d_%H:%M:%S")),f,'child CSDs removed'])
						duplicates = duplicates.drop_duplicates(subset=header[:-1], keep='first') #just get rid of them; get rid of everything!
						#but there's a problem...detecting and removing the old CSDs while preserving the new one. 
						#maybe look for a match on old CSDUID = new CSDUID...if they don't match, then remove all with the old CSDUID.
						#but this will have to happen wayyy back up there, before creating/looking for duplicate values
						#oh well, just don't put a 4 as conversion type for now
					
					duplicates = duplicates.drop_duplicates(subset=header[:-1], keep='last')			#clean up duplicates

					#duplicates.to_csv('duplicates %s' % (f), index=False) #saves a file of all the duplicates found in a file
					#integrate updated duplicate values with the data
					dataset = dataset.reset_index().drop_duplicates(subset=header[:-1],keep=False)		#remove all remnants of old duplicates
					dataset = dataset.append(duplicates)[dataset.columns.tolist()]						#add back our updated duplicates, and retain original 
					dataset = dataset.sort_values(by=['CSDUID','Year',header[3]],ascending=True)		#sort by CSD Name
					dataset.set_index('index', inplace=True)
				
				else:
					#for when no duplicates are detected
					log.append([(time.strftime("%Y/%m/%d_%H:%M:%S")),f,'csd name updated -no duplicates'])
					print(' No duplicates found')
				
				if not os.path.exists(SAVE_DIR):
					os.makedirs(SAVE_DIR)
				
				dataset.drop_duplicates(inplace=True)
				dataset.to_csv(SAVE_DIR+'%s' % (f), index=False)										#save as CSV
				counter += 1
				
			else: #dataset shouldn't be consolidated. File will have non-valid CSDs removed. These ones probably need to be manually redone due to intricacies of how values are derived.
				print(' File skipped')
				log.append([(time.strftime("%Y/%m/%d_%H:%M:%S")),f,'csd name updated -passed'])
				dataset.to_csv(SAVE_DIR+'%s' % (f), index=False)										#save as CSV
				pass
			
			del duplicates
			del dataset
			
			elapsed = time.clock()-start
			
			print('\n {:0.2f} seconds'.format(elapsed))
			
	columns = ['datestamp','dataset','type']
	log = pd.DataFrame(log, columns=columns)
	log.to_csv('log.csv', index=False)
