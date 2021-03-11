#!/usr/bin/env python
# coding: utf-8

# # Assignment 4
# 
# Welcome to Assignment 4. This will be the most fun. Now we will prepare data for plotting.
# 
# Just make sure you hit the play button on each cell from top to down. There are three functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook.
# 
# 

# This notebook is designed to run in a IBM Watson Studio default runtime (NOT the Watson Studio Apache Spark Runtime as the default runtime with 1 vCPU is free of charge). Therefore, we install Apache Spark in local mode for test purposes only. Please don't use it in production.
# 
# In case you are facing issues, please read the following two documents first:
# 
# https://github.com/IBM/skillsnetwork/wiki/Environment-Setup
# 
# https://github.com/IBM/skillsnetwork/wiki/FAQ
# 
# Then, please feel free to ask:
# 
# https://coursera.org/learn/machine-learning-big-data-apache-spark/discussions/all
# 
# Please make sure to follow the guidelines before asking a question:
# 
# https://github.com/IBM/skillsnetwork/wiki/FAQ#im-feeling-lost-and-confused-please-help-me
# 
# 
# If running outside Watson Studio, this should work as well. In case you are running in an Apache Spark context outside Watson Studio, please remove the Apache Spark setup in the first notebook cells.

# In[1]:


from IPython.display import Markdown, display
def printmd(string):
    display(Markdown('# <span style="color:red">'+string+'</span>'))


if ('sc' in locals() or 'sc' in globals()):
    printmd('<<<<<!!!!! It seems that you are running in a IBM Watson Studio Apache Spark Notebook. Please run it in an IBM Watson Studio Default Runtime (without Apache Spark) !!!!!>>>>>')


# In[2]:


get_ipython().system('pip install pyspark==2.4.5')


# In[3]:


try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
except ImportError as e:
    printmd('<<<<<!!!!! Please restart your kernel after installing Apache Spark !!!!!>>>>>')


# In[4]:


sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession     .builder     .getOrCreate()


# Sampling is one of the most important things when it comes to visualization because often the data set gets so huge that you simply
# 
# - can't copy all data to a local Spark driver (Watson Studio is using a "local" Spark driver)
# - can't throw all data at the plotting library
# 
# Please implement a function which returns a 10% sample of a given data frame:

# In[16]:


#I need to unwrap the df and extract RDD (No, it is to be done in the next cell) before sampling 10% of datapoints.
def getSample():
    return df.sample(False,0.1)


# Now we want to create a histogram and boxplot. Please ignore the sampling for now and return a python list containing all temperature values from the data set

# In[6]:


def getListForHistogramAndBoxPlot():
    my_list = spark.sql("""
        SELECT temperature from washing where temperature is not null
    """).rdd.map(lambda row: row.temperature).collect()
    if not type(my_list)==list:
        raise Exception('return type not a list')
    return my_list


# Finally we want to create a run chart. Please return two lists (encapsulated in a python tuple object) containing temperature and timestamp (ts) ordered by timestamp. Please refer to the following link to learn more about tuples in python: https://www.tutorialspoint.com/python/python_tuples.htm

# In[7]:


def getListsForRunChart():
    double_tuple_rdd = spark.sql("""
        select temperature, ts from washing where temperature is not null order by ts asc
    """).sample(False,0.1).rdd.map(lambda row : (row.ts,row.temperature))
    result_array_ts = double_tuple_rdd.map(lambda ts_temperature: ts_temperature[0]).collect()
    result_array_temperature = double_tuple_rdd.map(lambda ts_temperature: ts_temperature[1]).collect()
    return (result_array_ts,result_array_temperature)


# Now it is time to grab a PARQUET file and create a dataframe out of it. Using SparkSQL you can handle it like a database. 

# In[8]:


get_ipython().system('wget https://github.com/IBM/coursera/blob/master/coursera_ds/washing.parquet?raw=true')
get_ipython().system('mv washing.parquet?raw=true washing.parquet')


# In[9]:


df = spark.read.parquet('washing.parquet')
df.createOrReplaceTempView('washing')
df.show()


# Now we gonna test the functions you've completed and visualize the data.

# In[10]:


get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt


# In[11]:


plt.hist(getListForHistogramAndBoxPlot())
plt.show()


# In[12]:


plt.boxplot(getListForHistogramAndBoxPlot())
plt.show()


# In[13]:


lists = getListsForRunChart()


# In[14]:


plt.plot(lists[0],lists[1])
plt.xlabel("time")
plt.ylabel("temperature")
plt.show()


# Congratulations, you are done! The following code submits your solution to the grader. Again, please update your token from the grader's submission page on Coursera

# In[15]:


get_ipython().system('rm -f rklib.py')
get_ipython().system('wget https://raw.githubusercontent.com/IBM/coursera/master/rklib.py')


# In[17]:


from rklib import submitAll
import json

key = "S5PNoSHNEeisnA6YLL5C0g"
email = "imansoltani56@gmail.com"
token = "cuV16XmPypbDcP7f"


# In[18]:


parts_data = {}
parts_data["iLdHs"] = json.dumps(str(type(getListForHistogramAndBoxPlot())))
parts_data["xucEM"] = json.dumps(len(getListForHistogramAndBoxPlot()))
parts_data["IyH7U"] = json.dumps(str(type(getListsForRunChart())))
parts_data["MsMHO"] = json.dumps(len(getListsForRunChart()[0]))

submitAll(email, token, key, parts_data)


# In[ ]:




