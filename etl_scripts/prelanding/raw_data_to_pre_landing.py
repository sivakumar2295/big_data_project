# Databricks notebook source
#library import
from pyspark.sql import functions

#Script_execution_for_raw_to_pre_landing
class Raw_to_Prelanding:
  
  """Script_execution_for_raw_to_pre_landing"""
  
  def __init__(self,params):
    
    """getting parameters for execution"""
    self.params=params
      
  def read_csv(self,file_params,dest_path):
    
    """reading csv file based on file params from destination path"""
    #spark.read.format(file_params['dest_format']).option("path",dest_path)
    return "reading "+file_params['dest_format']+dest_path
  
  def structure_df_function(self,df):
    pass
  
  def write_csv(self):
    pass
  
  def main_block(self):
    
    """main execution block"""
    df=self.read_csv(self.params,self.params['dest_path'])
    structured_df=self.structure_df_function(df)
    self.write_csv(structured_df)
    
    print(df)



# COMMAND ----------

def add(a,b):
  return a+b

c=add(5,6)
structure_df(c)

# COMMAND ----------

if __name__=='__main__':
  print(__name__)
  
  file_params={
    "src_path":"source_path",
    "dest_format":"csv",
    "dest_path":"destination_path"
  }
  
  obj=Raw_to_Prelanding(file_params)
  
  obj.main_block()

# COMMAND ----------


