## utility functions
import pandas as pd

# read in the json files
def get_data(path):
  ''' read the .json data '''
  
  df = pd.read_json(path, orient='records', lines=True)
  return df

# giving simple names for offer_id
offer_names = {
  'ae264e3637204a6fb9bb56bc8210ddfd':'b1',
  '4d5c57ea9a6940dd891ad53e9dbe8da0':'b2',
  '3f207df678b143eea3cee63160fa8bed':'i1',
  '9b98b8c7a33c4b65b9aebfe6a799e6d9':'b3',
  '0b1e1539f2cc45b7b9fa7c272da2e1d7':'d1',
  '2298d6c36e964ae4a3e7e9706d1fb8c2':'d2',
  'fafdcd668e3743c1bb461111dcafc2a4':'d3',
  '5a8bc65990b245e5a138643cd4eb9837':'i2',
  'f19421c1d4aa40978ebb69ca19b0e20d':'b4',
  '2906b810c7d4411798c6938adc9daaa5':'d4'  
}