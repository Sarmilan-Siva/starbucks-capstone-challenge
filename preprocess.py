## preprocessing codes

import pandas as pd
import numpy as np

# preprocess portfolio dataset
def process_portfolio(df):
  ''' preprocess Portfolio data'''
  
  pf_channels = pd.get_dummies(df['channels'].apply(pd.Series).stack(), prefix="ch").sum(level=0)
  df = pd.concat([df, pf_channels], axis=1, sort=False)
  df['tot_channels'] = df['channels'].apply(len)
  df.drop(columns='channels', inplace=True)
  df.rename(columns={'id':'offer_id'}, inplace=True)
  
  return df

# preprocess profile dataset
def process_profile(df):
  ''' preprocess Profile data'''
  
  pf_gender = pd.get_dummies(df['gender'], prefix="gn")
  df_pf = pd.concat([df, pf_gender], axis=1, sort=False)
  
  df_pf['became_member_on'] = df_pf['became_member_on'].apply(lambda x: pd.to_datetime(x, format='%Y%m%d'))
  df_pf['member_for'] = round((pd.to_datetime('today') - df_pf['became_member_on'])/np.timedelta64(365, 'D'), 2)
  
  # flag the age 118 rows
  df_pf['pf_age_valid'] = (df_pf['age'] != 118)
  # replace age 118 with nan, and 
  df_pf['age'] = df_pf['age'].replace({118:np.nan})
  df_pf.rename(columns={'id':'customer_id'}, inplace=True)

  return df_pf

# preprocess transcript data
def process_transcript(df):
  ''' preprocess Transcript data and extract useful information'''
  df = pd.concat([df, df['value'].apply(pd.Series)], axis=1)
  df['offer id'] = df['offer id'].fillna(df['offer_id'])
  df.drop(columns=['value', 'offer_id'], inplace=True)
  
  # dropping ALL duplicate values
  df.drop_duplicates(subset=None, keep = 'first', inplace = True)
  
  df.rename(columns={'offer id':'offer_id', 'person':'customer_id'}, inplace=True)
  
  return df

# identify duplicate records in a dataframe
def any_duplicates(df, return_df=False):
  ''' count and return duplicate records if there are '''
  
  df_t = df.copy()
  duplicate = df_t[df_t.duplicated(keep = 'last')]
  
  print('Number of duplicate record: {}'.format(str(duplicate.shape[0])))
  
  if return_df:
    return duplicate