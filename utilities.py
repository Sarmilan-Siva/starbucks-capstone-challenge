## utility functions
import pandas as pd
from sklearn.cluster import AgglomerativeClustering

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

## functions in helping to create individual level information
# aggregated transaction and rewards
def get_TransactionRewards(df, df_pf):
  ''' get transaction and reward information for all customers '''
  
  df_indv = df.groupby(['customer_id']).agg(
    tot_amount = ('amount', 'sum'),
    n_trans = ('amount', 'count'),
    tot_rewards = ('reward_x', 'sum'),
    n_rewards = ('reward_x', 'count')).reset_index()
  
  # calculate value per transaction and offer
  df_indv['val_trans'] = round(df_indv['tot_amount']/df_indv['n_trans'], 2)
  df_indv['val_rewards'] = round(df_indv['tot_rewards']/df_indv['n_rewards'], 2)
  
  # merge customer information
  df_indall = pd.merge(df_indv,
                      df_pf[['customer_id', 'gender','age', 'income', 'member_for', 'pf_age_valid']],
                      on='customer_id', how="left")
  
  return df_indall

# transaction while claiming the offer
def get_OfferTransaction(df, offer_split):
  ''' aggregated total transaction information occured while claiming the offer '''
  
  df_t = df.copy()
  df_t['amount_s1'] = df_t.groupby('customer_id')['amount'].apply(lambda x: x.shift(1))
  df_t['ts1_isTrue'] = df_t.groupby('customer_id')['time'].apply(lambda x: x.shift(1) == x)
  
  if offer_split is not None:
    df_tt = df_t[(df_t.event == 'offer completed') & (df_t.ts1_isTrue == True)].groupby(
      ['customer_id', offer_split]).agg(
      tot_offerTrans = ('amount_s1', 'sum')).reset_index()
  else:
    df_tt = df_t[(df_t.event == 'offer completed') & (df_t.ts1_isTrue == True)].groupby(
      ['customer_id']).agg(
      tot_offerTrans = ('amount_s1', 'sum')).reset_index()
  
  return df_tt

# creating Offer received, viewed and completed details at different offer level
def get_Offers_nEvents(df, offer_split):
  ''' extract offer information at event level '''
  
  df_t = df.copy()
  df_sm = df_t.groupby(['event', offer_split]).agg(off_count = ('customer_id', 'count')).reset_index()
  df_sm_pv = df_sm.pivot(index=offer_split, columns='event', values='off_count').reset_index()
  df_sm_pv = df_sm_pv.rename_axis(None, axis = 1)
  
  # create additional ratio information
  df_sm_pv['off_rec_viewed'] = round(df_sm_pv['offer viewed']/df_sm_pv['offer received'], 2)
  df_sm_pv['off_rec_completed'] = round(df_sm_pv['offer completed']/df_sm_pv['offer received'], 2)
  
  return df_sm_pv

# count customers who recieved each offers and offer types
def count_Unique_Offers(df, offer_split):
  ''' count how many customers received each offers '''
  
  df_t = df.copy()
  df_sm = df_t.groupby(['event', offer_split]).agg(unique_cust = ('customer_id', 'nunique')).reset_index()
  df_sm_pv = df_sm.pivot(index=offer_split, columns='event', values='unique_cust').reset_index()
  df_sm_pv = df_sm_pv.rename_axis(None, axis = 1)

  # create additional ratio information
  df_sm_pv['off_rec_viewed'] = round(df_sm_pv['offer viewed']/df_sm_pv['offer received'], 2)
  df_sm_pv['off_rec_completed'] = round(df_sm_pv['offer completed']/df_sm_pv['offer received'], 2)
  tot_cust = df_t['customer_id'].nunique()
  df_sm_pv['perc_recv_offer'] = round(df_sm_pv['offer received']*100/tot_cust, 2)
  
  return df_sm_pv

# reward value for each customer against its type
def get_OfferReward(df, offer_split):
  ''' extract reward value against offer types '''
  
  df_t = df[df.event == 'offer completed'].groupby(['customer_id', offer_split]).agg(
    tot_rewards = ('reward_x', 'sum'),
    n_rewards = ('reward_x', 'count')).reset_index()
  
  return df_t

# calculating different types of customer engagements
def get_OfferEngagements(df):
  ''' calculating customer level offers and engagements at Offer Level '''
  
  df_t = df.copy()
  df_ec = df_t.groupby(['customer_id', 'offer_type', 'event']).agg(event_count = ('offer_type', 'count')).reset_index()
  
  #creating unique event types
  df_ec['event'] = df_ec['event'].str.replace(' ', '')
  df_ec['off_type_event'] = df_ec['offer_type'] + '_' + df_ec['event']
  
  df_ecp = df_ec.pivot(index='customer_id', columns='off_type_event', values='event_count').reset_index()
  df_ecp = df_ecp.rename_axis(None, axis = 1)
  
  #fill offer_completed with a small number
  col_fill = ['bogo_offercompleted', 'discount_offercompleted',
              'informational_offerviewed', 'bogo_offerviewed', 'discount_offerviewed']
  df_ecp[col_fill] = df_ecp[col_fill].fillna(0.01)
  
  #calculating ratios
  df_ecp['b_off_rec_view'] = round(df_ecp['bogo_offerviewed']/df_ecp['bogo_offerreceived'], 2)
  df_ecp['b_off_rec_compl'] = round(df_ecp['bogo_offercompleted']/df_ecp['bogo_offerreceived'], 2)
  df_ecp['d_off_rec_view'] = round(df_ecp['discount_offerviewed']/df_ecp['discount_offerreceived'], 2)
  df_ecp['d_off_rec_compl'] = round(df_ecp['discount_offercompleted']/df_ecp['discount_offerreceived'], 2)
  df_ecp['i_off_rec_view'] = round(df_ecp['informational_offerviewed']/df_ecp['informational_offerreceived'], 2)
  
  cols = ['customer_id', 'b_off_rec_view', 'b_off_rec_compl', 'd_off_rec_view', 'd_off_rec_compl', 'i_off_rec_view']
  
  return df_ecp[cols]

# clustering using AgglomerativecClustering and summary information of clusters
def cluster_Outcomes(df_m, df, n_cluster):
  '''summarizing the outcome of AgglomerativecClustering '''
  
  cluster = AgglomerativeClustering(n_clusters=n_cluster, affinity='euclidean', linkage='ward')
  class_cl = cluster.fit_predict(df_m)
  
  # assigning cluster numbers
  df['cluster'] = class_cl
  
  # summarizing the results
  df_summ = df.groupby(['cluster']).agg(
    cl_count = ('customer_id', 'count'),
    m_trans = ('tot_amount', 'mean'),
    m_ntrans = ('n_trans', 'mean'),
    m_rewards = ('tot_rewards', 'mean'),
    m_nrewards = ('n_rewards', 'mean'),
    m_age = ('age', 'mean'),
    m_income = ('income', 'mean'),
    m_membY = ('member_for', 'mean'),
    m_bogo_compl = ('b_off_rec_compl', 'mean'),
    m_disc_compl = ('d_off_rec_compl', 'mean')).reset_index()
  
  # calculate gender ratios
  df_gen = df.groupby(['cluster', 'gender']).agg(
    gn_count = ('customer_id', 'count')).reset_index()
  
  df_genp = df_gen.pivot(index='cluster', columns='gender', values='gn_count').reset_index()
  df_genp['ratio_FM'] = round(df_genp['F']/df_genp['M'], 2)
  
  df_cl_summ = pd.merge(df_summ, df_genp, on='cluster', how="left")
  
  return df_cl_summ

# calculating customer response to all offer IDs
def get_OfferEngagements_ID(df, l_offer = ['d1', 'd2', 'd3', 'd4', 'b1', 'b2', 'b3', 'b4']):
  ''' calculating customer level offer engagements at Offer ID level '''
  
  # creating columns for offer completion calculation
  off_cols = [[
    '{}_compl_ratio'.format(l),
    '{}_offercompleted'.format(l),
    '{}_offerreceived'.format(l)
  ] for l in l_offer]
  
  #columns for filling na values
  col_na = ['{}_offercompleted'.format(l) for l in l_offer]
  
  df_t = df.copy()
  df_ec = df_t.groupby(['customer_id', 'offer_id', 'event']).agg(event_count = ('offer_id', 'count')).reset_index()

  #creating unique event types
  df_ec['event'] = df_ec['event'].str.replace(' ', '')
  df_ec['off_id_event'] = df_ec['offer_id'] + '_' + df_ec['event']

  df_ecp = df_ec.pivot(index='customer_id', columns='off_id_event', values='event_count').reset_index()
  df_ecp = df_ecp.rename_axis(None, axis = 1)
  
  #fill _completed columns with small value to get value for offer received/completed ratio
  df_ecp[col_na] = df_ecp[col_na].fillna(0.001)

  for offer in off_cols:
    df_ecp[offer[0]] = round(df_ecp[offer[1]]/df_ecp[offer[2]], 2)

  cols = ['customer_id'] + [col[0] for col in off_cols]

  return df_ecp[cols]

# converting long tables into wide tables
def df_wide(df, index, columns, values, new_names: dict):
  ''' converting long tables into wide '''
  df_w = df.pivot(index=index, columns=columns, values=values).reset_index()
  df_w = df_w.rename_axis(None, axis = 1)
  df_w.rename(columns=new_names, inplace=True)
  
  return df_w

# model prediction
def get_recommendations(customer_id, model, n_rec=3):
  ''' function to recommend best offers to a customer '''
  
  est_score = []
  
  #offers
  l_offer = ['d1', 'd2', 'd3', 'd4', 'b1', 'b2', 'b3', 'b4']
  offers = ['{}_compl_ratio'.format(l) for l in l_offer]
  
  # get a prediction for specific userID and offerID.
  for offer in offers:
    pred = model.predict(customer_id, offer, verbose=False)
    est_score.append(pred[3])
    
  df = pd.DataFrame({'offers':l_offer, 'score':est_score})
  dft = df.sort_values(by=['score'], ascending=False).head(n_rec)
  
  return dft