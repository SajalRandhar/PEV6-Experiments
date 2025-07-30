import json
import re
import os
import pandas as pd
import numpy as np
from tqdm import tqdm

class previousInsurerProcessing():
    def __init__(self, df, col):
        self.df = df
        self.col = col
        self.new_col = col+"_map"
        
    def get_unique_insurer_list(self, df):
        # Convert the insurer name into tokens
        df[self.col] = df[self.col].astype('str')
        _list = list(df[self.col].unique())
        
        self._list_norm = []
        for i in _list:
            if i and len(i)>1:
                self._list_norm.append(i)
            else:
                pass
        _tokens = [re.sub(r'[^a-zA-Z ]', ' ', i).lower().split(" ") for i in self._list_norm]
        return _tokens

    def process_tokens(self, tokens):
        # Removing the Irrelevant words for the analysis
        processed_tokens = []
        for token in tokens:
            processed_tokens.append([ elem for elem in token if elem not in ['co', 'ltd', '', 'assurance', 'insurance', 'general', 'company', 'limited']] )
        processed_tokens_2 = [x for x in processed_tokens if x != []]
        return processed_tokens_2 

    def jaccard_similarity(self, list1, list2):
        intersection = len(list(set(list1).intersection(list2)))
        union = (len(list1) + len(list2)) - intersection
        return float(intersection) / union

    def insurer_mapping(self, processed_tokens, tokens):
        _dict = {}
        for token_x in processed_tokens:
            for index_y,token_y in enumerate(processed_tokens):
                if self.jaccard_similarity(token_x, token_y)>0.4:
                    company_name = " ".join(i for i in token_x if i!='the')
                    if ('remember' in company_name) or \
                       ('na' in company_name) or \
                        ('nan' in company_name) or \
                     ('None' in company_name):
                        continue
                    _dict[self._list_norm[index_y]] = company_name
                    
#         print(_dict)
#         insurer_mapping_df = pd.DataFrame(_dict.items(), columns=['insurer name', 'company'])
#         insurer_mapping_df = insurer_mapping_df[~insurer_mapping_df['company'].str.contains('remember$|na$|nan$|None$', regex=True)]
        return _dict

    def map(self, x):
        try:
#             a = self.insurer_mapping_df.loc[self.insurer_mapping_df['insurer name']==x, 'company'].iloc[0]
            a = self.insurer_mapping_dict[x]
        except:
            a = np.nan
        return a

    def process(self):
        tqdm.pandas()
        tokens = self.get_unique_insurer_list(self.df)
        processed_tokens = self.process_tokens(tokens)
#         self.insurer_mapping_df = self.insurer_mapping(processed_tokens, tokens)

        ## Instead of DF based mapping, made disctionary based mapping. 
        self.insurer_mapping_dict = self.insurer_mapping(processed_tokens, tokens)


        self.df[self.new_col] = self.df[self.col].progress_map(lambda x: self.map(x))

        self.df[self.new_col].replace({'godigit' : 'go digit',
                                       'go digital': 'go digit', 
                                                     'iffco tokyo': 'iffco tokio',
                                                     'bharathi axa':  'bharti axa', 
                                                     'bharati axa': 'bharti axa',
                                                     'univeral sompo': 'universal sompo'}, inplace=True) 
        return self.df


def get_od_claim_history_date_accounted_updated(claim_data,cleaned_data, policy_start_date, reg_year):
    global CNT
    if pd.isnull(cleaned_data): 
        return get_od_claim_history_date_accounted(claim_data, policy_start_date, reg_year)

    try:
        jsonified_temp = json.loads(cleaned_data)
        if type(jsonified_temp) == list:
            jsonified = jsonified_temp
        elif (type(jsonified_temp) == dict) & ('claim_details' in jsonified_temp):
            jsonified = jsonified_temp['claim_details']
        else:
            return np.nan
        claim_count = 0
        for claim in jsonified:
            if len(claim) == 0:
                continue
            if claim["claim_type"] == 'OD' and float(claim['od_claims_paid']) < 0:
                insurer = claim["insurer_name"]
                if "royal" in insurer.lower():
                    claim['od_claims_paid'] = -float(claim['od_claims_paid'])
            if claim['claim_type'] == 'OD' and float(claim['od_claims_paid']) >= 1000:
                # if(
                #     (pd.to_datetime(claim['accident_loss_date'], format = '%Y-%m-%d') < policy_start_date)
                #      & 
                #     (pd.to_datetime(claim["accident_loss_date"], format = '%Y-%m-%d').year >= float(reg_year))
                # ):
                if ( (pd.to_datetime(claim['accident_loss_date'], format='%Y-%m-%d %H:%M:%S.%f') < policy_start_date)
                    & (pd.to_datetime(claim["accident_loss_date"], format='%Y-%m-%d %H:%M:%S.%f').year >= float(reg_year))
                ):
                    claim_count += 1

        return claim_count
    except Exception as e:
        print("ERRRR: ", e)
        return np.nan


def get_od_claim_history_date_accounted(claim_data, policy_start_date, reg_year):
    """
    For every policy aggregates the past OD claim count.
    """
    """
    args:
        - x is the "claims_history" series
        - cleaned_data is the "cleaned_data" series
    """
    global CNT
    if pd.isnull(claim_data): return np.nan
    try:
        jsonified = json.loads(claim_data)
        if jsonified['success'] == False:
            return np.nan
        if len(jsonified["claims"]) == 0:
            return 0
        else:
            claim_count = 0
            for claim in jsonified["claims"]:
        #             cl_jsnfied = json.loads(claim)
                if len(claim) == 0:
                    continue
                if claim["claim_type"] == 'OD'  and float(claim['total_od_amount']) < 0:
                    insurer = claim["insurer"]
                    if "royal" in insurer.lower():
                        claim['total_od_amount'] = -float(claim['total_od_amount'])
                if claim["claim_type"] == 'OD' and float(claim['total_od_amount']) >= 1000:
                    if (
                        (pd.to_datetime(claim["date_of_loss"], format = '%Y-%m-%d') < policy_start_date)
                       & 
                        (pd.to_datetime(claim["date_of_loss"], format = '%Y-%m-%d').year >= float(reg_year))
                    ):
                        claim_count += 1
            return claim_count
    except Exception as e:
        print("Error: ",e)
        return np.nan

def get_last_claim_year_updated(cleaned_data, policy_start_date, reg_year):
    """
    For every policy aggregates the past OD claim count.
    """
    if pd.isnull(cleaned_data): return np.nan
    try:
        jsonified_temp = json.loads(cleaned_data)
        if type(jsonified_temp) == list:
            jsonified = jsonified_temp
        elif (type(jsonified_temp) == dict) & ('claim_details' in jsonified_temp):
            jsonified = jsonified_temp['claim_details']
        else:
            return np.nan
        last_claim_year = 0
        for claim in jsonified:
            if len(claim) == 0:
                continue
            if claim["claim_type"] == 'OD':
                if (
                    (pd.to_datetime(claim["accident_loss_date"], format = '%Y-%m-%d') < policy_start_date)
                   & 
                    (pd.to_datetime(claim["accident_loss_date"], format = '%Y-%m-%d').year >= float(reg_year))
                ):
                    loss_year = pd.to_datetime(claim["accident_loss_date"], format = '%Y-%m-%d').year
                    last_claim_year = max(loss_year, last_claim_year)
        return last_claim_year
    except Exception as e:
        print(e)
        return np.nan
def get_last_claim_year(x, cleaned_data, policy_start_date, reg_year):
    """
    For every policy aggregates the past OD claim count.
    """
    if pd.isnull(x):
        return get_last_claim_year_updated(cleaned_data, policy_start_date, reg_year)
    try:
        jsonified = json.loads(x)
    #         print(jsonified)
        if jsonified['success'] == False:
            return np.nan
        if len(jsonified["claims"]) == 0:
            return 0
        else:
            last_claim_year = 0
            for claim in jsonified["claims"]:
                if len(claim) == 0:
                    continue
                if claim["claim_type"] == 'OD':
                    # if (
                    #     (pd.to_datetime(claim["date_of_loss"], format = '%Y-%m-%d') < policy_start_date)
                    #    & 
                    #     (pd.to_datetime(claim["date_of_loss"], format = '%Y-%m-%d').year >= float(reg_year))
                    # ):
                    if ( (pd.to_datetime(claim['date_of_loss'], format='%Y-%m-%d %H:%M:%S.%f') < policy_start_date)
                    & (pd.to_datetime(claim["date_of_loss"], format='%Y-%m-%d %H:%M:%S.%f').year >= float(reg_year))
                ):    
                        loss_year = pd.to_datetime(claim["date_of_loss"], format = '%Y-%m-%d').year
                        last_claim_year = max(loss_year, last_claim_year)
            return last_claim_year
    except Exception as e:
        print(e)
        return np.nan

def get_customer_age(data):
    data['customer_age'] = np.round((data['policy_start_date'] - data['dob_final']).dt.days/365)
    # self.df['customer_age'] = self.df['customer_age'].fillna(self.df['customer_age_old'])
    data['customer_age'] = np.clip(data['customer_age'], 18, 80)
    return data

def get_car_age(data):
    data['registration_date'] = pd.to_datetime(data['registration_year'].astype(int).astype(str) + 
                                               '/' + data['registration_month'].astype(int).astype(str) + 
                                               '/01')
    # data['car_age'] = np.clip(round((data['policy_created_on'] - 
    #                                        data['registration_date']).dt.days/365), 0, 16)
    
    data['car_age'] = np.clip(round((data['policy_start_date'] - 
                                           data['registration_date']).dt.days/365), 0, 16)
    return data


max_ncb = {
    1 : 20, 
    2 : 25,
    3 : 35,
    4 : 45
}

def normalize_ncb(ncb, car_age):
    if car_age == 0:
        return 0
    elif car_age >= 5:
        res = ncb / 50
        return res if res <= 1 else np.nan
    else:
        res = ncb / max_ncb[car_age]
        return res if res <= 1 else np.nan

def get_normalized_ncb(data):
    data["normalized_ncb"] = data.progress_apply(lambda x : normalize_ncb(x["base_cover_ncb"], x["car_age"]), axis = 1)
    return data


def normalize_od_claim_history(od_claim_history, car_age):
    return od_claim_history / min(car_age, 5) if car_age > 0 else np.nan

def get_normalized_claim_hist(data):
    data["od_claim_count"] = data.progress_apply(lambda x : get_od_claim_history_date_accounted_updated(
                                                                x["claim_history"],
                                                                x["claim_history_cleaned"],
                                                                x["policy_created_on"],
                                                                x["registration_year"]), axis = 1)

    data["od_claim_count"] = data["od_claim_count"].fillna(0)
    data['norm_od_claim_hist'] = data.progress_apply(lambda x : normalize_od_claim_history(x['od_claim_count'], 
                                                                                   x['car_age']), axis=1)
    return data

def lower(x):
    if pd.isnull(x):
        return np.nan
    return x.lower()

def replace_numeric_strings_with_nan(insurer):
    if str(insurer).isnumeric():
        return np.nan
    return insurer

def clean_insurer_category(insurer):
    if pd.isnull(insurer):
        return np.nan
    else:
        # insurer = insurer.lower()
        insurer = lower(insurer)
        insurer = re.sub(r'^(the)\s', '', insurer, flags=re.IGNORECASE)
        insurer = insurer.strip()
        insurer = insurer.replace('-', ' ')
        insurer = insurer.split()
        insurer = ' '.join(insurer[:2])
        word_replacements = {'godigit' : 'go digit','go digital': 'go digit', 'iffco tokyo': 'iffco tokio','bharathi axa':  'bharti axa', 'bharati axa': 'bharti axa','univeral sompo': 'universal sompo', 'any others/': "", 'nan':""}
        # Replace words using a loop
        for old_word, new_word in word_replacements.items():
            insurer = insurer.replace(old_word, new_word)
        # insurer
        pattern = r"(don't|do not|dont)\s?\w?" 
        insurer = re.sub(pattern, '', insurer, flags=re.IGNORECASE)
        pattern = r"(general|gen.|generel|gen)\s?\w?"
        insurer = re.sub(pattern, '', insurer, flags=re.IGNORECASE)
        insurer = insurer.split()
        insurer = ' '.join(insurer[:1])
        insurer = replace_numeric_strings_with_nan(insurer)
        return insurer


# def get_previous_insurer_corrected(data):
#     insurer_corrector = previousInsurerProcessing(data, 'previous_insurer')
#     data = insurer_corrector.process()
#     return data

add_models = {
    'BND-AL10' : 'Honor 7 X', 
    'AC2001' : 'OnePlus Nord',
    'GM1901' : 'OnePlus', 
    'HD1901' : 'OnePlus', 
    'GM1911' : 'OnePlus',
    'KB2001' : 'OnePlus',
    'IN2011' : 'OnePlus',
    'EB2101' : 'OnePlus Nord',
    'DN2101' : 'OnePlus Nord',
    'LE2101' : 'OnePlus',
    'IN2021' : 'OnePlus', 
    'HD1911' : 'OnePlus',
    'LE2111' : 'OnePlus',
    }

def get_generalize_device_models(x):
    if pd.isnull(x): 
        return x
    # x = x.lower()
    if 'iPhone' in x:
        return 'iPhone'
    elif 'Mac' in x:
        return 'PC - Mac'
    elif 'SM-M' in x:
        return 'M Series'
    elif 'SM-N' in x:
        return 'Note Series'
    elif 'SM-T' in x:
        return 'Tab Series'
    elif ('SM-G' in x) or ('SM-S' in x):
        return 'S Series'
    elif 'SM-A' in x:
        return 'A Series'
    elif 'SM-X' in x:
        return 'Tab-S Series'
    elif 'SM-E' in x:
        return 'F Series'
    elif 'SM-J' in x:
        return 'J Series'
    elif 'SM-' in x:
        return 'Unclassified Samsung'
    elif 'Redmi Note' in x:
        return 'Redmi Note'
    elif 'Redmi' in x:
        return 'Redmi'
    elif 'Mi' in x:
        return 'Xiaomi'
    elif 'CPH1' in x:
        return 'Oppo - Lower'
    elif 'CPH2' in x:
        return 'Oppo - Mid'
    elif 'RMX' in x:
        return 'RealMe'
    elif 'LLD' in x:
        return 'Honor'
    elif ('oneplus' in x.lower()):
        return 'OnePlus'
    elif 'poco' in x.lower():
        return 'POCO by Xiaomi'
    elif 'moto' in x.lower():
        return 'Motorola'
    elif x.startswith('M2'):
        return 'Xiaomi'
    elif x.startswith('V2'):
        return 'vivo'
    elif x in add_models:
        return add_models[x]
    else:
        return np.nan
