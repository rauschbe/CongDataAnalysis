'''
Created on 17 Sep 2018

@author: rauschb
'''
from bs4 import BeautifulSoup as bs
import datetime
import re
import datetime
import pandas as pd
from threading import current_thread
import requests
import os
import io
import numpy as np
import glob
from sklearn import tree
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_recall_fscore_support
from sklearn.externals.six import StringIO
import pydot

#TODO mapping of Anforderer: 9900535000007(SHN), 4033872000058(TenneT),



def read_in_data(output = True, wdir = '/Users/benni/Desktop/Uni/Paper/Einsman/'):
    """
    reads in data from saved and downloaded Einsman-csv files for ava and edi
    can be easily appened in the same manner for all other downloaded data
    constructs a complete dataframe for every distinct grid operator
    Be careful about
    :param output: if True dataframe gets saved as a CSV in a set directory
    :return: dataframe per grid operator
    """
    #Set the reading directory
    ava = glob.glob('/Users/benni/Desktop/Uni/Paper/Einsman/ava/*.csv')
    edi = glob.glob('/Users/benni/Desktop/Uni/Paper/Einsman/EDI/*.csv')
    shn = glob.glob('/Users/benni/Desktop/Uni/Paper/Einsman/shn/*.csv')
    bag = glob.glob('/Users/benni/Desktop/Uni/Paper/Einsman/bag/*.csv')

    complete_ava = pd.DataFrame()
    complete_edi = pd.DataFrame()
    complete_shn = pd.DataFrame()
    complete_bag = pd.DataFrame()
    os.chdir(wdir)
    i = 0
    for f in sorted(ava):
        data = pd.read_csv(f, encoding = 'utf-8')
        data.rename(columns = {'Anlagenschlüssel':'Anlagenschluessel', 'Entschädigungspflicht':'Entschaedigungspflicht'}, inplace = True)
        data['Start'] = pd.to_datetime(data['Start'])
        data['Time (CET)'] = data['Start']
        complete_ava = complete_ava.append(data)
        print('Iteration: ',i)
        i = i + 1
    i = 0
    if output:
        complete_ava.to_csv('ava.csv', index=False)

    for f in sorted(edi):
        data = pd.read_csv(f, encoding = 'utf-8')
        data.rename(columns = {'Anlagenschlüssel':'Anlagenschluessel', 'Entschädigungspflicht':'Entschaedigungspflicht'}, inplace = True)
        data['Start'] = pd.to_datetime(data['Start'])
        data['Time (CET)'] = data['Start']
        data['Dauer(min)'] = data['Dauer(min)'].astype(int)
        complete_edi = complete_edi.append(data)
        print('Iteration: ',i)
        i = i + 1
    if output:
        complete_edi.to_csv('edi.csv', index = False)

    i = 0
    for f in sorted(shn):
        data = pd.read_csv(f, encoding='utf-8')
        data.rename(
            columns={'Anlagenschlüssel': 'Anlagenschluessel', 'Entschädigungspflicht': 'Entschaedigungspflicht'},
            inplace=True)
        data['Start'] = pd.to_datetime(data['Start'])
        data['Time (CET)'] = data['Start']
        complete_shn = complete_shn.append(data)
        print('Iteration: ', i)
        i = i + 1

    if output:
        complete_shn.to_csv('shn.csv', index = False)
    i = 0
    for f in sorted(bag):
        data = pd.read_csv(f, encoding='utf-8')
        data.rename(
            columns={'Anlagenschlüssel': 'Anlagenschluessel', 'Entschädigungspflicht': 'Entschaedigungspflicht'},
            inplace=True)
        data['Start'] = pd.to_datetime(data['Start'])
        data['Time (CET)'] = data['Start']
        complete_bag = complete_bag.append(data)
        print('Iteration: ', i)
        i = i + 1
    if output:
        complete_bag.to_csv('bag.csv', index = False)
    return complete_ava, complete_edi, complete_shn, complete_bag

def edit_data(complete_ava, complete_edi, output = False):
    """
    GOAL: only checks if any unit in the respective grid was used for einsman or not {0,1}
    edits data and creates an hourly dump for the data which is available in a minutios resolution
    it classifies a einsman-measure as done if it its duration is >= 30 min.
    Only shows if any unit was used for einsman
    :param complete_ava: avacon dataframe (source: read_in_data)
    :param complete_edi: edis dataframe (source: read_in_data)
    :param output: if True creates a dataframe each for avacon and edis with transformed data
    :return: 2 dataframes
    """
    c = ['Time (CET)', 'Start', 'Ende']
    time_frame = pd.DataFrame({'Time (CET)': pd.DatetimeIndex(start = '2015-01-02', end = '2017-12-31 23:59:59', freq = '1min')})
    complete_ava = pd.merge(time_frame, complete_ava, how='left', on=['Time (CET)', 'Time (CET)'])
    os.chdir('/Users/benni/Desktop/Uni/Paper/Einsman')
    complete_ava = complete_ava.drop_duplicates(subset = ['Time (CET)','Dauer(min)'])
    complete_ava[c] = complete_ava[c].apply(pd.to_datetime)
    complete_ava['Dauer(min)'] = complete_ava['Dauer(min)'].fillna(0)
    complete_ava['Dauer(min)'] = complete_ava['Dauer(min)'].astype(int)
    complete_ava = complete_ava.loc[complete_ava.index.repeat(complete_ava['Dauer(min)'] + 1)]
    complete_ava['Time (CET)'] += pd.to_timedelta(complete_ava.groupby(level=0).cumcount(), unit='s') * 60
    complete_ava = complete_ava.reset_index(drop=True)
    if output:
        complete_ava.to_csv('ava.csv', index = False, encoding = 'utf-8')
    os.chdir('/Users/benni/Desktop/Uni/Paper/Einsman')
    complete_edi = pd.merge(time_frame, complete_edi, how='left', on=['Time (CET)', 'Time (CET)'])
    complete_edi = complete_edi.drop_duplicates(subset = ['Time (CET)','Dauer(min)'])
    complete_edi[c] = complete_edi[c].apply(pd.to_datetime)
    complete_edi['Dauer(min)'] = complete_edi['Dauer(min)'].fillna(0)
    complete_edi['Dauer(min)'] = complete_edi['Dauer(min)'].astype(int)
    complete_edi = complete_edi.loc[complete_edi.index.repeat(complete_edi['Dauer(min)'] + 1)]
    complete_edi['Time (CET)'] += pd.to_timedelta(complete_edi.groupby(level=0).cumcount(), unit='s') * 60
    complete_edi = complete_edi.reset_index(drop=True)
    if output:
        complete_edi.to_csv('edi.csv', index = False, encoding = 'utf-8')
    return complete_ava, complete_edi

def edit_data_2(df, output = True,name = 'nonamegiven'):
    c = ['Time (CET)', 'Start', 'Ende']
    df['Time (CET)'] = pd.to_datetime(df['Time (CET)'], dayfirst = True, utc = True)
    print(df.head(1))
    time_frame = pd.DataFrame({'Time (CET)': pd.DatetimeIndex(start = '2015-01-02', end = '2017-12-31 23:59:59', freq = '1min')})
    time_frame['Time (CET)'] = pd.to_datetime(time_frame['Time (CET)'], utc = True)
    df = pd.merge(time_frame, df, how='left', on=['Time (CET)', 'Time (CET)'])
    os.chdir('/Users/benni/Desktop/Uni/Paper/Einsman')
    cdf = df.drop_duplicates(subset = ['Time (CET)','Dauer(min)'])
    df[c] = df[c].apply(pd.to_datetime)
    df['Dauer(min)'] = df['Dauer(min)'].fillna(0)
    df['Dauer(min)'] = df['Dauer(min)'].astype(int)
    df = df.loc[df.index.repeat(df['Dauer(min)'] + 1)]
    df['Time (CET)'] += pd.to_timedelta(df.groupby(level=0).cumcount(), unit='s') * 60
    df = df.reset_index(drop=True)
    if output:
        df.to_csv(name + '.csv', index = False, encoding = 'utf-8')


def edit_data_3(df, output = True,name = 'nonamegiven'):
    c = ['Time (CET)', 'Start', 'Ende']
    time_frame = pd.DataFrame({'Time (CET)': pd.DatetimeIndex(start = '2015-01-02', end = '2017-12-31 23:59:59', freq = '1min')})
    time_frame = pd.to_datetime(time_frame['Time (CET)'], utc = True)
    df['Time (CET)'] = pd.to_datetime(df['Time (CET)'], utc = True, dayfirst = True)
    df = pd.merge(time_frame, df, how='left', on=['Time (CET)', 'Time (CET)'])
    os.chdir('/Users/benni/Desktop/Uni/Paper/Einsman')
    df = df.drop_duplicates(subset = ['Time (CET)','Dauer(min)'])
    df[c] = df[c].apply(pd.to_datetime)
    df['Dauer(min)'] = df['Dauer(min)'].fillna(0)
    df['Dauer(min)'] = df['Dauer(min)'].astype(int)
    df = df.loc[df.index.repeat(df['Dauer(min)'] + 1)]
    df['Time (CET)'] += pd.to_timedelta(df.groupby(level=0).cumcount(), unit='s') * 60
    df = df.reset_index(drop=True).drop(['Start', 'Ende', 'Ort Engpass','Gebiet','Ursache', 'Netzbetreiber'], axis=1)
    df[name +'_einsman'] = np.where(df['Dauer(min)'] > 30, 1,0)
    os.chdir('/Users/benni/Desktop/Uni/Paper/Einsman')
    if output:
        df.to_csv(name + '_30minmerged.csv', index = False, encoding='utf-8')
    return df


def congestion_data():
    os.chdir('/Users/benni/Desktop/Uni/Paper')
    lineload15q1 = pd.read_csv('Leitungslast/Netzlast50HzQ12015.csv', encoding='ISO-8859-1', sep=None, engine='python')
    lineload15q2q3q41601 = pd.read_csv('Leitungslast/NetzlastQ220152016.csv', encoding='ISO-8859-1', sep=None,
                                       engine='python')
    lineload16q1 = pd.read_csv('Leitungslast/NetzlastQ150Hz2016.csv', encoding='ISO-8859-1', sep=None,
                               engine='python')  ##ACHTUNG erst ab 11.01.2016
    lineload16q2 = pd.read_csv('Leitungslast/NetzlastQ250Hz2016.csv', encoding='ISO-8859-1', sep=None, engine='python')
    lineload16q3 = pd.read_csv('Leitungslast/NetzlastQ350Hz2016.csv', encoding='ISO-8859-1', sep=None, engine='python')
    lineload16q4 = pd.read_csv('Leitungslast/NetzlastQ450Hz2016.csv', encoding='ISO-8859-1', sep=None, engine='python')
    lineload17 = pd.read_csv('Leitungslast/Netzlast50HzQ12017.csv', encoding='ISO-8859-1', sep=None, engine='python')

    frame = [lineload15q1, lineload15q2q3q41601, lineload16q1, lineload16q2, lineload16q3, lineload16q4, lineload17]

    lineload = pd.concat(frame, axis=0,
                         ignore_index=True)  # concatenating and joining new columns in all data frames with NaN as value
    lineload['Zeit'] = pd.to_datetime(lineload['Zeit'], dayfirst=True)
    lineload = lineload.fillna(
        'NaN')  # prefilling of NaN with a string object to allow string operations on all columns and to avoid exceptions

    for col in lineload.columns[0:-1]:  # iterating from first column to last
        lineload[col] = lineload[col].str.split('/').str[-1]  # split everything before '/'
        lineload[col] = lineload[col].str.strip()  # removing of whitespace before color indicator

    # print(lineload.apply(pd.value_counts(values='hoch')))
    # lineload1 = lineload.replace(['grün','grü','gr','g','orange','grau','rot','hoch','gelb','0'],[1,1,1,1,1,'NaN',0,0,1,'NaN'])
    # lineload1.to_csv('lineload1.csv')
    lineload2 = lineload.replace(['grün', 'grü', 'gr', 'g', 'orange', 'grau', 'rot', 'hoch', 'gelb'],
                                 [0, 0, 0, 0, 1, 'NaN', 1, 1, 1])
    lineload = lineload.replace(['grün', 'grü', 'gr', 'g', 'orange', 'grau', 'rot', 'hoch', 'gelb'],
                                [0, 0, 0, 0, 0, 'NaN', 1, 1, 0])  # replacing color indicator with binary code/NaN

    os.chdir('/Users/benni/Desktop/Uni/Paper/Einsman')
    lineload2.to_csv('congestion_data.csv', index = False)
    return lineload2

def decomposing_data(data, output = False,name = 'nonamegiven'):
    """
    GOAL: decomposes data on a unit level such that every unit is hourly shown if einsman
    measures were taken by a binary encoding
    Be aware you either use this function or the edit_data function
    :param data: complete_ava or complete_edi dataframe
    :param output: if True it generates a decomposed .csv / or .pkl (be aware it needs a lot of space)
    :return: decomposed data as pandas DataFrame (blocks a lot of RAM)
    """
    generation_units = list(data['Anlagen-ID'].unique())
    length = len(generation_units)
    print(length)
    time_frame = pd.DataFrame(data['Time (CET)'])
    helper = pd.DataFrame()
    os.chdir('/Users/benni/Desktop/Uni/Paper/Einsman/')
    cols = ['Start', 'Ende','Gebiet',
            'Ort Engpass', 'Ursache', 'Anforderer', 'Netzbetreiber',
            'Entschaedigungspflicht', 'Anlagenschluessel', 'ID', 'Einsatz-ID']
    data = data.dropna()
    del data['Start']
    del data['Ende']
    del data['Gebiet']
    del data['Ort Engpass']
    del data['Ursache']
    del data['Anforderer']
    del data['Netzbetreiber']
    del data['Entschaedigungspflicht']
    del data['Anlagenschluessel']
    del data['ID']
    del data['Einsatz-ID']
    i = 0
    for unit in generation_units:
        if str(unit) != 'nan':
            helper = data[data['Anlagen-ID'] == unit]
            helper = pd.merge(time_frame, helper, how='left', on=['Time (CET)', 'Time (CET)'])
            helper[unit] = np.where(helper['Dauer(min)'] > 30, 1, 0)
            del helper['Anlagen-ID']
            data[unit] = helper[unit]
            i = i + 1
            print('Progress: ',i/length*100,'%')
            if output:
                helper.to_csv('helper' + unit + '.csv', index = False)
        else:
            pass
    del data['Anlagen-ID']
    data.to_csv('decomposed_'+name +'.csv', index = False)
    return data


def corr_merged(congestion_data, einsman_data, output = False):
    line_list = [ 'Leitung Nr. 203', 'Leitung Nr. 205', 'Leitung Nr. 206',
                 'Leitung Nr. 207', 'Leitung Nr. 208', 'Leitung Nr. 211',
                 'Leitung Nr. 212', 'Leitung Nr. 225', 'Leitung Nr. 226',
                 'Leitung Nr. 231', 'Leitung Nr. 232', 'Leitung Nr. 275',
                 'Leitung Nr. 291', 'Leitung Nr. 293', 'Leitung Nr. 294',
                 'Leitung Nr. 295', 'Leitung Nr. 301', 'Leitung Nr. 302',
                 'Leitung Nr. 302/1', 'Leitung Nr. 303', 'Leitung Nr. 304',
                 'Leitung Nr. 304-303', 'Leitung Nr. 304-508', 'Leitung Nr. 305',
                 'Leitung Nr. 306', 'Leitung Nr. 313', 'Leitung Nr. 314',
                 'Leitung Nr. 315', 'Leitung Nr. 316', 'Leitung Nr. 316/1',
                 'Leitung Nr. 316/2', 'Leitung Nr. 317', 'Leitung Nr. 318',
                 'Leitung Nr. 319', 'Leitung Nr. 324', 'Leitung Nr. 328',
                 'Leitung Nr. 331', 'Leitung Nr. 332', 'Leitung Nr. 345',
                 'Leitung Nr. 346', 'Leitung Nr. 357', 'Leitung Nr. 358',
                 'Leitung Nr. 367', 'Leitung Nr. 368', 'Leitung Nr. 386',
                 'Leitung Nr. 413', 'Leitung Nr. 414', 'Leitung Nr. 415',
                 'Leitung Nr. 415/1', 'Leitung Nr. 415/2', 'Leitung Nr. 416',
                 'Leitung Nr. 419', 'Leitung Nr. 420', 'Leitung Nr. 421',
                 'Leitung Nr. 423', 'Leitung Nr. 424', 'Leitung Nr. 437',
                 'Leitung Nr. 438', 'Leitung Nr. 445', 'Leitung Nr. 446',
                 'Leitung Nr. 449', 'Leitung Nr. 450', 'Leitung Nr. 454',
                 'Leitung Nr. 459', 'Leitung Nr. 460', 'Leitung Nr. 461',
                 'Leitung Nr. 462', 'Leitung Nr. 463', 'Leitung Nr. 464',
                 'Leitung Nr. 467', 'Leitung Nr. 468', 'Leitung Nr. 471',
                 'Leitung Nr. 472', 'Leitung Nr. 475', 'Leitung Nr. 479',
                 'Leitung Nr. 489', 'Leitung Nr. 490', 'Leitung Nr. 491',
                 'Leitung Nr. 492', 'Leitung Nr. 493', 'Leitung Nr. 494',
                 'Leitung Nr. 495', 'Leitung Nr. 496', 'Leitung Nr. 498',
                 'Leitung Nr. 499', 'Leitung Nr. 500', 'Leitung Nr. 501',
                 'Leitung Nr. 502', 'Leitung Nr. 503', 'Leitung Nr. 504',
                 'Leitung Nr. 507', 'Leitung Nr. 507-306', 'Leitung Nr. 508',
                 'Leitung Nr. 509', 'Leitung Nr. 510', 'Leitung Nr. 512',
                 'Leitung Nr. 513', 'Leitung Nr. 514', 'Leitung Nr. 514/1',
                 'Leitung Nr. 514/2', 'Leitung Nr. 515', 'Leitung Nr. 516',
                 'Leitung Nr. 517', 'Leitung Nr. 518', 'Leitung Nr. 520',
                 'Leitung Nr. 521', 'Leitung Nr. 522', 'Leitung Nr. 526',
                 'Leitung Nr. 531', 'Leitung Nr. 532', 'Leitung Nr. 535',
                 'Leitung Nr. 536', 'Leitung Nr. 538', 'Leitung Nr. 538-535',
                 'Leitung Nr. 539', 'Leitung Nr. 540', 'Leitung Nr. 541',
                 'Leitung Nr. 542', 'Leitung Nr. 543', 'Leitung Nr. 544',
                 'Leitung Nr. 546', 'Leitung Nr. 547', 'Leitung Nr. 547/1',
                 'Leitung Nr. 547/2', 'Leitung Nr. 548', 'Leitung Nr. 548/1',
                 'Leitung Nr. 548/2', 'Leitung Nr. 551', 'Leitung Nr. 552',
                 'Leitung Nr. 553', 'Leitung Nr. 554', 'Leitung Nr. 555',
                 'Leitung Nr. 556', 'Leitung Nr. 557', 'Leitung Nr. 558',
                 'Leitung Nr. 559', 'Leitung Nr. 560', 'Leitung Nr. 562',
                 'Leitung Nr. 565', 'Leitung Nr. 566', 'Leitung Nr. 567',
                 'Leitung Nr. 568', 'Leitung Nr. 571', 'Leitung Nr. 572',
                 'Leitung Nr. 573', 'Leitung Nr. 574', 'Leitung Nr. 575',
                 'Leitung Nr. 577', 'Leitung Nr. 577/1', 'Leitung Nr. 577/2',
                 'Leitung Nr. 578', 'Leitung Nr. 578/1', 'Leitung Nr. 578/2',
                 'Leitung Nr. 585', 'Leitung Nr. 586', 'Leitung Nr. 587',
                 'Leitung Nr. 588', 'Leitung Nr. 589', 'Leitung Nr. 590',
                 'Leitung Nr. 591', 'Leitung Nr. 592', 'Leitung Nr. 594',
                 'Leitung Nr. 596', 'Leitung Nr. 903', 'Leitung Nr. 903/1',
                 'Leitung Nr. 904', 'Leitung Nr. 906', 'Leitung Nr. 906/1',
                 'Leitung Nr. 907', 'Leitung Nr. 908', 'Leitung Nr. 919',
                 'Leitung Nr. 919/1', 'Leitung Nr. 920', 'Leitung Nr. 920/1',
                 'Leitung Nr. 921', 'Leitung Nr. 922', 'Leitung Nr. 949',
                 'Leitung Nr. 951', 'Leitung Nr. 960', 'Leitung Nr. 961',
                 'Leitung Nr. 962', 'Leitung Nr. 971', 'Leitung Nr. 972',
                 'Leitung Nr. 981', 'Leitung Nr. 982', 'Leitung Nr. 991',
                 'Leitung Nr. 992', 'Leitung Nr. 993', 'Leitung Nr. 994',
                 'Leitung Nr. G', 'Leitung Nr. G/1', 'Leitung Nr. GRUEN',
                 'Leitung Nr. H', 'Leitung Nr. HAV 1', 'Leitung Nr. HAV 2',
                 'Leitung Nr. HGUE1', 'Leitung Nr. HaN Gr', 'Leitung Nr. K6',
                 'Leitung Nr. ROT', 'Leitung Nr. VIE']
    congestion_data['Time (CET)'] = congestion_data['Zeit']
    del congestion_data['Zeit']
    congestion_data['Time (CET)'] = pd.to_datetime(congestion_data['Time (CET)'])
    einsman_data = einsman_data.iloc[::60,:]
    einsman_data['Time (CET)'] = pd.to_datetime(einsman_data['Time (CET)'])
    corrava = []
    corredi = []
    if output:
        merged_df.to_csv('einspluscong.csv')
    for col in line_list:
        line = congestion_data[['Time (CET)', col]]
        einsman_data['Anforderer_x'] = einsman_data['Anforderer_x'].astype(str)
        einsman_data = einsman_data.reset_index(drop = True)
        merged_df = pd.merge(line, einsman_data, on = 'Time (CET)')
        merged_df = merged_df.set_index('Time (CET)')
        merged_df[(merged_df['Anforderer_x'] == '50 Hertz') | (merged_df['Anforderer_x'] == 'nan')]
        corravacong = merged_df[col].corr(merged_df['ava_einsman'])
        corrava.append(corravacong)
    for col in line_list:
        line = congestion_data[['Time (CET)', col]]
        einsman_data['Anforderer_y'] = einsman_data['Anforderer_y'].astype(str)
        einsman_data = einsman_data.reset_index(drop = True)
        merged_df = pd.merge(line, einsman_data, on='Time (CET)')
        merged_df = merged_df.dropna(subset = [col])
        merged_df[(merged_df['Anforderer_y'] == '50 Hertz') | (merged_df['Anforderer_y'] == 'nan')]
        corredicong = merged_df[col].corr(merged_df['edi_einsman'])
        corredi.append(corredicong)
    df_corrava = pd.DataFrame()
    df_corredi = pd.DataFrame()
    df_corrava['Leitungen'] = pd.Series(line_list)
    df_corrava['corr'] = pd.Series(corrava)
    df_corredi['Leitungen'] = pd.Series(line_list)
    df_corredi['corr'] = pd.Series(corredi)
    df_corrava = df_corrava.sort_values(by = 'corr', ascending= False)
    df_corredi = df_corredi.sort_values(by = 'corr', ascending = False)
    return df_corrava, df_corredi

def corr_unit(congestion_data, einsman_data):
    line_list = [ 'Leitung Nr. 203', 'Leitung Nr. 205', 'Leitung Nr. 206',
                 'Leitung Nr. 207', 'Leitung Nr. 208', 'Leitung Nr. 211',
                 'Leitung Nr. 212', 'Leitung Nr. 225', 'Leitung Nr. 226',
                 'Leitung Nr. 231', 'Leitung Nr. 232', 'Leitung Nr. 275',
                 'Leitung Nr. 291', 'Leitung Nr. 293', 'Leitung Nr. 294',
                 'Leitung Nr. 295', 'Leitung Nr. 301', 'Leitung Nr. 302',
                 'Leitung Nr. 302/1', 'Leitung Nr. 303', 'Leitung Nr. 304',
                 'Leitung Nr. 304-303', 'Leitung Nr. 304-508', 'Leitung Nr. 305',
                 'Leitung Nr. 306', 'Leitung Nr. 313', 'Leitung Nr. 314',
                 'Leitung Nr. 315', 'Leitung Nr. 316', 'Leitung Nr. 316/1',
                 'Leitung Nr. 316/2', 'Leitung Nr. 317', 'Leitung Nr. 318',
                 'Leitung Nr. 319', 'Leitung Nr. 324', 'Leitung Nr. 328',
                 'Leitung Nr. 331', 'Leitung Nr. 332', 'Leitung Nr. 345',
                 'Leitung Nr. 346', 'Leitung Nr. 357', 'Leitung Nr. 358',
                 'Leitung Nr. 367', 'Leitung Nr. 368', 'Leitung Nr. 386',
                 'Leitung Nr. 413', 'Leitung Nr. 414', 'Leitung Nr. 415',
                 'Leitung Nr. 415/1', 'Leitung Nr. 415/2', 'Leitung Nr. 416',
                 'Leitung Nr. 419', 'Leitung Nr. 420', 'Leitung Nr. 421',
                 'Leitung Nr. 423', 'Leitung Nr. 424', 'Leitung Nr. 437',
                 'Leitung Nr. 438', 'Leitung Nr. 445', 'Leitung Nr. 446',
                 'Leitung Nr. 449', 'Leitung Nr. 450', 'Leitung Nr. 454',
                 'Leitung Nr. 459', 'Leitung Nr. 460', 'Leitung Nr. 461',
                 'Leitung Nr. 462', 'Leitung Nr. 463', 'Leitung Nr. 464',
                 'Leitung Nr. 467', 'Leitung Nr. 468', 'Leitung Nr. 471',
                 'Leitung Nr. 472', 'Leitung Nr. 475', 'Leitung Nr. 479',
                 'Leitung Nr. 489', 'Leitung Nr. 490', 'Leitung Nr. 491',
                 'Leitung Nr. 492', 'Leitung Nr. 493', 'Leitung Nr. 494',
                 'Leitung Nr. 495', 'Leitung Nr. 496', 'Leitung Nr. 498',
                 'Leitung Nr. 499', 'Leitung Nr. 500', 'Leitung Nr. 501',
                 'Leitung Nr. 502', 'Leitung Nr. 503', 'Leitung Nr. 504',
                 'Leitung Nr. 507', 'Leitung Nr. 507-306', 'Leitung Nr. 508',
                 'Leitung Nr. 509', 'Leitung Nr. 510', 'Leitung Nr. 512',
                 'Leitung Nr. 513', 'Leitung Nr. 514', 'Leitung Nr. 514/1',
                 'Leitung Nr. 514/2', 'Leitung Nr. 515', 'Leitung Nr. 516',
                 'Leitung Nr. 517', 'Leitung Nr. 518', 'Leitung Nr. 520',
                 'Leitung Nr. 521', 'Leitung Nr. 522', 'Leitung Nr. 526',
                 'Leitung Nr. 531', 'Leitung Nr. 532', 'Leitung Nr. 535',
                 'Leitung Nr. 536', 'Leitung Nr. 538', 'Leitung Nr. 538-535',
                 'Leitung Nr. 539', 'Leitung Nr. 540', 'Leitung Nr. 541',
                 'Leitung Nr. 542', 'Leitung Nr. 543', 'Leitung Nr. 544',
                 'Leitung Nr. 546', 'Leitung Nr. 547', 'Leitung Nr. 547/1',
                 'Leitung Nr. 547/2', 'Leitung Nr. 548', 'Leitung Nr. 548/1',
                 'Leitung Nr. 548/2', 'Leitung Nr. 551', 'Leitung Nr. 552',
                 'Leitung Nr. 553', 'Leitung Nr. 554', 'Leitung Nr. 555',
                 'Leitung Nr. 556', 'Leitung Nr. 557', 'Leitung Nr. 558',
                 'Leitung Nr. 559', 'Leitung Nr. 560', 'Leitung Nr. 562',
                 'Leitung Nr. 565', 'Leitung Nr. 566', 'Leitung Nr. 567',
                 'Leitung Nr. 568', 'Leitung Nr. 571', 'Leitung Nr. 572',
                 'Leitung Nr. 573', 'Leitung Nr. 574', 'Leitung Nr. 575',
                 'Leitung Nr. 577', 'Leitung Nr. 577/1', 'Leitung Nr. 577/2',
                 'Leitung Nr. 578', 'Leitung Nr. 578/1', 'Leitung Nr. 578/2',
                 'Leitung Nr. 585', 'Leitung Nr. 586', 'Leitung Nr. 587',
                 'Leitung Nr. 588', 'Leitung Nr. 589', 'Leitung Nr. 590',
                 'Leitung Nr. 591', 'Leitung Nr. 592', 'Leitung Nr. 594',
                 'Leitung Nr. 596', 'Leitung Nr. 903', 'Leitung Nr. 903/1',
                 'Leitung Nr. 904', 'Leitung Nr. 906', 'Leitung Nr. 906/1',
                 'Leitung Nr. 907', 'Leitung Nr. 908', 'Leitung Nr. 919',
                 'Leitung Nr. 919/1', 'Leitung Nr. 920', 'Leitung Nr. 920/1',
                 'Leitung Nr. 921', 'Leitung Nr. 922', 'Leitung Nr. 949',
                 'Leitung Nr. 951', 'Leitung Nr. 960', 'Leitung Nr. 961',
                 'Leitung Nr. 962', 'Leitung Nr. 971', 'Leitung Nr. 972',
                 'Leitung Nr. 981', 'Leitung Nr. 982', 'Leitung Nr. 991',
                 'Leitung Nr. 992', 'Leitung Nr. 993', 'Leitung Nr. 994',
                 'Leitung Nr. G', 'Leitung Nr. G/1', 'Leitung Nr. GRUEN',
                 'Leitung Nr. H', 'Leitung Nr. HAV 1', 'Leitung Nr. HAV 2',
                 'Leitung Nr. HGUE1', 'Leitung Nr. HaN Gr', 'Leitung Nr. K6',
                 'Leitung Nr. ROT', 'Leitung Nr. VIE']
    congestion_data['Time (CET)'] = congestion_data['Zeit']
    congestion_data['Time (CET)'] = pd.to_datetime(congestion_data['Time (CET)'])
    einsman_data = einsman_data.iloc[::60,:]
    einsman_data['Time (CET)'] = pd.to_datetime(einsman_data['Time (CET)'])
    generation_units = list(einsman_data.columns)
    generation_units = generation_units[15:]
    corrava = {}
    for col in line_list:
        print(col)
        for unit in generation_units:
            line = congestion_data[['Time (CET)', col]]
            helper = einsman_data[['Time (CET)', unit]]
            merged_df = pd.merge(line, helper, on = 'Time (CET)')
            merged_df = merged_df.dropna(subset = [col])
            merged_df[col] = merged_df[col].astype(float)
            corr = merged_df[col].corr(merged_df[unit])
            if str(corr) != 'nan':
                corrava[col + '/' + unit] = corr
            if abs(corr) > 0.4:
                print(col,'Einspeiser: ',unit,'Korrelation: ',corr)
    return corrava


def einsman_tree(congestion_data, einsman_data):
    congestion_data['Time (CET)'] = congestion_data['Zeit']
    del congestion_data['Zeit']
    congestion_data['Time (CET)'] = pd.to_datetime(congestion_data['Time (CET)'])
    einsman_data = einsman_data.iloc[::60,:]
    einsman_data['Time (CET)'] = pd.to_datetime(einsman_data['Time (CET)'])
    einsman_data = einsman_data.drop(['Anforderer_x','Anforderer_y'], axis = 1)
    ava = pd.merge(congestion_data, einsman_data[['ava_einsman','Time (CET)']],
                   on = 'Time (CET)')
    edi = pd.merge(congestion_data, einsman_data[['edi_einsman', 'Time (CET)']],
                   on = 'Time (CET)')
    ava = ava.drop('Time (CET)', axis = 1)
    ava = ava.fillna(0)
    edi = edi.fillna(0)
    edi = edi.drop('Time (CET)', axis = 1)
    ava = ava.as_matrix()
    edi = edi.as_matrix()
    X_ava = ava[:, 0:-1]
    y_ava = ava[:,-1]
    X_edi = edi[:, 0:-1]
    y_edi = edi[:,-1]
    clf = tree.DecisionTreeClassifier(min_impurity_split=1e-5)
    clf.fit(X_ava, y_ava)
    dot_data = StringIO()
    tree.export_graphviz(clf, out_file=dot_data)
    graph = pydot.graph_from_dot_data(dot_data.getvalue())
    graph[0].write_pdf('ava.pdf')
    clf.fit(X_edi, y_edi)
    tree.export_graphviz(clf, out_file = dot_data)
    graph = pydot.graph_from_dot_data(dot_data.getvalue())
    graph[0].write_pdf('edi.pdf')
    print('EDI',clf.score(X_edi,y_edi))
    print('AVA',clf.score(X_ava,y_ava))
    return 'finished'




os.chdir('/Users/benni/Desktop/Uni/Paper/Einsman')
'''
congestion_data = pd.read_csv('congestion_data.csv')
complete_ava, complete_edi = read_in_data()
complete_ava, complete_edi = edit_data(complete_ava, complete_edi)
'''


'''
congestion_data = pd.read_csv('congestion_data.csv')
complete_ava, complete_edi = read_in_data()
einsman_data = edit_data_merged(complete_ava, complete_edi)
results = einsman_tree(congestion_data, einsman_data)
'''



bag = pd.read_csv('bag.csv')
decomposing_data(bag, name = 'bag')
edi = pd.read_csv('edi.csv')
decomposing_data(edi, name = 'edi')


#corrava, corredi = corr_merged(congestion_data, einsman_data, output = False)
#corrava.to_csv('corrava.csv', index = False)
#corredi.to_csv('corredi.csv', index = False)



#congestion_data = pd.read_csv('congestion_data.csv')
#einsman_data = pd.read_csv('helper_df/decomposed_edi.csv')
#einsman_data = einsman_data.drop_duplicates(subset = ['Time (CET)'])
#corr = corr(congestion_data, einsman_data)
#print(corr)
#corr = pd.DataFrame(corr)
#print(corr)





#complete_ava = decomposing_data(complete_ava)
#complete_ava.to_csv('decomposed_ava.csv', index = False)
