import pandas as pd
import numpy as np




def descriptor_einsman(df, name = "no name defined", limited = True):
    df['Time (CET)'] = pd.to_datetime(df['Time (CET)'], utc = True)
    df['Time (CET)'] = df['Time (CET)']
    df.sort_values(by = 'Time (CET)', inplace = True)
    if limited:
        df = df[(df['Time (CET)'].dt.year < 2018)]
    col_list = ['ID', 'Einsatz-ID', 'Dauer(min)', 'Gebiet',
                'Ort Engpass', 'Stufe(%)', 'Ursache', 'Anlagenschluessel', 'Anforderer',
                'Netzbetreiber', 'Anlagen-ID', 'Entschaedigungspflicht', 'Time (CET)']
    for i in col_list:
        if i == 'Time (CET)':
            print('Starting date', df[i].head(1))
            print('Ending date', df[i].tail(1))
        elif i in ['Ursache', 'Stufe(%)', 'Anforderer',
                   'Netzbetreiber', 'Entschaedigungspflicht']:
            print('Unique values of {}'.format(i), df[i].unique())
        elif i == "Dauer(min)":
            print('Durchschnittliche Dauer', df[i].mean())
            print('Median Dauer', df[i].median())
            print('Std Dauer', df[i].std())
            print('Max Dauer', df[i].max())
            print('Min Dauer', df[i].min())
        else:
            print('Length of {}'.format(i), len(df[i].unique()))
    print("finished processing of {} \n".format(name))
    return


def filter_tso(df, tso = 'none', limited = True, output = True, name = 'no_name', high_resolution = False):
    df['Time (CET)'] = pd.to_datetime(df['Time (CET)'], utc = True)
    df['Time (CET)'] = df['Time (CET)']
    df.sort_values(by = 'Time (CET)', inplace = True)
    df.drop(columns = ['ID', 'Einsatz-ID', 'Start', 'Ende', 'Anlagen-ID', 'Entschaedigungspflicht'], inplace = True)
    if limited:
        df = df[(df['Time (CET)'].dt.year < 2018)]
    if tso not in ['50 Hertz','50Hertz', 'TenneT', 'tennet']:
        print('No tso name given - no processing')
    elif tso in ['50 Hertz','50Hertz']:
        df.Anforderer = df.Anforderer.apply(str)
        df = df[df.Anforderer.str.contains('Hertz')]
    elif tso == ['TenneT','tennet']:
        df.Anforderer = df.Anforderer.apply(str)
        df.Anforderer = df.Anforderer.replace(['4033872000058','4050404000003'], 'TenneT')
        df = df[df.Anforderer.str.contains(tso)]
    if high_resolution:
        time_frame = pd.DataFrame()
        time_frame['Time (CET)'] = pd.date_range(start='2015-01-01', end='2017-12-31 23:59:59',
                                                 freq='s')
        time_frame['Time (CET)'] = pd.to_datetime(time_frame['Time (CET)'], utc=True)
        df = pd.merge(time_frame, df, how='left', on=['Time (CET)', 'Time (CET)'])
    if output:
        df.to_csv('/Users/benni/PycharmProjects/CongDataAnalysis/Filtered EinsManData/' +
                  name + '_filtered_for_' + tso + '.csv', index=False)
    print('finished filtering of dataframe {name} for TSO {tso}'.format(name = name, tso = tso))
    return df


def read_in_redispatch_data(filter = 'none', split = True, translate = True, output = True):
    time_frame = pd.DataFrame()
    time_frame['Begin Time (CET)'] = pd.date_range(start='2015-01-01', end='2017-12-31 23:59:59', freq='H')
    redispatch = pd.read_csv('/Users/benni/PycharmProjects/CongDataAnalysis/redispatch_data/redispatch_netztranparenz.csv',
                        encoding='ISO-8859-1', sep=None, engine='python')
    redispatch = redispatch.loc[:, ~redispatch.columns.str.contains('^Unnamed')]
    redispatch['Beginn Zeit (CET)'] = pd.to_datetime(redispatch['BEGINN_DATUM'] + ' ' + redispatch['BEGINN_UHRZEIT'],
                                                    errors = 'coerce', dayfirst = True)
    redispatch['Ende Zeit (CET)'] = pd.to_datetime(redispatch['ENDE_DATUM'] + ' ' + redispatch['ENDE_UHRZEIT'],
                                                  errors = 'coerce', dayfirst = True)
    redispatch = redispatch[(redispatch['Beginn Zeit (CET)'].dt.year > 2014) &
                            (redispatch['Ende Zeit (CET)'].dt.year < 2018)]
    redispatch.drop(columns = ['BEGINN_DATUM','BEGINN_UHRZEIT','ENDE_DATUM', 'ENDE_UHRZEIT'], inplace = True)
    redispatch = redispatch[redispatch['GRUND_DER_MASSNAHME'] != 'Spannungsbedingter Redispatch']
    redispatch = redispatch[~redispatch['GRUND_DER_MASSNAHME'].isnull()]
    redispatch = redispatch.dropna(subset=['Beginn Zeit (CET)', 'Ende Zeit (CET)','GRUND_DER_MASSNAHME'])
    redispatch['Dauer'] = redispatch['Ende Zeit (CET)'] - redispatch['Beginn Zeit (CET)']
    redispatch['Dauer'] = round(redispatch['Dauer'] / np.timedelta64(60,'m'))
    redispatch['Dauer'] = redispatch['Dauer'].fillna(0)
    redispatch = redispatch[(redispatch.Dauer > 0)] #only include redispatch longer than one minute
    redispatch['Dauer'] = redispatch['Dauer'].astype(int)
    redispatch = redispatch.loc[redispatch.index.repeat(redispatch['Dauer'] + 1)]
    redispatch['Beginn Zeit (CET)'] += pd.to_timedelta(redispatch.groupby(level=0).cumcount(), unit='h')
    redispatch = redispatch.reset_index(drop=True)
    if translate:
        redispatch.rename(columns = {
       'NETZREGION':'Net_region', 'GRUND_DER_MASSNAHME':'Reason_for_measure', 'RICHTUNG':'Direction',
        'MITTLERE_LEISTUNG_MW':'Mean_power_MW', 'MAXIMALE_LEISTUNG_MW':'MAX_POWER_MW',
       'GESAMTE_ARBEIT_MWH':'TOTAL_WORK_MWH', 'ANWEISENDER_UENB':'DIRECTING_TSO',
       'ANFORDERNDER_UENB':'REQUESTING_TSO', 'BETROFFENE_ANLAGE':'AFFECTED_PS', 'Redispatch_KW':'Redispatch_KW',
        'Beginn Zeit (CET)':'Begin Time (CET)', 'Ende Zeit (CET)': 'End Time (CET)', 'Dauer':'Duration_hour' }, inplace = True)
    if split:
        redispatch['Direction'] = redispatch['Direction'].replace('Wirkleistungseinspeisung reduzieren',
                                                            'decrease')
        redispatch['Direction'] = redispatch['Direction'].str.replace(r'(^.*Wirk.*)',
                                                                'increase')
    if filter in ['50Hertz', 'TenneT', 'TransnetBW', 'Amprion']:
        redispatch = redispatch[redispatch['REQUESTING_TSO'].str.contains(filter)]
        redispatch = redispatch.reset_index(drop=True)
        print('Data has been filtered for TSO {}'.format(filter))
    else:
        print('Data has not been filtered or does not match needed filter')
    result = pd.merge(time_frame, redispatch,how='left', on=['Begin Time (CET)', 'Begin Time (CET)'])
    if output:
        result.to_csv('redispatch_data/Transformed_redispatch_data/redispatch_filtered_for_TSO_{}'.format(filter) + '.csv',
                      index = False, encoding = 'utf-8')
        if split:
            result[result['Direction'] == 'increase'].to_csv(
                'redispatch_data/Transformed_redispatch_data/redispatch_filtered_for_TSO_{}_increase'.format(filter) + '.csv',
                index=False, encoding = 'ISO-8859-1')
            result[result['Direction'] == 'decrease'].to_csv(
                'redispatch_data/Transformed_redispatch_data/redispatch_filtered_for_TSO_{}_decrease'.format(
                    filter) + '.csv', index=False, encoding = 'ISO-8859-1')
    return result

def binarize_redispatch(df, output = True, name = 'no_filter'):
    df.drop(
        columns=['Net_region', 'Reason_for_measure', 'Mean_power_MW', 'MAX_POWER_MW', 'TOTAL_WORK_MWH', 'DIRECTING_TSO',
                 'REQUESTING_TSO', 'AFFECTED_PS', 'Redispatch_KW', 'Duration_hour',
                'End Time (CET)'], inplace=True)
    df['Direction'] = df['Direction'].str.replace('decrease', '-1')
    df['Direction'] = df['Direction'].replace('increase', '1')
    df['Begin Time (CET)'] = pd.to_datetime(df['Begin Time (CET)'])
    df = df.drop_duplicates(subset='Begin Time (CET)')
    df['Direction'] = df['Direction'].astype(str)
    df['Direction'] = df['Direction'].str.replace('nan', '0')
    df['Direction'] = df['Direction'].astype(int)
    print('Data is binarized - ready for classification')
    if output:
        df.to_csv('redispatch_data/binarized_redispatch/binarized_redispatch_for_TSO_{}.csv'.format(name), index = False)
        print('Binarized output generated for TSO {}'.format(name))
    return df

def binarize_einsman(df, output = True, hourly = True, name = 'no name defined'):
    time_frame = pd.DataFrame(
        {'Time (CET)': pd.date_range(start='2015-01-02', end='2017-12-31 23:59:59', freq='1min')})
    time_frame = pd.to_datetime(time_frame['Time (CET)'], utc=True)
    df['Time (CET)'] = pd.to_datetime(df['Time (CET)'], utc=True, dayfirst=True)
    df = df[df['Time (CET)'].dt.year < 2018]
    df = pd.merge(time_frame, df, how='left', on=['Time (CET)', 'Time (CET)'])
    df = df.drop_duplicates(subset=['Time (CET)', 'Dauer(min)'])
    df['Dauer(min)'] = df['Dauer(min)'].fillna(0)
    df['Dauer(min)'] = df['Dauer(min)'].astype(int)
    df = df.loc[df.index.repeat(df['Dauer(min)'] + 1)]
    df['Time (CET)'] += pd.to_timedelta(df.groupby(level=0).cumcount(), unit='m')
    df[name + '_einsman'] = np.where(df['Dauer(min)'] > 30, 1, 0)
    df = df.drop_duplicates(subset='Time (CET)')
    df = df[df['Time (CET)'].dt.year < 2018]
    df = df.set_index('Time (CET)', drop = True)
    df.drop(columns = ['Dauer(min)', 'Gebiet', 'Ort Engpass', 'Stufe(%)', 'Ursache',
       'Anlagenschluessel', 'Anforderer', 'Netzbetreiber'], inplace = True)
    if hourly:
        df = df.resample('H').pad()
    if output:
        df.to_csv('binarized_einsman/'+name + '_30minmerged.csv', encoding='utf-8')
    return df

def merger(output = True):
    ava = pd.read_csv('/Users/benni/PycharmProjects/CongDataAnalysis/binarized_einsman/50Hertz_ava_30minmerged.csv')
    edi = pd.read_csv('/Users/benni/PycharmProjects/CongDataAnalysis/binarized_einsman/50Hertz_edi_30minmerged.csv')
    ava['50Hertz_edi_einsman'] = edi['50Hertz_edi_einsman']
    redispatch_binned = pd.read_csv('redispatch_data/binarized_redispatch/binarized_redispatch_for_TSO_50Hertz.csv')
    redispatch_binned.rename(columns={'Begin Time (CET)': 'Time (CET)'}, inplace=True)
    ava['Time (CET)'] = pd.to_datetime(ava['Time (CET)'], utc = True)
    redispatch_binned['Time (CET)'] = pd.to_datetime(redispatch_binned['Time (CET)'], utc=True)
    lineload = bin_lineload_data(output = False)
    merged = pd.merge(redispatch_binned, ava, on=['Time (CET)', 'Time (CET)'], how='left')
    merged = pd.merge(lineload, merged, on = ['Time (CET)', 'Time (CET)'], how = 'left')
    merged['Direction'] = merged['Direction'].astype(str)
    merged['Direction'] = merged['Direction'].str.replace('-1','1')
    merged['Direction'] = merged['Direction'].astype(int)
    if output:
        merged.to_csv('merged_all_cong_data_IMP.csv', index = False)
    return merged


def bin_lineload_data(output = True):
    lineload15q1 = pd.read_csv('Leitungslast/Netzlast50HzQ12015.csv', encoding='ISO-8859-1', sep=None, engine='python')
    lineload15q2q3q41601 = pd.read_csv('Leitungslast/NetzlastQ220152016.csv', encoding='ISO-8859-1', sep=None,
                                       engine='python')
    lineload16q1 = pd.read_csv('Leitungslast/NetzlastQ150Hz2016.csv', encoding='ISO-8859-1', sep=None,
                               engine='python')  ##ACHTUNG erst ab 11.01.2016
    lineload16q2 = pd.read_csv('Leitungslast/NetzlastQ250Hz2016.csv', encoding='ISO-8859-1', sep=None, engine='python')
    lineload16q3 = pd.read_csv('Leitungslast/NetzlastQ350Hz2016.csv', encoding='ISO-8859-1', sep=None, engine='python')
    lineload16q4 = pd.read_csv('Leitungslast/NetzlastQ450Hz2016.csv', encoding='ISO-8859-1', sep=None, engine='python')
    lineload17 = pd.read_csv('Leitungslast/Netzlast50HzQ12017.csv', encoding='ISO-8859-1', sep=None, engine='python')
    lineload172 = pd.read_csv('Leitungslast/Netzlast2017abMai.csv', encoding='ISO-8859-1', sep=None, engine='python')

    frame = [lineload15q1, lineload15q2q3q41601, lineload16q1, lineload16q2, lineload16q3, lineload16q4, lineload17]

    time_frame = pd.DataFrame(
        {'Zeit': pd.date_range(start='2015-01-01', end='2017-12-31 23:59:59', freq='H')})
    lineload172['Zeit'] = pd.date_range(start='5/31/2017', end='12/31/2017', freq='H')
    lineload = pd.concat(frame, axis=0, ignore_index=True,
                         sort=True)  # concatenating and joining new columns in all data frames with NaN as value
    lineload['Zeit'] = pd.to_datetime(lineload['Zeit'], dayfirst=True)
    lineload = pd.concat([lineload, lineload172], axis=0, ignore_index=True, sort=True)
    lineload = lineload.fillna(
        'NaN')  # prefilling of NaN with a string object to allow string operations on all columns and to avoid exceptions
    for col in lineload.columns[0:-1]:  # iterating from first column to last
        lineload[col] = lineload[col].str.split('/').str[-1]  # split everything before '/'
        lineload[col] = lineload[col].str.strip()  # removing of whitespace before color indicator
    lineload = lineload.replace(['grün', 'grü', 'gr', 'g', 'gr,n', 'orange', 'grau', 'rot', 'hoch', 'gelb'],
                                [0, 0, 0, 0, 0, 1, 'NaN', 1, 1, 1])
    lineload = pd.merge(time_frame, lineload, how='left', on=['Zeit', 'Zeit'])
    lineload = lineload.set_index('Zeit', drop=True)
    col_list = lineload.columns
    lineload['CongestedLine_yellow'] = lineload[col_list].eq(1).any(axis=1).astype(int)
    lineload.drop(columns=col_list, inplace=True)
    lineload = lineload.reset_index()
    lineload.rename(columns={'Zeit': 'Time (CET)'}, inplace=True)
    lineload['Time (CET)'] = pd.to_datetime(lineload['Time (CET)'], utc = True)
    if output:
        lineload.to_csv('Leitungslast/merged_lineload_binned.csv')
    return lineload
