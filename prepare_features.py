import pandas as pd
import datetime
import os
import glob
from datetime import date

Y= 2000 #dummy  year
#4 seasons per year 1=winter, 2=spring, 3=summer,4=autumn
seasons = [('1',(date(Y,1,1), date(Y,3,20))),
           ('2', (date(Y,3,21), date(Y,6,20))),
           ('3', (date(Y,6,21), date(Y,9,22))),
           ('4', (date(Y,9,23), date(Y,12,20))),
           ('1', (date(Y,12,21), date(Y,12,31)))]

def get_season(now):
    if isinstance(now, datetime):
        now = now.date()
    now = now.replace(year = Y) #replacing year with dummy variable to allow converting
    return next(season for season, (start,end) in seasons
                if start <= now <= end)

#autodetecting zone since CET zone is home zone of Karlsruhe,GER
from datetime import datetime
import time
def datetime_from_utc_to_local(utc_datetime):
    now_timestamp = time.mktime(utc_datetime.timetuple())
    offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset

#rescaling data to [-1,1] usage per column
def rescaler(df):
    df = (df - df.mean()) / (df.max() - df.min())
    return df
#z-transformation/studentisation ~ N(0,1) usage per column
def studentization(df):
    df = (df - df.mean())/(df.var())
    return df
#creating holiday calendar for public holidays in control zone of 50 Hz, adding reformation day to normal federal holidays which occur in every german state

from pandas.tseries.holiday import Holiday,AbstractHolidayCalendar

class GermanHolidays(AbstractHolidayCalendar):
    rules = [Holiday('New Years Day', month=1, day=1),
             Holiday('First of May', month=5, day=1),
             Holiday('German Unity Day', month=10,day=3),
             Holiday('First Christmas Day', month=12,day=25),
             Holiday('Second Christmas Day', month=12, day=26),
             Holiday('Reformation Day', month=10,day=31),
             Holiday('Karfreitag 2015',year=2015,month=4,day=3),
             Holiday('Karfreitag 2016', year=2016,month=3,day=25),
             Holiday('Karfreitag 2017', year=2017, month=4,day=14),
             Holiday('Ostermontag 2015', year=2015,month=4,day=6),
             Holiday('Ostermontag 2016', year=2016,month=3,day=28),
             Holiday('Ostermontag 2017', year=2017,month=4,day=17),
             Holiday('Christi Himmelfahrt 2015', year=2015,month=5,day=14),
             Holiday('Christi Himmelfahrt 2016', year=2016,month=5,day=5),
             Holiday('Christi Himmelfahrt 2017', year=2017,month=5,day=25),
             Holiday('Pfingstmontag 2015', year=2015,month=5,day=25),
             Holiday('Pfingstmontag 2016', year=2016,month=5,day=16),
             Holiday('Pfingstmontag 2017', year=2017,month=6,day=5)]

cal = GermanHolidays()

def dataset_forecast():


    os.chdir('/Users/benni/Desktop/Uni/Paper')
    #reading in input files
    #netload 50 hz control zone

    netload15 = pd.read_csv('Netzlast/NetzlastENTSOE50Hz2015.csv')
    netload16 = pd.read_csv('Netzlast/NetzlastENTSOE50Hz2016.csv')
    netload17 = pd.read_csv('Netzlast/NetzlastENTSOE50Hz2017.csv')

    frame = [netload15,netload16,netload17] #framing of the 3 years input data
    netload = pd.concat(frame, sort = True) #concatenating of the 3 dataframes into one dataframe
    netload['Time (CET)'] = netload['Time (CET)'].str.split('-').str[0] #string operation on the Time (CET) column to ease datetime formatting
    netload['Time (CET)'] = pd.to_datetime(netload['Time (CET)'], dayfirst = True) #datetime formatting

    netloadactual = netload[['Time (CET)','Actual Total Load [MW] - CTA|DE(50Hertz)']]

    del netload['Actual Total Load [MW] - CTA|DE(50Hertz)'] #delete actual load
    netload = netload[netload['Day-ahead Total Load Forecast [MW] - CTA|DE(50Hertz)']!='-'] #delete all non value lines
    mask = netload['Time (CET)'] == netload['Time (CET)'].dt.floor('H') #only show the discrete values for every full hour
    netload = netload[mask] #apply mask
    netload = netload.reset_index(drop = True)
    print('# Loading and transforming netload data finished')

        #netload Germany

    netload15de = pd.read_csv('NetzlastDeutschland/NetzlastDE2015.csv')
    netload16de = pd.read_csv('NetzlastDeutschland/NetzlastDE2016.csv')
    netload17de = pd.read_csv('NetzlastDeutschland/NetzlastDe2017.csv')

    frame = [netload15de,netload16de,netload17de]

    netloadde = pd.concat(frame, sort = True)

    netloadde['Time (CET)'] = netloadde['Time (CET)'].str.split('-').str[0]
    netloadde['Time (CET)'] = pd.to_datetime(netloadde['Time (CET)'],dayfirst = True)
    netloaddeactual = netloadde[['Time (CET)','Actual Total Load [MW] - Germany (DE)']]
    del netloadde['Actual Total Load [MW] - Germany (DE)']
    netloadde = netloadde[netloadde['Day-ahead Total Load Forecast [MW] - Germany (DE)']!='-']
    mask = netloadde['Time (CET)'] == netloadde['Time (CET)'].dt.floor('H')
    netloadde = netloadde[mask]
    netloadde = netloadde.reset_index(drop=True)
    netloadde = netloadde.set_index('Time (CET)', drop = True)
    print('#1 Loading and transforming netload data for Germany finished')
    #net input 50 hz control zone (Netzeinspeisung)

    print('Netload Germany finished ######')

    netinput15 = pd.read_csv('Netzeinspeisung/Netzeinspeisung_2015.csv', sep = None, skiprows = [0,1,2,3], engine = 'python') #no seperator, first 4 rows dont have important information
    netinput16 = pd.read_csv('Netzeinspeisung/Netzeinspeisung_2016.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')
    netinput17 = pd.read_csv('Netzeinspeisung/Netzeinspeisung_2017.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')
    frame = [netinput15,netinput16, netinput17]
    netinput = pd.concat(frame)
    netinput['Datum'] = pd.to_datetime(netinput['Datum'] + ' ' + netinput['Von'], dayfirst = True, errors = 'coerce') #datetime formatting
    del netinput['Von']
    del netinput['bis']
    del netinput['Unnamed: 4']
    mask = netinput['Datum'] == netinput['Datum'].dt.floor('H') #only show the discrete values for every full hour
    netinput = netinput[mask] #apply mask
    netinput = netinput.reset_index(drop = True)
    netinput = netinput.set_index('Datum', drop = True)
    netinput.rename(columns = {'MW':'Netinput_in_MW'}, inplace = True)
    print('##Loading and transforming netinput data finished')
    #solar energy

    solar15 = pd.read_csv('Solarenergie/Solarenergie_Prognose_2015.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')
    solar16 = pd.read_csv('Solarenergie/Solarenergie_Prognose_2016.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')
    solar17 = pd.read_csv('Solarenergie/Solarenergie_Prognose_2017.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')

    frame = [solar15,solar16,solar17]
    solar = pd.concat(frame, sort = True)

    solar['Datum'] = pd.to_datetime(solar['Datum'] + ' '+ solar['Von'], errors = 'coerce', dayfirst = True)
    mask = solar['Datum'] == solar['Datum'].dt.floor('H')
    solar = solar[mask]
    solar.rename(columns = {'MW': 'Solar_in_MW'}, inplace = True)
    del solar['Von']
    del solar['bis']
    del solar['Unnamed: 4']
    solar = solar.reset_index(drop = True)
    solar = solar.set_index('Datum', drop = True)
    print('#### Loading and transforming solar data finished')

    #wind energy

    wind15 = pd.read_csv('Windenergie/Windenergie_Prognose_2015.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')
    wind16 = pd.read_csv('Windenergie/Windenergie_Prognose_2016.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')
    wind17 = pd.read_csv('Windenergie/Windenergie_Prognose_2017.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')

    frame = [wind15,wind16,wind17]
    wind = pd.concat(frame, sort = True)

    wind['Datum'] = pd.to_datetime(wind['Datum'] + ' ' + wind['Von'], errors = 'coerce', dayfirst = True)
    mask = wind['Datum'] == wind['Datum'].dt.floor('H')
    wind = wind[mask]
    del wind['Von']
    del wind['bis']
    del wind['Unnamed: 6']
    del wind['Offshore MW']
    del wind['Onshore MW']
    wind.rename(columns = {'MW': 'Wind_in_MW'}, inplace = True)
    wind = wind.reset_index(drop=True) #reindexing
    wind = wind.set_index('Datum', drop = True)
    print('###### Loading and transforming wind data finished')

    #import,exports boarders

    iedecz15 = pd.read_csv('ImportExportDaten/ScheduledExchanges2015CZDE.csv')
    iedecz16 = pd.read_csv('ImportExportDaten/ScheduledExchanges2016CZDE.csv')
    iedecz17 = pd.read_csv('ImportExportDaten/ScheduledExchanges2017CZDE.csv', usecols = ['Time (CET)',
           'CTA|CZ > CTA|DE(50Hertz) Total [MW]',
           'CTA|DE(50Hertz) > CTA|CZ Total [MW]'])
    iededk15 = pd.read_csv('ImportExportDaten/ScheduledExchanges2015DKDE.csv')
    iededk16 = pd.read_csv('ImportExportDaten/ScheduledExchanges2016DKDE.csv')
    iededk17 = pd.read_csv('ImportExportDaten/ScheduledExchanges2017DKDE.csv', usecols = ['Time (CET)',
           'CTA|DK > CTA|DE(50Hertz) Total [MW]',
           'CTA|DE(50Hertz) > CTA|DK Total [MW]'])
    iedepl15 = pd.read_csv('ImportExportDaten/ScheduledExchanges2015PLDE.csv')
    iedepl16 = pd.read_csv('ImportExportDaten/ScheduledExchanges2016PLDE.csv')
    iedepl17 = pd.read_csv('ImportExportDaten/ScheduledExchanges2017PLDE.csv', usecols = ['Time (CET)',
           'CTA|PL > CTA|DE(50Hertz) Total [MW]',
           'CTA|DE(50Hertz) > CTA|PL Total [MW]'])
    iededk17.rename(columns = {'Time (CET)':'Time (UTC)','CTA|DK > CTA|DE(50Hertz) Total [MW]':'CTA|DK > CTA|DE(50Hertz) [MW]',
                               'CTA|DE(50Hertz) > CTA|DK Total [MW]':'CTA|DE(50Hertz) > CTA|DK [MW]'}, inplace = True)
    iedecz17.rename(columns = {'Time (CET)':'Time (UTC)','CTA|CZ > CTA|DE(50Hertz) Total [MW]':'CTA|CZ > CTA|DE(50Hertz) [MW]',
                               'CTA|DE(50Hertz) > CTA|CZ Total [MW]':'CTA|DE(50Hertz) > CTA|CZ [MW]'}, inplace = True)
    iedepl17.rename(columns = {'Time (CET)':'Time (UTC)','CTA|PL > CTA|DE(50Hertz) Total [MW]':'CTA|PL > CTA|DE(50Hertz) [MW]',
                               'CTA|DE(50Hertz) > CTA|PL Total [MW]':'CTA|DE(50Hertz) > CTA|PL [MW]'}, inplace = True)
    framecz = [iedecz15,iedecz16,iedecz17]
    framedk = [iededk15,iededk16,iededk17]
    framepl = [iedepl15,iedepl16,iedepl17]

    impexp = pd.concat(framecz)
    impexpdk = pd.concat(framedk, sort = True)
    impexpdk = impexpdk.set_index('Time (UTC)')
    impexppl = pd.concat(framepl, sort = True)
    impexppl = impexppl.set_index('Time (UTC)')
    impexp = impexp.join(impexpdk, on = 'Time (UTC)', how ='inner')
    impexp = impexp.join(impexppl, on = 'Time (UTC)', how = 'inner')
    impexp = impexp[impexp['CTA|PL > CTA|DE(50Hertz) [MW]']!='-']
    impexp['Time (UTC)'] = impexp['Time (UTC)'].str.split('-').str[0]
    impexp['Time (UTC)'] = pd.to_datetime(impexp['Time (UTC)'], dayfirst = True)
    impexp['Time (UTC)'] = impexp['Time (UTC)'].apply(lambda x: datetime_from_utc_to_local(x))
    impexp.rename(columns = {'Time (UTC)' : 'Time (CET)'}, inplace = True)
    col_list = ['CTA|CZ > CTA|DE(50Hertz) [MW]', 'CTA|PL > CTA|DE(50Hertz) [MW]','CTA|DE(50Hertz) > CTA|CZ [MW]','CTA|DE(50Hertz) > CTA|PL [MW]','CTA|DE(50Hertz) > CTA|DK [MW]','CTA|DK > CTA|DE(50Hertz) [MW]']
    impexp[col_list] = impexp[col_list].astype(float) #dtype conversion
    impexp['CTA|DE(50Hertz) > CTA|CZ [MW]'] = impexp['CTA|DE(50Hertz) > CTA|CZ [MW]'] - impexp['CTA|CZ > CTA|DE(50Hertz) [MW]'] #creating one variable showing import (R-) or export (R+)
    del impexp['CTA|CZ > CTA|DE(50Hertz) [MW]']
    impexp['CTA|DE(50Hertz) > CTA|PL [MW]'] = impexp['CTA|DE(50Hertz) > CTA|PL [MW]'] - impexp['CTA|PL > CTA|DE(50Hertz) [MW]']
    del impexp['CTA|PL > CTA|DE(50Hertz) [MW]']
    impexp['CTA|DE(50Hertz) > CTA|DK [MW]'] = impexp['CTA|DE(50Hertz) > CTA|DK [MW]'] - impexp['CTA|DK > CTA|DE(50Hertz) [MW]']
    del impexp['CTA|DK > CTA|DE(50Hertz) [MW]']
    impexp = impexp.rename(columns = {'CTA|DE(50Hertz) > CTA|CZ [MW]':'Balance_ImpExp_DECZ','CTA|DE(50Hertz) > CTA|PL [MW]':'Balance_ImpExp_DEPL','CTA|DE(50Hertz) > CTA|DK [MW]':'Balance_ImpExp_DEDK'})#renaming of columns
    impexp = impexp.set_index('Time (CET)', drop = True)
    print('Loading and transforming impexp data finished')

    #generation data
    gen15 = pd.read_csv('GenerationDayAhead/Generation2015.csv')
    gen16= pd.read_csv('GenerationDayAhead/Generation2016.csv')
    gen17= pd.read_csv('GenerationDayAhead/Generation2017.csv')
    frame = [gen15,gen16,gen17]
    gen = pd.concat(frame)
    gen['MTU'] = gen['MTU'].str.split('-').str[0]
    gen['MTU'] = pd.to_datetime(gen['MTU'], dayfirst = True)
    gen= gen[gen['Scheduled Generation [MW] (D) - CTA|DE(50Hertz)']!='-']
    gen['Scheduled Generation [MW] (D) - CTA|DE(50Hertz)'] = gen['Scheduled Generation [MW] (D) - CTA|DE(50Hertz)'].astype(float)
    del gen['Scheduled Consumption [MW] (D) - CTA|DE(50Hertz)']
    gen = gen.set_index('MTU', drop = True)
    print('Loading and transforming generation data finished')



    prices = pd.DataFrame({'Time (CET)':pd.DatetimeIndex(start = '2016-01-01', end='2017-12-31 23:59',freq='H')})

    prices1617 = pd.DataFrame()
    files = glob.glob('/Users/benni/Desktop/Uni/Paper/Preise/*.csv')
    for f in sorted(files):
        data = pd.read_csv(f, sep=None,usecols=[4],skiprows=[0,1,2,4,5,6,7,8,9], engine = 'python')
        data = data.iloc[::-1]
        data = data.drop(data.index[[0,1,2,3,4]])
        prices1617 = prices1617.append(data)


    prices1617 = prices1617.reset_index()
    result = pd.concat([prices,prices1617],axis=1)
    del result['index']
    result['Time (CET)'] = pd.to_datetime(result['Time (CET)'])
    result['Price'] = result['Price'].astype(str)
    result['Price'] = [x.replace(',','.') for x in result['Price']]
    result['Price'] = result['Price'].astype(float)
    price15 = pd.read_csv('/Users/benni/Desktop/Uni/Paper/Preise2015/priceVolume_2015.csv', sep = None, usecols=[0,2],skiprows=[0], names = ['Price','Time (CET)'], engine = 'python')
    cols = price15.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    price15 = price15[cols]
    price15['Time (CET)'] = pd.to_datetime(price15['Time (CET)'])
    prices151617 = price15.append(result)
    prices151617 = prices151617.dropna()
    prices151617 = prices151617.reset_index()
    prices151617 = prices151617.set_index('Time (CET)',drop=True)

    print('Loading and transforming price data finished')

    #creating a concatenated dataset, merging is controlled by time variable
    X = netload
    X = X.join(netloadde, on = 'Time (CET)', how= 'inner')
    X = X.join(netinput, on = 'Time (CET)', how = 'inner')
    X = X.join(solar, on = 'Time (CET)', how = 'inner')
    X = X.join(wind, on = 'Time (CET)', how ='inner')
    X = X.join(impexp, on = 'Time (CET)', how = 'inner')
    X = X.join(gen, on='Time (CET)', how = 'inner')
    X = X.join(prices151617, on ='Time (CET)', how = 'inner')
    del X['Unnamed: 4']
    X['Price'] = rescaler(X['Price'])
    X['Balance_ImpExp_DECZ'] = rescaler(X['Balance_ImpExp_DECZ'])
    X['Balance_ImpExp_DEPL'] = rescaler(X['Balance_ImpExp_DEPL'])
    X['Balance_ImpExp_DEDK'] = rescaler(X['Balance_ImpExp_DEDK'])

    X['Scheduled Generation [MW] (D) - CTA|DE(50Hertz)'] = rescaler(X['Scheduled Generation [MW] (D) - CTA|DE(50Hertz)'])

    X['Wind_in_MW'] = X['Wind_in_MW'].astype(str)
    X['Wind_in_MW'] = [x.replace('.','') for x in X['Wind_in_MW']]
    X['Wind_in_MW'] = [x.replace(',','.') for x in X['Wind_in_MW']]
    X['Wind_in_MW'] = X['Wind_in_MW'].astype(float) #type conversion
    X['Wind_in_MW'] = rescaler(X['Wind_in_MW'])


    X['Solar_in_MW'] = X['Solar_in_MW'].astype(str)
    X['Solar_in_MW'] = [x.replace('.','') for x in X['Solar_in_MW']]
    X['Solar_in_MW'] = [x.replace(',','.') for x in X['Solar_in_MW']]
    X['Solar_in_MW'] = X['Solar_in_MW'].astype(float) #type conversion
    X['Solar_in_MW'] = rescaler(X['Solar_in_MW'])

    X['Netinput_in_MW'] = rescaler(X['Netinput_in_MW'])

    #X['VLoad_in_MW'] = X['VLoad_in_MW'].astype(str)
    #X['VLoad_in_MW'] = [x.replace('.','') for x in X['VLoad_in_MW']]
    #X['VLoad_in_MW'] = X['VLoad_in_MW'].astype(float)
    #X['VLoad_in_MW'] = rescaler(X['VLoad_in_MW'])

    X['Day-ahead Total Load Forecast [MW] - CTA|DE(50Hertz)'] = X['Day-ahead Total Load Forecast [MW] - CTA|DE(50Hertz)'].astype(float) #type conversion
    X['Day-ahead Total Load Forecast [MW] - Germany (DE)'] = X['Day-ahead Total Load Forecast [MW] - Germany (DE)'].astype(float) #type conversion
    X['Day-ahead Total Load Forecast [MW] - Germany (DE)'] = X['Day-ahead Total Load Forecast [MW] - Germany (DE)'] - X['Day-ahead Total Load Forecast [MW] - CTA|DE(50Hertz)'] #avoiding colinearity between netload of Germany and 50Hz control zone
    X['Day-ahead Total Load Forecast [MW] - Germany (DE)'] = rescaler(X['Day-ahead Total Load Forecast [MW] - Germany (DE)'])
    X['Day-ahead Total Load Forecast [MW] - CTA|DE(50Hertz)'] = rescaler(X['Day-ahead Total Load Forecast [MW] - CTA|DE(50Hertz)'])

    X['YSeason'] = X['Time (CET)']
    X['YSeason'] = X['YSeason'].apply(lambda x : get_season(x)) #season indexing of datetime
    X['YSeason'] = X['YSeason'].astype('category')
    X['BDay/WE'] = ((pd.DatetimeIndex(X['Time (CET)']).dayofweek) // 5 == 1).astype(float) #// integer division to check if weekday or not returning a True or false which is converted to 1 or 0(false)
    holidays = cal.holidays(start=X['Time (CET)'].min(), end = X['Time (CET)'].max())
    print(holidays)
    X['Time (CET)'] = pd.to_datetime(X['Time (CET)'])
    X = X.sort_values(by = 'Time (CET)')
    X['Time (CET)'] = X['Time (CET)'].dt.normalize()
    X = X.assign(Holidays = X['Time (CET)'].isin(holidays).astype(int))
    X['BDay/WE'] = X['BDay/WE'] + X['Holidays']
    del X['Holidays']
    X['BDay/WE'] = X['BDay/WE'].astype(str)
    X['BDay/WE'] = [x.replace('2','1') for x in X['BDay/WE']]
    X['BDay/WE'] = X['BDay/WE'].astype(float)
    del X['index']
    X.to_csv('/Users/benni/PycharmProjects/CongDataAnalysis/features.csv', index = False)

    return X
