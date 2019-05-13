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

    #netload actual 50 Hz
    netloadactual = netloadactual[netloadactual['Actual Total Load [MW] - CTA|DE(50Hertz)']!='-']
    mask = netloadactual['Time (CET)'] == netloadactual['Time (CET)'].dt.floor('H')
    netloadactual = netloadactual[mask]
    netloadactual = netloadactual.reset_index(drop = True)
    netloadactual = netloadactual.set_index('Time (CET)',drop = True)
    netloadactual = netloadactual.astype(float)

    netloadactual['Actual Total Load [MW] - CTA|DE(50Hertz)'] = rescaler(netloadactual['Actual Total Load [MW] - CTA|DE(50Hertz)'])
    netloadautoreg = netloadactual['Actual Total Load [MW] - CTA|DE(50Hertz)'].shift(periods = 1,freq='d')
    netloadautoreg = netloadautoreg.rename(columns = {'Actual Total Load [MW] - CTA|DE(50Hertz)':'Autoreg_Netload'})
    netloadautoreg = netloadautoreg.to_frame(name = 'Autoreg_Netload')
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


    netloaddeactual = netloaddeactual[netloaddeactual['Actual Total Load [MW] - Germany (DE)']!='-']
    mask = netloaddeactual['Time (CET)'] == netloaddeactual['Time (CET)'].dt.floor('H')
    netloaddeactual = netloaddeactual[mask]
    netloaddeactual = netloaddeactual.reset_index(drop = True)
    netloaddeactual = netloaddeactual.set_index('Time (CET)',drop = True)
    netloaddeactual = netloaddeactual.astype(float)

    netloaddeactual['Actual Total Load [MW] - Germany (DE)'] = rescaler(netloaddeactual['Actual Total Load [MW] - Germany (DE)'])

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

    netinputactual15 = pd.read_csv('Netzeinspeisungrealisiert/Netzeinspeisung_2015.csv', sep = None, skiprows = [0,1,2,3], engine = 'python') #no seperator, first 4 rows dont have important information
    netinputactual16 = pd.read_csv('Netzeinspeisungrealisiert/Netzeinspeisung_2016.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')
    netinputactual17 = pd.read_csv('Netzeinspeisungrealisiert/Netzeinspeisung_2017.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')
    frame = [netinputactual15,netinputactual16, netinputactual17]
    netinputactual = pd.concat(frame)
    netinputactual['Datum'] = pd.to_datetime(netinputactual['Datum'] + ' ' + netinputactual['Von'], dayfirst = True, errors = 'coerce') #datetime formatting
    del netinputactual['Von']
    del netinputactual['bis']
    del netinputactual['Unnamed: 4']
    mask = netinputactual['Datum'] == netinputactual['Datum'].dt.floor('H') #only show the discrete values for every full hour
    netinputactual = netinputactual[mask] #apply mask
    netinputactual = netinputactual.reset_index(drop = True)
    netinputactual = netinputactual.set_index('Datum', drop = True)
    netinputactual['MW'] = rescaler(netinputactual['MW'])
    netinputactual.rename(columns = {'MW':'Netinput_realisiert_in_MW'}, inplace = True)
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

    solaractual15 = pd.read_csv('Solarenergierealisiert/Solarenergie_Hochrechnung_2015.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')
    solaractual16 = pd.read_csv('Solarenergierealisiert/Solarenergie_Hochrechnung_2016.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')
    solaractual17 = pd.read_csv('Solarenergierealisiert/Solarenergie_Hochrechnung_2017.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')

    frame = [solaractual15, solaractual16, solaractual17]
    solaractual = pd.concat(frame, sort = True)
    solaractual['Datum'] = pd.to_datetime(solaractual['Datum'] + ' '+ solaractual['Von'], errors = 'coerce', dayfirst = True)
    mask = solaractual['Datum'] == solaractual['Datum'].dt.floor('H')
    solaractual = solaractual[mask]
    solaractual.rename(columns = {'MW':'Solaractual_in_MW'}, inplace = True)
    solaractual = solaractual.reset_index(drop = True)
    solaractual = solaractual.set_index('Datum', drop = True)
    solaractual['Solaractual_in_MW'] = solaractual['Solaractual_in_MW'].astype(str)
    solaractual['Solaractual_in_MW'] = [x.replace('.','') for x in solaractual['Solaractual_in_MW']]
    solaractual['Solaractual_in_MW'] = [x.replace(',','.') for x in solaractual['Solaractual_in_MW']]
    solaractual['Solaractual_in_MW'] = solaractual['Solaractual_in_MW'].astype(float) #type conversion
    solaractual['Solaractual_in_MW'] = rescaler(solaractual['Solaractual_in_MW'])
    del solaractual['Von']
    del solaractual['bis']
    del solaractual['Unnamed: 4']
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

    #realized wind input
    windactual15 = pd.read_csv('Windrealisiert/Windenergie_Hochrechnung_2015.csv', sep = None, skiprows=[0,1,2,3], engine = 'python')
    windacutal16 = pd.read_csv('Windrealisiert/Windenergie_Hochrechnung_2016.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')
    windactual17 = pd.read_csv('Windrealisiert/Windenergie_Hochrechnung_2017.csv', sep = None, skiprows = [0,1,2,3], engine = 'python')

    frame = [windactual15,windacutal16, windactual17]
    windactual = pd.concat(frame, sort = True)
    windactual['Datum'] = pd.to_datetime(windactual['Datum']+ ' '+ windactual['Von'], errors = 'coerce', dayfirst = True)
    mask = windactual['Datum'] == windactual['Datum'].dt.floor('H')
    del windactual['Von']
    del windactual['bis']
    del windactual['Offshore MW']
    del windactual['Onshore MW']
    del windactual['Unnamed: 4']
    del windactual['Unnamed: 6']
    windactual.rename(columns = {'MW':'Windactual_in_MW'}, inplace = True)
    windactual = windactual.reset_index(drop = True)
    windactual = windactual.set_index('Datum', drop = True)
    windactual['Windactual_in_MW'] = windactual['Windactual_in_MW'].astype(str)
    windactual['Windactual_in_MW'] = [x.replace('.','') for x in windactual['Windactual_in_MW']]
    windactual['Windactual_in_MW'] = [x.replace(',','.') for x in windactual['Windactual_in_MW']]
    windactual['Windactual_in_MW'] = windactual['Windactual_in_MW'].astype(float) #type conversion
    windactual['Windactual_in_MW'] = rescaler(windactual['Windactual_in_MW'])
    windautoreg = windactual.shift(periods = 1,freq='d')
    windautoreg = windautoreg.rename(columns = {'Windactual_in_MW':'Autoreg_Wind_MW'})

    #impexp real
    cbfdecz15 = pd.read_csv('RealizedCrossBoarderFlows/Cross-Border Physical FlowDECZ2015.csv')
    cbfdecz16 = pd.read_csv('RealizedCrossBoarderFlows/Cross-Border Physical FlowDECZ2016.csv')
    cbfdecz17 = pd.read_csv('RealizedCrossBoarderFlows/Cross-Border Physical FlowDECZ2017.csv')
    cbfdedk15 = pd.read_csv('RealizedCrossBoarderFlows/Cross-Border Physical FlowDEDK2015.csv')
    cbfdedk16 = pd.read_csv('RealizedCrossBoarderFlows/Cross-Border Physical FlowDEDK2016.csv')
    cbfdedk17 = pd.read_csv('RealizedCrossBoarderFlows/Cross-Border Physical FlowDEDK2017.csv')
    cbfdepl15 = pd.read_csv('RealizedCrossBoarderFlows/Cross-Border Physical FlowDEPL2015.csv')
    cbfdepl16 = pd.read_csv('RealizedCrossBoarderFlows/Cross-Border Physical FlowDEPL2016.csv')
    cbfdepl17 = pd.read_csv('RealizedCrossBoarderFlows/Cross-Border Physical FlowDEPL2017.csv')

    framecz = [cbfdecz15,cbfdecz16,cbfdecz17]
    framedk = [cbfdedk15,cbfdedk16,cbfdedk17]
    framepl = [cbfdepl15,cbfdepl16,cbfdepl17]

    cbf = pd.concat(framecz, sort = True)
    print(cbf.columns)
    cbfdk = pd.concat(framedk, sort = True)
    print(cbfdk.columns)
    cbfdk = cbfdk.set_index('Time (CET)')
    cbfpl = pd.concat(framepl, sort = True)
    print(cbfpl.columns)
    cbfpl = cbfpl.set_index('Time (CET)')

    cbf = cbf.join(cbfdk, on = 'Time (CET)', how = 'inner')
    cbf = cbf.join(cbfpl, on = 'Time (CET)', how = 'inner')
    cbf['Time (CET)'] = cbf['Time (CET)'].str.split('-').str[0]
    cbf['Time (CET)'] = pd.to_datetime(cbf['Time (CET)'], dayfirst = True)
    col_list = ['CTA|CZ > CTA|DE(50Hertz) [MW]', 'CTA|PL > CTA|DE(50Hertz) [MW]','CTA|DE(50Hertz) > CTA|CZ [MW]','CTA|DE(50Hertz) > CTA|PL [MW]','CTA|DE(50Hertz) > CTA|DK [MW]','CTA|DK > CTA|DE(50Hertz) [MW]']
    cbf[col_list] = cbf[col_list].replace(['n/e','N/A'],['0','0'])
    cbf[col_list] = cbf[col_list].replace('-','0')
    cbf[col_list] = cbf[col_list].astype(float)
    cbf['CTA|DE(50Hertz) > CTA|CZ [MW]'] = cbf['CTA|DE(50Hertz) > CTA|CZ [MW]'] - cbf['CTA|CZ > CTA|DE(50Hertz) [MW]'] #creating one variable showing import (R-) or export (R+)
    del cbf['CTA|CZ > CTA|DE(50Hertz) [MW]']
    cbf['CTA|DE(50Hertz) > CTA|PL [MW]'] = cbf['CTA|DE(50Hertz) > CTA|PL [MW]'] - cbf['CTA|PL > CTA|DE(50Hertz) [MW]']
    del cbf['CTA|PL > CTA|DE(50Hertz) [MW]']
    cbf['CTA|DE(50Hertz) > CTA|DK [MW]'] = cbf['CTA|DE(50Hertz) > CTA|DK [MW]'] - cbf['CTA|DK > CTA|DE(50Hertz) [MW]']
    del cbf['CTA|DK > CTA|DE(50Hertz) [MW]']
    cbf = cbf.rename(columns = {'CTA|DE(50Hertz) > CTA|CZ [MW]':'Realized_ImpExp_DECZ','CTA|DE(50Hertz) > CTA|PL [MW]':'Realized_ImpExp_DEPL','CTA|DE(50Hertz) > CTA|DK [MW]':'Realized_ImpExp_DEDK'})#renaming of columns
    cbf = cbf.set_index('Time (CET)', drop = True)
    #cbf = cbf.fillna(0)
    cbfautoreg = cbf['Realized_ImpExp_DECZ']
    #del cbfautoreg['Realized_ImpExp_DEPL']
    #del cbfautoreg['Realized_ImpExp_DEDK']
    cbfautoreg = cbfautoreg.to_frame('Autoreg_ImpExp_DECZ')
    cbfautoreg = cbfautoreg.shift(periods =1, freq='d')
    cbfautoreg = rescaler(cbfautoreg['Autoreg_ImpExp_DECZ'])

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


    pricesreal1516 = pd.read_csv('Preiserealisiert/50Hertz_Großhandelspreise_20152016.csv', sep = None, engine='python',
                                 usecols = [0,1,15], encoding = 'utf-8', decimal = ',')
    pricesreal17 = pd.read_csv('Preiserealisiert/50Hertz_Großhandelspreise_2017.csv', sep = None, engine = 'python',
                               usecols = [0,1,15], encoding = 'utf-8', decimal = ',')
    frame = [pricesreal1516,pricesreal17]
    pricesreal = pd.concat(frame, sort = True)
    pricesreal.rename(columns = {'Deutschland/Österreich/Luxemburg[Euro/MWh]':
                                  'Preise_realisiert','\ufeffDatum':'Datum'}, inplace = True)
    pricesreal['Preise_realisiert'] = pricesreal['Preise_realisiert'].replace('-','NaN')
    pricesreal['Preise_realisiert'] = pricesreal['Preise_realisiert'].astype(float)
    pricesreal['Preise_realisiert'] = rescaler(pricesreal['Preise_realisiert'])
    pricesreal['Datum'] = pd.to_datetime(pricesreal['Datum'] + ' ' + pricesreal['Uhrzeit'], dayfirst = True)
    del pricesreal['Uhrzeit']
    pricesreal = pricesreal.set_index('Datum', drop = True)

    generationreal1516 = pd.read_csv('Generationrealisiert/50Hertz_Realisierte Erzeugung_20152016.csv',
                                decimal = ',', sep = None, engine = 'python')
    generationreal17 = pd.read_csv('Generationrealisiert/50Hertz_Realisierte_Erzeugung_2017.csv',
                                  decimal = ',', sep = None, engine = 'python')
    frame = [generationreal1516, generationreal17]
    generationreal = pd.concat(frame, sort = True)
    generationreal.rename(columns = {'\ufeffDatum':'Datum'},inplace = True)
    generationreal['Datum'] = pd.to_datetime(generationreal['Datum'] + ' ' + generationreal['Uhrzeit'],
                                            dayfirst = True)
    del generationreal['Uhrzeit']
    mask = generationreal['Datum'] == generationreal['Datum'].dt.floor('H')
    generationreal = generationreal[mask]
    generationreal = generationreal.set_index('Datum', drop = True)
    for column in generationreal.columns:
        generationreal[column] = generationreal[column].astype(str)
        generationreal[column] = [x.replace('.','') for x in generationreal[column]]
        generationreal[column] = [x.replace(',','.') for x in generationreal[column]]
        generationreal[column] = [x.replace('-','NaN') for x in generationreal[column]]
        generationreal[column] = generationreal[column].astype(float)

    generationreal['Total_Generation_MWh'] = generationreal.sum(axis = 1)
    generationreal = pd.DataFrame(generationreal['Total_Generation_MWh'])
    generationreal['Total_Generation_MWh'] = rescaler(generationreal['Total_Generation_MWh'])

    print('Loading and transforming price data finished')


    #Reading in Clustered Reference Day
    print(netload.tail(1))
    print(netloadde.tail(1))
    print(netinput.tail(1))
    print(solar.tail(1))
    print(wind.tail(1))
    print(impexp.tail(1))
    print(gen.tail(1))
    print(prices151617.tail(1))

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


    X = X.join(cbf, on = 'Time (CET)', how = 'inner')
    X = X.join(netloadactual, on = 'Time (CET)', how ='inner')
    X = X.join(windactual, on ='Time (CET)', how ='inner')
    X = X.join(netloadautoreg, on ='Time (CET)', how ='inner')
    X = X.join(windautoreg, on = 'Time (CET)', how ='inner')
    X = X.join(cbfautoreg, on = 'Time (CET)', how = 'inner')
    X = X.join(netloaddeactual, on = 'Time (CET)', how = 'inner')
    X = X.join(netinputactual, on = 'Time (CET)', how = 'inner')
    X = X.join(solaractual, on = 'Time (CET)', how = 'inner')
    X = X.join(generationreal, on = 'Time (CET)', how = 'inner')
    X = X.join(pricesreal, on = 'Time (CET)', how = 'inner')
    X['Realized_ImpExp_DECZ'] = rescaler(X['Realized_ImpExp_DECZ'])
    X['Realized_ImpExp_DEPL'] = rescaler(X['Realized_ImpExp_DEPL'])
    X['Realized_ImpExp_DEDK'] = rescaler(X['Realized_ImpExp_DEDK'])
    del X['index']


    return X
