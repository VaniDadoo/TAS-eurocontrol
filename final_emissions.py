from tracemalloc import start
import pandas as pd
#import gc 
import time

start_time = time.time()


#Read data
parameters = ['AC Type', 'Actual Distance Flown (nm)']
parquet_para = ['CO2', 'Ac Operator'] #Add other emissions later

# ---- MARCH -----
month1 = 'march'
month1num = '03'

#Distance
m1 = pd.read_csv(f"eurocontrol-data/2015/2015{month1num}/Flights_2015{month1num}01_2015{month1num}31.csv")
m2 = pd.read_csv(f"eurocontrol-data/2016/2016{month1num}/Flights_2016{month1num}01_2016{month1num}31.csv")
m3 = pd.read_csv(f"eurocontrol-data/2017/2017{month1num}/Flights_2017{month1num}01_2017{month1num}31.csv")
m4 = pd.read_csv(f"eurocontrol-data/2018/2018{month1num}/Flights_2018{month1num}01_2018{month1num}31.csv")
m5 = pd.read_csv(f"eurocontrol-data/2019/2019{month1num}/Flights_2019{month1num}01_2019{month1num}31.csv")


#Emissions
m1e = pd.read_parquet(f"reduced-parquet/2015-{month1}-red.parquet")
m2e = pd.read_parquet(f"reduced-parquet/2016-{month1}-red.parquet")
m3e = pd.read_parquet(f"reduced-parquet/2017-{month1}-red.parquet")
m4e = pd.read_parquet(f"reduced-parquet/2018-{month1}-red.parquet")
m5e = pd.read_parquet(f"reduced-parquet/2018-{month1}-red.parquet")

# --- March ---
m1dist = m1[parameters].groupby('AC Operator').sum()
m2dist = m2[parameters].groupby('AC Operator').sum()
m3dist = m3[parameters].groupby('AC Operator').sum()
m4dist = m4[parameters].groupby('AC Operator').sum()
m5dist = m5[parameters].groupby('AC Operator').sum()

# Total in all months
marchdist = pd.concat([m1dist, m2dist, m3dist, m4dist, m5dist])
marchdist.reset_index('AC Operator', inplace=True)
totalmarchdist = marchdist.groupby('AC Operator').sum().sort_values('Actual Distance Flown (nm)', ascending=False)
totalmarchdist['Actual Distance Flown (nm)'] = totalmarchdist['Actual Distance Flown (nm)']/5
totalmarchdist.sort_values('Actual Distance Flown (nm)', ascending=False)
samplemarchdist = totalmarchdist.head(30)

# ---- June -----
month1 = 'june'
month1num = '06'

#Distance
j1 = pd.read_csv(f"eurocontrol-data/2015/2015{month1num}/Flights_2015{month1num}01_2015{month1num}30.csv")
j2 = pd.read_csv(f"eurocontrol-data/2016/2016{month1num}/Flights_2016{month1num}01_2016{month1num}30.csv")
j3 = pd.read_csv(f"eurocontrol-data/2017/2017{month1num}/Flights_2017{month1num}01_2017{month1num}30.csv")
j4 = pd.read_csv(f"eurocontrol-data/2018/2018{month1num}/Flights_2018{month1num}01_2018{month1num}30.csv")
j5 = pd.read_csv(f"eurocontrol-data/2019/2019{month1num}/Flights_2019{month1num}01_2019{month1num}30.csv")

#Emissions
j1e = pd.read_parquet(f"reduced-parquet/2015-{month1}-red.parquet")
j2e = pd.read_parquet(f"reduced-parquet/2016-{month1}-red.parquet")
j3e = pd.read_parquet(f"reduced-parquet/2017-{month1}-red.parquet")
j4e = pd.read_parquet(f"reduced-parquet/2018-{month1}-red.parquet")
j5e = pd.read_parquet(f"reduced-parquet/2019-{month1}-red.parquet")

# --- June ---
j1dist = j1[parameters].groupby('AC Operator').sum()
j2dist = j2[parameters].groupby('AC Operator').sum()
j3dist = j3[parameters].groupby('AC Operator').sum()
j4dist = j4[parameters].groupby('AC Operator').sum()
j5dist = j5[parameters].groupby('AC Operator').sum()

# Total in all months
junedist = pd.concat([j1dist, j2dist, j3dist, j4dist, j5dist])
junedist.reset_index('AC Operator', inplace=True)
totaljunedist = junedist.groupby('AC Operator').sum().sort_values('Actual Distance Flown (nm)', ascending=False)
totaljunedist['Actual Distance Flown (nm)'] = totaljunedist['Actual Distance Flown (nm)']/5
totaljunedist.sort_values('Actual Distance Flown (nm)', ascending=False)
samplejunedist = totaljunedist.head(30)

# ---- SEPTEMBER -----
month1 = 'september'
month1num = '09'

#Distance
s1 = pd.read_csv(f"eurocontrol-data/2015/2015{month1num}/Flights_2015{month1num}01_2015{month1num}30.csv")
s2 = pd.read_csv(f"eurocontrol-data/2016/2016{month1num}/Flights_2016{month1num}01_2016{month1num}30.csv")
s3 = pd.read_csv(f"eurocontrol-data/2017/2017{month1num}/Flights_2017{month1num}01_2017{month1num}30.csv")
s4 = pd.read_csv(f"eurocontrol-data/2018/2018{month1num}/Flights_2018{month1num}01_2018{month1num}30.csv")
s5 = pd.read_csv(f"eurocontrol-data/2019/2019{month1num}/Flights_2019{month1num}01_2019{month1num}30.csv")

#Emissions
s1e = pd.read_parquet(f"reduced-parquet/2015-{month1}-red.parquet")
s2e = pd.read_parquet(f"reduced-parquet/2016-{month1}-red.parquet")
s3e = pd.read_parquet(f"reduced-parquet/2017-{month1}-red.parquet")
s4e = pd.read_parquet(f"reduced-parquet/2018-{month1}-red.parquet")
s5e = pd.read_parquet(f"reduced-parquet/2019-{month1}-red.parquet")

# --- September ---
s1dist = s1[parameters].groupby('AC Operator').sum()
s2dist = s2[parameters].groupby('AC Operator').sum()
s3dist = s3[parameters].groupby('AC Operator').sum()
s4dist = s4[parameters].groupby('AC Operator').sum()
s5dist = s5[parameters].groupby('AC Operator').sum()

# Total in all months
septdist = pd.concat([s1dist, s2dist, s3dist, s4dist, s5dist])
septdist.reset_index('AC Operator', inplace=True)
totalseptdist = septdist.groupby('AC Operator').sum().sort_values('Actual Distance Flown (nm)', ascending=False)
totalseptdist['Actual Distance Flown (nm)'] = totalseptdist['Actual Distance Flown (nm)']/5
totalseptdist.sort_values('Actual Distance Flown (nm)', ascending=False)
sampleseptdist = totalseptdist.head(30)

# ---- DECEMBER -----
month1 = 'december'
month1num = '12'

#Distance
d1 = pd.read_csv(f"eurocontrol-data/2015/2015{month1num}/Flights_2015{month1num}01_2015{month1num}31.csv")
d2 = pd.read_csv(f"eurocontrol-data/2016/2016{month1num}/Flights_2016{month1num}01_2016{month1num}31.csv")
d3 = pd.read_csv(f"eurocontrol-data/2017/2017{month1num}/Flights_2017{month1num}01_2017{month1num}31.csv")
d4 = pd.read_csv(f"eurocontrol-data/2018/2018{month1num}/Flights_2018{month1num}01_2018{month1num}31.csv")
d5 = pd.read_csv(f"eurocontrol-data/2019/2019{month1num}/Flights_2019{month1num}01_2019{month1num}31.csv")

#Emissions
d1e = pd.read_parquet(f"reduced-parquet/2015-{month1}-red.parquet")
d2e = pd.read_parquet(f"reduced-parquet/2016-{month1}-red.parquet")
d3e = pd.read_parquet(f"reduced-parquet/2017-{month1}-red.parquet")
d4e = pd.read_parquet(f"reduced-parquet/2018-{month1}-red.parquet")
d5e = pd.read_parquet(f"reduced-parquet/2019-{month1}-red.parquet")

# --- December ---
d1dist = d1[parameters].groupby('AC Operator').sum()
d2dist = d2[parameters].groupby('AC Operator').sum()
d3dist = d3[parameters].groupby('AC Operator').sum()
d4dist = d4[parameters].groupby('AC Operator').sum()
d5dist = d5[parameters].groupby('AC Operator').sum()

# Total in all months
decdist = pd.concat([d1dist, d2dist, d3dist, d4dist, d5dist])
decdist.reset_index('AC Operator', inplace=True)
totaldecdist = septdist.groupby('AC Operator').sum().sort_values('Actual Distance Flown (nm)', ascending=False)
totaldecdist['Actual Distance Flown (nm)'] = totaldecdist['Actual Distance Flown (nm)']/5
totaldecdist.sort_values('Actual Distance Flown (nm)', ascending=False)
sampledecdist = totaldecdist.head(30)

# --- Emissions ---

# --- March ---
m1emis = m1e[parquet_para].groupby('Ac Operator').sum()
m2emis = m2e[parquet_para].groupby('Ac Operator').sum()
m3emis = m3e[parquet_para].groupby('Ac Operator').sum()
m4emis = m4e[parquet_para].groupby('Ac Operator').sum()
m5emis = m5e[parquet_para].groupby('Ac Operator').sum()

marchemis = pd.concat([m1emis, m2emis, m3emis, m4emis, m5emis])
marchemis.reset_index('Ac Operator', inplace=True)
marchemis.rename(columns = {'Ac Operator':'AC Operator'}, inplace = True)
totalmarchemis= marchemis.groupby('AC Operator').sum().sort_values('CO2', ascending=False)
totalmarchemis['CO2'] = totalmarchemis['CO2']/1000000 # get million kg
totalmarchemis['CO2'] = totalmarchemis['CO2']/5 # get average
samplemarchemis = totalmarchemis.head(40)

# --- June ---
j1emis = j1e[parquet_para].groupby('Ac Operator').sum()
j2emis = j2e[parquet_para].groupby('Ac Operator').sum()
j3emis = j3e[parquet_para].groupby('Ac Operator').sum()
j4emis = j4e[parquet_para].groupby('Ac Operator').sum()
j5emis = j5e[parquet_para].groupby('Ac Operator').sum()

juneemis = pd.concat([j1emis, j2emis, j3emis, j4emis, j5emis])
juneemis.reset_index('Ac Operator', inplace=True)
juneemis.rename(columns = {'Ac Operator':'AC Operator'}, inplace = True)
totaljuneemis= juneemis.groupby('AC Operator').sum().sort_values('CO2', ascending=False)
totaljuneemis['CO2'] = totaljuneemis['CO2']/1000000 # get million kg
totaljuneemis['CO2'] = totaljuneemis['CO2']/5 # get average
samplejuneemis = totaljuneemis.head(40)


# --- September ---
s1emis = s1e[parquet_para].groupby('Ac Operator').sum()
s2emis = s2e[parquet_para].groupby('Ac Operator').sum()
s3emis = s3e[parquet_para].groupby('Ac Operator').sum()
s4emis = s4e[parquet_para].groupby('Ac Operator').sum()
s5emis = s5e[parquet_para].groupby('Ac Operator').sum()

septemis = pd.concat([s1emis, s2emis, s3emis, s4emis, s5emis])
septemis.reset_index('Ac Operator', inplace=True)
septemis.rename(columns = {'Ac Operator':'AC Operator'}, inplace = True)
totalseptemis= septemis.groupby('AC Operator').sum().sort_values('CO2', ascending=False)
totalseptemis['CO2'] = totalseptemis['CO2']/1000000 # get million kg
totalseptemis['CO2'] = totalseptemis['CO2']/5 # get average
sampleseptemis = totalseptemis.head(40)

# --- December ---
d1emis = d1e[parquet_para].groupby('Ac Operator').sum()
d2emis = d2e[parquet_para].groupby('Ac Operator').sum()
d3emis = d3e[parquet_para].groupby('Ac Operator').sum()
d4emis = d4e[parquet_para].groupby('Ac Operator').sum()
d5emis = d5e[parquet_para].groupby('Ac Operator').sum()

decemis = pd.concat([d1emis, d2emis, d3emis, d4emis, d5emis])
decemis.reset_index('Ac Operator', inplace=True)
decemis.rename(columns = {'Ac Operator':'AC Operator'}, inplace = True)
totaldecemis= decemis.groupby('AC Operator').sum().sort_values('CO2', ascending=False)
totaldecemis['CO2'] = totaldecemis['CO2']/1000000 # get million kg
totaldecemis['CO2'] = totaldecemis['CO2']/5 # get average
sampledecemis = totaldecemis.head(40)

#Get average emissions
#--March--
final = pd.merge(samplemarchdist, samplemarchemis, on="AC Operator", how="left")
final.rename(columns = {'Actual Distance Flown (nm)':'Distance'}, inplace = True)
final['avg emission'] = final['CO2']*1000000/final['Distance']
final.sort_values('Distance', ascending=False)
final['Month'] = 'March'
final2 = final.head(15)

#--September--
septfinal = pd.merge(sampleseptdist, sampleseptemis, on="AC Operator", how="left")
septfinal.rename(columns = {'Actual Distance Flown (nm)':'Distance'}, inplace = True)
septfinal['avg emission'] = septfinal['CO2']*1000000/septfinal['Distance']
septfinal.sort_values('Distance', ascending=False)
septfinal['Month'] = 'Sept'
septfinal2 = septfinal.head(15)

#--June--
junefinal = pd.merge(samplejunedist, samplejuneemis, on="AC Operator", how="left")
junefinal.rename(columns = {'Actual Distance Flown (nm)':'Distance'}, inplace = True)
junefinal['avg emission'] = junefinal['CO2']*1000000/junefinal['Distance']
junefinal.sort_values('Distance', ascending=False)
junefinal['Month'] = 'June'
junefinal2 = junefinal.head(15)

#--December--
decfinal = pd.merge(sampledecdist, sampledecemis, on="AC Operator", how="left")
decfinal.rename(columns = {'Actual Distance Flown (nm)':'Distance'}, inplace = True)
decfinal['avg emission'] = decfinal['CO2']*1000000/decfinal['Distance']
decfinal.sort_values('Distance', ascending=False)
decfinal['Month'] = 'Dec'
decfinal2 = decfinal.head(15)

#sample = pd.merge(final2, septfinal2, on="AC Operator", how='left', suffixes=('_m', '_s'))
sample=pd.concat([final2, junefinal2, septfinal2], axis=0)
sample.reset_index('AC Operator', inplace=True)
print(sample)

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set_theme()
fig, axes = plt.subplots(3, 1, sharex=True, figsize=(12, 8))
axes[0].set_title('Total Distance Flown (nm)')
axes[1].set_title('Total CO2 emissions (million kg)')
axes[2].set_title('Average CO2 emissions per nm (kg / nm)')

f1 = sns.lineplot(data=sample, x='AC Operator', y='Distance', hue='Month', ax=axes[0], marker='o')
f1.set(xlabel = None, ylabel=None)

f2 = sns.barplot(data=sample, x='AC Operator', y='CO2', hue='Month', ax=axes[1])
f2.set(xlabel=None, ylabel=None)
f2.legend_.remove()

f3 = sns.barplot(data=sample, x='AC Operator', y='avg emission', hue='Month', ax=axes[2])
f3.set(xlabel=None, ylabel=None)
f3.legend_.remove()

plt.savefig('plot2-new.png')
plt.show()

print("time:", time.time()-start_time)