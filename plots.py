import json
from matplotlib import pyplot as plt
import numpy as np



with open('temp_data.json','r') as input_file:
    data = json.load(input_file)

selected_countries = list(filter(lambda x: x[0] in ["Australia","Poland","China","Brazil"],data))
unique_countries = set()
for a in selected_countries:
    unique_countries.add(a[0]) 

#plots of temp
fig, axs = plt.subplots(2,2, sharex=True)

for ax,country in zip(axs.ravel(),unique_countries):
    year, temp = np.array(list(map(lambda x: int(x[1]),filter(lambda x: x[0] == country,data)))), np.array(list(map(lambda x: float(x[2]),filter(lambda x: x[0] == country,data))))
    ax.scatter(year,temp)
    m, b = np.polyfit(year, temp, 1)
    ax.plot(year, m*year + b,color='red')
    ax.set_ylabel('Temperature ($^\circ$C)')
    ax.set_title(country)
    
start, end = axs[0][0].get_xlim()
axs[0][0].xaxis.set_ticks(np.arange(int(start+2), int(end), 2))

#plots of deviation
fig1, axs1 = plt.subplots(2,2, sharex=True)

for ax,country in zip(axs1.ravel(),unique_countries):
    year, deviation = np.array(list(map(lambda x: int(x[1]),filter(lambda x: x[0] == country,data)))), np.array(list(map(lambda x: float(x[3]),filter(lambda x: x[0] == country,data))))
    ax.scatter(year,deviation)
    m, b = np.polyfit(year, deviation, 1)
    ax.plot(year, m*year + b,color='red')
    ax.set_ylabel('Standard deviation ($^\circ$C)')
    ax.set_title(country)
    
start, end = axs[0][0].get_xlim()
axs1[0][0].xaxis.set_ticks(np.arange(int(start+2), int(end), 2))

plt.show()