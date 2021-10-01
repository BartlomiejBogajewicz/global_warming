Not everyone belive that climate change is real. I used pyspark and matplotlib to show how average temperature has changed
in past 20 years. I also added standard deviation to show how much temperature can vary between years. 
As data source I used dataset from kaggle (https://www.kaggle.com/subhamjain/temperature-of-all-countries-19952020)
which includes daily temperature measurements from diffrent countries.
I have processed that data in two different ways. First time I only used standard RDD approach and later on I managed 
to get the same results using pyspark DataFrame api. Additionaly I have connected to the postgresql to store the final results.

Below I placed the plots of temperature and standard deviation: 

![temp](https://github.com/BartlomiejBogajewicz/temperatures_analize/blob/master/Temp_charts.PNG)
![dev](https://github.com/BartlomiejBogajewicz/temperatures_analize/blob/master/Deviation_charts.PNG)

As you can see the temperature has risen in average by 1.5 degree. Other than that the standard deviation is changing which can
impact the temperature variability. The most obvious outcome of that will be the increased number of extreme heat waves in the summer.
