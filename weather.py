import datetime
import json
import array
d=datetime.datetime.today()
date=str(d.strftime("%Y-%m-%d"))

weatherdata=['../canberra/IDN60903.94925.json','../centralcoast/IDN60901.94782.json']
weathergraph=['../canberra/'+date+'-canberra.csv','../centralcoast/'+date+'-centralcoast.csv']

for i in range(len(weatherdata)):
	json_data=open(weatherdata[i])
	data=json.load(json_data)
		
	f=open(weathergraph[i],'w')

#	index=2*int(d.strftime("%H"))
	index=46
	while (index >= 0):
		time=str(data["observations"]["data"][index]["local_date_time_full"])
		air_temp=str(data["observations"]["data"][index]["air_temp"])
		apparent_temp=str(data["observations"]["data"][index]["apparent_t"]) 
		output=time+' '+air_temp+' '+apparent_temp

		f.write(output+'\n')
		index=index-1
	json_data.close()
