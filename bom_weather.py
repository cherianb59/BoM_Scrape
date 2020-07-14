#scrape all the observable weather stations from the NSW page, return a list of strings of the
import urllib2
from lxml import html
from lxml.html import parse
from lxml.etree import tostring
import pprint
import json
import StringIO
import gzip
import sqlite3
 
#if the returned page is gzipped then unzip otherwise just return the page
def unzip(opener):
    if opener.headers.get("content-encoding") = = "gzip":
        compresseddata = opener.read()                            
        #print(len(compresseddata))
 
 
        compressedstream = StringIO.StringIO(compresseddata) 
 
        gzipper = gzip.GzipFile(fileobj = compressedstream)    
        data = gzipper.read()                                
        return data
        #print(data)
    else:
        data = opener.read()
        return data
        #print(data)
 
def getpage(url_string):
     req = urllib2.Request(url_string,headers = header)
     page = urllib2.urlopen(req)
     return page
 
#this requests gzipped pags savs bandwidth, taken from a tutorial
def access_bom(url):
    #roxy = urllib2.ProxyHandler({'http': 'http://webproxy.production.local:8080'})
    #uth = urllib2.HTTPBasicAuthHandler()
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(url)
    request.add_header('Accept-encoding', 'gzip')
    requested_page = opener.open(request)
    return requested_page    
     
#headers needed for some sites    
header = {'User-Agent': 'Mozilla/5.0'}
 
#where to store
conn = sqlite3.connect("./weather.db")
c = conn.cursor()
 
state_url = ["http://www.bom.gov.au/nsw/observations/nswall.shtml","http://www.bom.gov.au/vic/observations/vicall.shtml","http://www.bom.gov.au/sa/observations/saall.shtml","http://www.bom.gov.au/wa/observations/waall.shtml","http://www.bom.gov.au/nt/observations/ntall.shtml","http://www.bom.gov.au/qld/observations/qldall.shtml","http://www.bom.gov.au/tas/observations/tasall.shtml"]
urls = []
bom_url = "http://www.bom.gov.au"
 
#for each state url read all the weather stations by scraping
for url in state_url:
    page = getpage(url)
 
    tree = html.fromstring(page.read())
 
    # look at all tables with a certain class, within these tables get the tr element, then get teh th element in the tr element
    station_tables = tree.findall('.//table[@class = "tabledata obs_table"]//tbody//tr//th//a')
    #make a list of all the
    for row in station_tables:
        urls.append(row.attrib["href"].replace("products","fwo").replace("shtml","json"))
 
 
#download page, unzip it, read it into json, create table based on "name",
 
for url in urls:
    page = access_bom(bom_url+url)
    json_data = json.loads(unzip(page))
    c.execute("CREATE TABLE IF NOT EXISTS AUS_WEATHER    (sort_order int,wmo int,name text,history_product text,local_date_time text,local_date_time_full text,aifstime_utc text,air_temp real,apparent_temp int,cloud text,cloud_base_m text,cloud_oktas text,cloud_type text,cloud_type_id text,delta_t real,dewpt real,gust_kmh real,gust_kt real,lat real,lon real,press real,press_msl real,press_qnh real,press_tend text,rain_trace real,rel_hum real,sea_state text,swell_dir_worded text,swell_height text,swell_period text,vis_km real,weather text,wind_dir text,wind_spd_kmh real,wind_spd_kt real,compositekey text unique)")
    #transform the dictionary into a list to insert into database
    insert = []
    data = json_data["observations"]["data"]
   
    for i in range(len(data)):
          insert.append([])
        insert[i].append(data[i]["sort_order"])
        insert[i].append(data[i]["wmo"])
        insert[i].append(data[i]["name"])
        insert[i].append(data[i]["history_product"])
        insert[i].append(data[i]["local_date_time"])
        insert[i].append(data[i]["local_date_time_full"])
        insert[i].append(data[i]["aifstime_utc"])
        insert[i].append(data[i]["air_temp"])
        insert[i].append(data[i]["apparent_t"])
        insert[i].append(data[i]["cloud"])
        insert[i].append(data[i]["cloud_base_m"])
        insert[i].append(data[i]["cloud_oktas"])
        insert[i].append(data[i]["cloud_type"])
        insert[i].append(data[i]["cloud_type_id"])
        insert[i].append(data[i]["delta_t"])
        insert[i].append(data[i]["dewpt"])
        insert[i].append(data[i]["gust_kmh"])
        insert[i].append(data[i]["gust_kt"])
        insert[i].append(data[i]["lat"])
        insert[i].append(data[i]["lon"])
        insert[i].append(data[i]["press"])
        insert[i].append(data[i]["press_msl"])
        insert[i].append(data[i]["press_qnh"])
        insert[i].append(data[i]["press_tend"])
        insert[i].append(data[i]["rain_trace"])
        insert[i].append(data[i]["rel_hum"])
        insert[i].append(data[i]["sea_state"])
        insert[i].append(data[i]["swell_dir_worded"])
        insert[i].append(data[i]["swell_height"])
        insert[i].append(data[i]["swell_period"])
        insert[i].append(data[i]["vis_km"])
        insert[i].append(data[i]["weather"])
        insert[i].append(data[i]["wind_dir"])
        insert[i].append(data[i]["wind_spd_kmh"])
        insert[i].append(data[i]["wind_spd_kt"])
        insert[i].append(str(data[i]["wmo"])+str(data[i]["aifstime_utc"])+data[i]["history_product"])
#        print data[i]["wmo"]
#    print insert[1][2]
    c.executemany("INSERT OR IGNORE INTO AUS_WEATHER VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", insert)
    conn.commit()
 
 
conn.close()
