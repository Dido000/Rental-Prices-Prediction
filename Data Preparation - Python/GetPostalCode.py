#!/usr/bin/env python

#####################################################################
#                                                         			#
#  Use: Get Zip Code from Google Geocoding API                   	#
#      														    	#
#  Date: 11/13/2016 											    #
#																	#
#  Author: Lihao Liu <lihao@nyu.edu>								#
#																	#
#####################################################################

import sys
import string
import csv
import requests
from lxml import html
from time import gmtime, strftime
import urllib, json
import time

addresslist = []
zipcodelist = []

filename = "latlng2.csv"

f = open(filename)
addresslist = csv.reader(f)

#print 'Number of record in file:',len(addresslist)

googlemap = "https://maps.googleapis.com/maps/api/geocode/json?key=AIzaSyBRJoEcdgq0V_gRkJiWpVSpJUD4JodOc-Q&latlng="

for address in addresslist:
	
	mapurl = ''
	mapresult = []
	getzip = False
	mapurl = googlemap + address[1] + ',' + address[0]
	maplink = urllib.urlopen(mapurl)	
	mapdata = json.loads(maplink.read())
	
	try:
		mapresult = mapdata['results'][0]['address_components']
		for x in mapresult:
			if 'postal_code' in x['types']:
				az = address,'\t',x['long_name']
				zipcodelist.append(az)
				getzip = True
				break
		if not getzip:
			mapresult = mapdata['results'][1]['address_components']
			for x in mapresult:
				if 'postal_code' in x['types']:
					az = address,'\t',x['long_name']
					zipcodelist.append(az)
					getzip = True
					break
	except IndexError, e:
		az = address,'\t','No Data'
		zipcodelist.append(az)
	
#print 'Number of record in zip code list:',len(zipcodelist)

with open("subway_entrance_zip_2.csv", "wb") as f:
    datawriter = csv.writer(f, dialect='excel')
    datawriter.writerows(zipcodelist)
f.close()