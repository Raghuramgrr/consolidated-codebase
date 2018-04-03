#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import math
import utils as u 

def helpDBSCAN():
	print "OPTIONS: nbCluster= number of clusters, interest=minimum interest score to belong to a cluster, callback=script to call when click on an item"
	print "SOURCE: path of the script"
	print "Data:"
	print "1. Create a cluster: {name:'fdfssdf', size:0, children=[]}"
	print "2. A child is like this: {'name':name, 'size':1, 'display':isMember, 'key': name}. key is the id of the item (given to the callback function), display: boolean to say if we display it in the list or not, size always 1, name: text displayed"
	print "3. Add children to cluster and increase the size"
	print "4. Crate an array of clusters = data : f.e [cluster1, cluster2, ... ]"
	print "Filters: Call function utils.addFilter(chart, filter)"
def dbscan(title='', description='', data=[], source="", options={}):
	if "nbCluster" not in options.keys():
		options["nbCluster"] = -1
	if "interest" not in options.keys():
		options["interest"] = 0
	if "callback" not in options.keys():
		options["callback"] = ""
	return {
		'retailgear':{
	       'size': {
	       		'width':'full'
	       },
	       'filters': [],
	       'title': title,
	       'subtitle': '',
	       'description': description,
	       'insight':'',
	       'daterange':'',
	       'script':source,
	       'type': "dbscan"
	    },
	    "nbCluster":options["nbCluster"],
	    "interest": options["interest"],
	    "callback": options["callback"],
	    "data":data
	}



def helpOptogenic():
	print "OPTIONS: range, mindate, maxdate"
	print 'Data : Array of {"articles": "nameArticle", "total":45, "name":"groupId"}'
	print "Articles of the same group have the same color."
def optogenic(title='', description='', data=[], source="", options={}):
	if "range" not in options.keys():
		options["range"] = [0,1,2,3,4,5,6]
	if "mindate" not in options.keys():
		options["mindate"] = str(u.monthdelta(u.today()-6)).split(' ')[0]
	if "maxdate" not in options.keys():
		options["maxdate"] = str(u.today()).split(' ')[0]
	return {
		'retailgear':{
	       'size': {
	       		'width':'full'
	       },
	       'filters': [],
	       'title': title,
	       'subtitle': '',
	       'description': description,
	       'insight':'',
	       'daterange':'',
	       'script':source,
	       'type': "optogenic"
	    },
	    "range": options["range"],
	    "mindate": options["mindate"],
	    "maxdate":options["maxdate"],
	    "data":data
	}



def helpSunburst():
	print "OPTIONS: NO OPTIONS NEEDED"
	print "DATA: Must be a tree"
	print 'ROOT: { "name": "Products Sold", "children":[]}'
	print 'CHILDREN: Same as ROOT: { "name": "CHILDREN 1", "children":[]}'
	print 'ONLY THE LAST CHILDREN ARE DIFFERENT: {"name":CHILDREN OF CHILDREN 1, "size": 0}'
def sunburst(title='', description='', data=[], source="", options={}):
	return {
		'retailgear':{
	       'size': {
	       		'width':'full'
	       },
	       'filters': [],
	       'title': title,
	       'subtitle': '',
	       'description': description,
	       'insight':'',
	       'daterange':'',
	       'script':source,
	       'type': "sunburst"
	    },
	    'data':data
	}
	

def basicbubble(title='', description='', data=[], source="", options={}):
	return {
		'retailgear':{
	       'size': {
	       		'width':'full'
	       },
	       'filters': [],
	       'title': title,
	       'subtitle': '',
	       'description': description,
	       'insight':'',
	       'daterange':'',
	       'script':source,
	       'type': "basicbubble"
	    },
	    'data':data
	}
	


def helpChord():
	print ""
def chord(title='', description='', data=[], source="", options={}):
	if "colors" not in options.keys():
		options["colors"]=['#00BCD4', '#7E57C2', '#90CAF9', '#009688', '#8BC34A', '#4CAF50', '#CDDC39', '#FF9800','#F44336', '#FF7043', '#BA68C8',  '#795548']
	return {
		'retailgear':{
	       'size': {
	       		'width':'full'
	       },
	       'filters': [],
	       'title': title,
	       'subtitle': '',
	       'description': description,
	       'insight':'',
	       'daterange':'',
	       'script':source,
	       'type': "chord"
	    },
	    'data':data,
	    "colors": options["colors"]
	}

def wordcloud(title='', description='', data=[], source="", options={}):
	return {
		'retailgear':{
	       'size': {
	       		'width':''
	       },
	       'filters': [],
	       'title': title,
	       'subtitle': '',
	       'description': description,
	       'insight':'',
	       'daterange':'',
	       'script':source,
	       'type': "wordcloud"
	    },
	    'data':data
	}
	

def community(title='', description='', data=[], source="", options={}):
	return {
		'retailgear':{
	       'size': {
	       		'width':''
	       },
	       'filters': [],
	       'title': title,
	       'subtitle': '',
	       'description': description,
	       'insight':'',
	       'daterange':'',
	       'script':source,
	       'type': "community"
	    },
	    'data':data
	}
	