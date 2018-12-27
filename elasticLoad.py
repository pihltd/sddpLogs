#!/usr/bin/env python


import argparse
import boto3
import sqlite3
import os
import re
import pprint
import requests
from elasticsearch import Elasticsearch
import json
import sys

def esTest(verbose):
#Just check to see if Elasticsearch is working
  results = requests.get("http://localhost:9200")
  pprint.pprint(results)

def getLogFiles(bucketname, test, verbose):
  #Returns a list of off the files in a bucket.  Test mode only returns 10 files
  filelist = []
  s3 = boto3.client('s3')
  paginator = s3.get_paginator('list_objects')
  if "/" in bucketname:
    namelist = bucketname.split('/')
    bucketname = namelist.pop(0)
    bucketkey = '/'.join(namelist)
    if test:
      page_iterator = paginator.paginate(Bucket = bucketname, Prefix = bucketkey, PaginationConfig = {'MaxItems' : 10})
    else:
      page_iterator = paginator.paginate(Bucket = bucketname, Prefix = bucketkey)
  else:
    if test:
      page_iterator = paginator.paginate(Bucket=bucketname, PaginationConfig = {'MaxItems' : 10})
    else:
      page_iterator = paginator.paginate(Bucket = bucketname)
  for page in page_iterator:
    for entry in page['Contents']:
      filelist.append(entry['Key'])
  return filelist


def s3ReadObject(bucket, file, verbose):
  #Reads a file stored in an S3 bucket
  s3 = boto3.resource('s3')
  obj = s3.Object(bucket, file)
  contents = obj.get()['Body'].read().decode('utf-8')
  return contents

def parseDate(date):
  #Convert the AWS format into something Elasticsearch understands as time
  months = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06','Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
  final = []
  date = re.sub("/", " ",date)
  date = re.sub("\[","",date)
  date = re.sub("\]","",date)
  datelist = date.split(' ')
  day = datelist[0]
  final.append(day)
  month = months[datelist[1]]
  final.append(month)
  final.append(datelist[2])

  return (':').join(final)

def logParse(data, project, verbose):
  #Parses the contents of a log file and returns a dictionary
  parsed = {}
  columns = ("owner", "bucket", "time", "remote_ip", "requester", "request_id", "operation", "key", "request_uri", "http_status", "error_code", "bytes_sent", "object_size", "total_time", "turn_around_time", "referrer", "user_agent", "version_id")

  #Split the line on quotes
  quotesplit = data.split('"')
  #Start splitting on spaces
  finallist = quotesplit[0].split(' ')
  #Refactor the date into something usable by Elasticsearch
  finallist[2] = parseDate(finallist[2])
  del finallist[3]

  finallist.append(quotesplit[1])
  templist = quotesplit[2].split(' ')
  finallist = finallist + templist
  finallist.append(quotesplit[3])
  #Ignore 4, it's an artifact of the quote split
  finallist.append(quotesplit[5])
  finallist.append(quotesplit[6])
  #Clean up arefacts from splitting, delete from end to avoid messing up positings
  del finallist[17]
  del finallist[10]
  del finallist[8]

  parsed["project"] = project
  counter = 0
  for column in columns:
    parsed[column] = finallist[counter]
    counter = counter +1

  return parsed

def createESIndex(indexname, verbose):
  mapping =''' {
  "mappings" : {
  "s3logs" : {
      "properties" : {
         "project" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "owner" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "bucket": {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "time" : {"type" : "date", "format" : "dd:MM:yyyy:HH:mm:ss"},
         "remote_ip" : {"type" : "ip"},
         "requester" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "request_id" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "operation" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "key" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "request_uri" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "http_status" : {"type" : "integer"},
         "error_code" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "bytes_sent" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "object_size" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "total_time" : {"type" : "integer"},
         "turn_around_time" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "referrer" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "user_agent" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}},
         "version_id" : {"type" : "text", "fields" : {"raw" : {"type" : "keyword"}}}

      }
    }
    }
  }'''

  es = Elasticsearch()
  results = es.indices.create(index=indexname, body=mapping)
  if verbose:
    pprint.pprint(results)

def loadES(indexname, data, verbose):
  es = Elasticsearch()
  results = es.index(index = indexname, body = json.dumps(data),doc_type="s3logs")
  if verbose:
    pprint.pprint(results)

def deleteIndex(indexname, verbose):
  es = Elasticsearch()
  results = es.indices.delete(index = indexname)
  if verbose:
    pprint.pprint(results)

def main(args):
  if args.estest:
    esTest(args.verbose)
  elif args.delete:
    deleteIndex(args.index, args.verbose)
  elif args.create:
    createESIndex(args.index, args.verbose)
  else:
    #Get the file list from the bucket
    if args.verbose:
      print("Getting file list from %s" % args.bucket)
    filelist = getLogFiles(args.bucket, args.testmode, args.verbose)
    if args.testmode:
      if args.verbose:
        print("Entering test mode")
    #Get the data from the log files
    counter = 1
    fullcount = len(filelist)
    for file in filelist:
      if args.verbose:
        print(("Processing %s of %s") % (str(counter), str(fullcount)))
      data = s3ReadObject(args.bucket, file, args.verbose)
      #Parse data into a dictionary
      parseddata = logParse(data, args.project, args.verbose)
      #Load the data into Elasticsearch
      loadES(args.index, parseddata, args.verbose)
      counter = counter + 1

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-v", "--verbose", action = "store_true", help = "Enable verbose feedback")
  parser.add_argument("-e", "--estest", action = "store_true", help = "Test the elsaticsearch connection")
  parser.add_argument("-b", "--bucket", required = True, help = "Bucket contaiins AWS log files")
  parser.add_argument("-t", "--testmode", action = "store_true", help = "Run in test mode, 10 log files only")
  parser.add_argument("-i", "--index", required = True, help = "Elasticsearch Index name")
  parser.add_argument("-p", "--project", required = True, help = "SDD Project name")
  parser.add_argument("-d", "--delete", action = "store_true", help = "Delete the named index")
  parser.add_argument("-c", "--create", action = "store_true", help = "Create the named index")
  args = parser.parse_args()
  main(args)
