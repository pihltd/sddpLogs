#!/usr/bin/env Python
import argparse
import requests
import pprint
import json
import sys
import boto3
import botocore
import re
from geolite2 import geolite2

def ipLocation(ip, verbose):
  loclist = []
  locstring = "N/A"
  reader = geolite2.reader()
  location = reader.get(ip)
  geolite2.close()
  if verbose:
    pprint.pprint(location)
  if location is not None:
    if 'city' in location:
      loclist.append(location['city']['names']['en'])
    else:
      loclist.append('n/a')
    if 'subdivisions' in location:
      loclist.append(location['subdivisions'][0]['iso_code'])
    else:
      loclist.append('n/a')
    if 'country' in location:
      loclist.append(location['country']['iso_code'])
    else:
      loclist.append('n/a')
    locstring = "\t".join(loclist)
    return locstring

def lineParse(data, verbose):
  parsed = {}
  headers = ['bucketOwner', 'bucket', 'time', 'remoteIP', 'requester', 'requesterID', 'operation', 'key', 'requestURI', 'httpStatus', 'errorCode',
    'bytesSent', 'objectSize', 'totalTime', 'turnAroundTime', 'referrer', 'userAgent', 'versionID']
  if verbose:
    pprint.pprint(data)

  info = {}
  #first split on quote
  quotesplit = data.split('"')

  if verbose:
    x = 0
    for quote in quotesplit:
      print(("Count: %s Value: %s") % (str(x), quote))
      x = x+ 1

  #Start splitting on spaces
  finallist = quotesplit[0].split(' ')
  #Put the date back together since it has a space and get rid of extra characters
  date = finallist[2] + ' ' + finallist[3]
  date = re.sub("/", " ", date)
  date = re.sub("\[", "", date)
  finallist[2] = date
  del finallist[3]

  finallist.append(quotesplit[1])
  templist = quotesplit[2].split(' ')
  finallist = finallist + templist
  finallist.append(quotesplit[3])
  #Ignore quotesplit[4], it is an aretfact of quote split
  finallist.append(quotesplit[5])
  finallist.append(quotesplit[6])
  if verbose:
    y =0
    for item in finallist:
      print(("Count: %s Value: %s") %(str(y),item))
      y = y + 1
  #Clean up artefacts from splitting
  del finallist[8]
  del finallist[10]
  del finallist[17]
  #Load the final dictionary
  counter = 0
  for header in headers:
    parsed[header] = finallist[counter]
    counter = counter + 1
  return parsed

def infoSummary(loginfo):
  summary = {}
  remoteIPList = []
  operationList = []
  timeList = []
  for entry in loginfo:
    if entry['remoteIP'] not in remoteIPList:
      remoteIPList.append(entry['remoteIP'])
    if entry['operation'] not in operationList:
      operationList.append(entry['operation'])
    if entry['time'] not in timeList:
      timeList.append(entry['time'])
  summary['remoteIP'] = remoteIPList
  summary['time'] = timeList
  summary['operation'] = operationList
  return summary

def getLogFiles(bucketname, verbose):
  filelist = []
  s3 = boto3.client('s3')
  paginator = s3.get_paginator('list_objects')
  if "/" in bucketname:
    namelist = bucketname.split('/')
    bucketname = namelist.pop(0)
    bucketkey = '/'.join(namelist)
    page_iterator = paginator.paginate(Bucket=bucketname, Prefix=bucketkey)
  else:
    page_iterator = paginator.paginate(Bucket=bucketname)
  for page in page_iterator:
    if verbose:
      pprint.pprint(page)
    for entry in page['Contents']:
      filelist.append(entry['Key'])
  return filelist

def bucketList():
  #Simply print the available buckets 
  s3 = boto3.resource('s3')
  for bucket in s3.buckets.all():
    print(bucket)

def s3ReadObject(bucket, key):
  s3 = boto3.resource('s3')
  obj = s3.Object(bucket, key)
  contents = obj.get()['Body'].read().decode('utf-8')
  return contents

def main(args):

  #If asked for a bucket list, print then exit
  if args.listbuckets:
    bucketList()
    sys.exit()

  bucketdata = []

  if args.file is not None:
    logfile = open(args.file, "r")
    data = logfile.read()
    results = lineParse(data, args.verbose)
    logfile.close
    print(results)

  if args.bucket is not None:
    #Get the list of files in the bucket
    logbucket = getLogFiles(args.bucket, args.verbose)
    if args.testmode:
      logbucket = logbucket[:10]

    if args.verbose:
      pprint.pprint(logbucket)
      print("File count:\t%s" % str(len(logbucket)))

    for file in logbucket:
      data = s3ReadObject(args.bucket, file)
      if args.verbose:
        print(data)
      results = lineParse(data, args.verbose)
      if args.verbose:
        pprint.pprint(results)
      #sys.exit()
      bucketdata.append(results)

    summary = infoSummary(bucketdata)
    locations = {}
    for ip in summary['remoteIP']:
      locations[ip] = ipLocation(ip, args.verbose)

    #Print out the report
    ips = str(len(summary['remoteIP']))
    tasks = str(len(summary['operation']))
    dates = str(len(summary['time']))
    print(("Unique IPs:\t%s\tUnique Tasks:\t%s\tUnique Dates:\t%s") % (ips, tasks, dates))
    print("\nUnique operations")
    for operation in summary['operation']:
      print(operation)
    print("\nUnique access times")
    for time in summary['time']:
      print(time)
  print("\nUnique remote IP addresses and potential locations")
  for ip, location in locations.items():
    print(("IP:\t%s\tLocation:\t%s") % (ip, location))

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-v", "--verbose", action = "store_true", help = 'Enable verbose feedback.')
  parser.add_argument("-b", "--bucket", help = "S3 bucket location")
  parser.add_argument("-f", "--file", help = "Process single log file only")
  parser.add_argument("-l", "--listbuckets", action = "store_true", help = "List all available buckets")
  parser.add_argument("-t", "--testmode", action = "store_true", help = "Run in test mode, only 10 files processed")
  args = parser.parse_args()
  main(args)
