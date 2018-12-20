#!/usr/bin/env python


import argparse
import boto3
import sqlite3
import os
import re
import pprint

def makeDB(dbfilename, verbose):
  #Open and configure the databse if necessary
  exists = os.path.isfile(dbfilename)
  db = sqlite3.connect(dbfilename)
  if not exists:
    #create the table
    cursor = db.cursor()
    cursor.execute('''
      CREATE TABLE logs(id INTEGER PRIMARY KEY, owner TEXT, bucket TEXT, time TEXT, remote_ip TEXT, requester TEXT, request_id TEXT,
        operation TEXT, key TEXT, request_uri TEXT, http_status INTEGER, error_code TEXT, bytes_sent INTEGER, object_size INTEGER,
        total_time INTEGER, turn_around_time INTEGER, referrer TEXT, user_agent TEXT, version_id TEXT)
    ''')
    db.commit()
  return db

def getLogFiles(bucketname, verbose):
  filelist = []
  s3 = boto3.client('s3')
  paginator = s3.get_paginator('list_objects')
  if "/" in bucketname:
    namelist = bucketname.split('/')
    bucketname = namelist.pop(0)
    bucketkey = '/'.join(namelist)
    page_iterator = paginator.paginate(Bucket = bucketname, Prefix = bucketkey)
  else:
    page_iterator = paginator.paginate(Bucket = bucketname)
  for page in page_iterator:
    for entry in page['Contents']:
      filelist.append(entry['Key'])
  return filelist


def s3ReadObject(bucket, file, verbose):
  s3 = boto3.resource('s3')
  obj = s3.Object(bucket, file)
  contents = obj.get()['Body'].read().decode('utf-8')
  return contents

def logParse(data, verbose):
  #Parses the contents of a log file and returns a dictionary
  parsed = {}
  columns = ("owner", "bucket", "time", "remote_ip", "requester", "request_id", "operation", "key", "request_uri", "http_status", "error_code", "bytes_sent", "object_size", "total_time", "turn_around_time", "referrer", "user_agent", "version_id")

  #Split the line on quotes
  quotesplit = data.split('"')
  #Start splitting on spaces
  finallist = quotesplit[0].split(' ')
  #Put the dat back together
  date = finallist[2] + ' ' + finallist[3]
  date = re.sub("/", " ",date)
  date = re.sub("\[","",date)
  finallist[2] = date
  del finallist[3]

  finallist.append(quotesplit[1])
  templist = quotesplit[2].split(' ')
  finallist = finallist + templist
  finallist.append(quotesplit[3])
  #Ignore 4, it's an aretefact of the quote split
  finallist.append(quotesplit[5])
  finallist.append(quotesplit[6])
  #Clean up arefacts from splitting
  del finallist[8]
  del finallist[10]
  del finallist[17]

  counter = 0
  for column in columns:
    parsed[column] = finallist[counter]
    counter = counter +1

  return parsed

def loadDatabase(data, db, verbose):
  cursor = db.cursor()
  cursor.execute('''
    INSERT INTO logs(owner, bucket, time, remote_ip, requester, request_id, operation, key, request_uri, http_status, error_code, bytes_sent, object_size, total_time, turn_around_time, referrer, user_agent, version_id)
    VALUES(:owner, :bucket, :time, :remote_ip, :requester, :request_id, :operation, :key, :request_uri, :http_status, :error_code, :bytes_sent, :object_size, :total_time, :turn_around_time, :referrer, :user_agent, :version_id)
  ''', data)
  db.commit()

def main(args):
  #Get the database handle
  db = makeDB(args.database, args.verbose)
  #Get the file list from the bucket
  filelist = getLogFiles(args.bucket, args.verbose)
  if args.testmode:
    filelist = filelist[:10]
  #Get the data from the log files
  counter = 1
  fullcount = len(filelist)
  for file in filelist:
    print(("Processing %s of %s") % (str(counter), str(fullcount)))
    data = s3ReadObject(args.bucket, file, args.verbose)
    parseddata = logParse(data, args.verbose)
    if args.verbose:
      pprint.pprint(parseddata)
    loadDatabase(parseddata, db, args.verbose)
    counter = counter + 1

  db.close()

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-v", "--verbose", action = "store_true", help = "Enable verbose feedback")
  parser.add_argument("-d", "--database", required = True, help = "Database file to use")
  parser.add_argument("-b", "--bucket", required = True, help = "Bucket contaiins AWS log files")
  parser.add_argument("-t", "--testmode", action = "store_true", help = "Run in test mode, 10 log files only")
  args = parser.parse_args()
  main(args)
