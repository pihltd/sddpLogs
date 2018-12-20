#!/usr/bin/env python

import requests
import argparse

def main (args):
  res = requests.get("http://localhost:9200")
  print(res.content)
  
if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-v", "--verbose", action = "store_true", help = "Enable verbose feedback")

  args = parser.parse_args()
  main(args)
