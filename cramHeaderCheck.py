#!/usr/bin/env python
import argparse
import subprocess


def main(args):
  if args.verbose:
    print(args.file)
  cmd1 = ['samtools', 'view', '-H', args.file]
  cmd2 = ['grep', '^@SQ']

  samtoolsproc = subprocess.Popen(cmd1, stdout = subprocess.PIPE)
  grepproc = subprocess.Popen(cmd2, stdin = samtoolsproc.stdout, stdout = subprocess.PIPE)
  samtoolsproc.stdout.close()
  if args.verbose:
    print(grepproc.stdout)
  results,errors = grepproc.communicate()
  if args.verbose:
    print(results)
    print(errors)
  #fh = open('sq.txt', 'w')
  reslist = results.decode('utf-8').splitlines()
  #for line in reslist:
  #  fh.write(line + "\n")
  #  if args.verbose:
  #    print(line)
  #fh.close
  if args.verbose:
    counter = 0
    for line in reslist:
      temp = line.split('\t')
      for item in temp:
        print(("%s: %s") % (item, str(counter)))
        counter = counter + 1

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("-v", "--verbose", action = "store_true", help = "Verbose output")
  parser.add_argument("-f", "--file", required = True, help = "Cram file to analyze")
  args = parser.parse_args()
  main(args)
