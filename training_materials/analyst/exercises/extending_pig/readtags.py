#!/usr/bin/env python

from tagger import *
import sys

# INPUT:  Path of an MP3 file
# 
# OUTPUT: Line with the following tab-delimited fields:
#
#  0: Path of MP3 file (matches input value)
#  1: Call category
#  2: CS agent ID
#  3: Customer ID
#  4: Timestamp (call start time)
#

def extract_data(mp3_path):
   tags = ID3v2(mp3_path)

   result = {'customer_id': '',
             'agent_id':    '',
             'timestamp':   '',
             'category':    ''}

   for frame in tags.frames:
      if frame.fid == 'TCON':
         result['category'] = frame.strings[0].rstrip('\0')
      elif frame.fid == 'TIT2':
         result['timestamp'] = frame.strings[0].rstrip('\0')
      elif frame.fid == 'TRCK':
         result['customer_id'] = frame.strings[0].rstrip('\0')
      elif frame.fid == 'TALB':
         result['agent_id'] = frame.strings[0].rstrip('\0')

   print "%s\t%s\t%s\t%s\t%s" % (mp3_path, result['category'], result['agent_id'], result['customer_id'], result['timestamp'])


for line in sys.stdin:
   line = line.strip()

   extract_data(line)

