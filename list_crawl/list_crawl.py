import os
import sys
###
from collections import defaultdict
###
# from prettyplotlib import plt
###
import boto
conn = boto.connect_s3(anon=True)

# Since April 2016, the public dataset bucket is s3://commoncrawl 
# (migrated from s3://aws-publicdatasets/common-crawl)
pds = conn.get_bucket('commoncrawl')

# CC-MAIN-2016-07
target = str(sys.argv[1])

sys.stderr.write('Processing {}\n'.format(target))

cdx_bucket_conn = pds
cdx_path = 'cc-index/cdx/' + target
if len(sys.argv) > 2:
    print('Looking for cdx files in s3://{}/{}/'.format(sys.argv[2], sys.argv[3]))
    cdx_bucket_conn = boto.connect_s3().get_bucket(sys.argv[2])
    cdx_path = str(sys.argv[3])

# Get all segments
segments = list(pds.list('crawl-data/{}/segments/'.format(target), delimiter='/'))
# Record the total size and all file paths for the segments
files = dict(warc=[], wet=[], wat=[], segment=[x.name for x in segments],
             robotstxt=[], non200responses=[], cdx=[])
size = dict(warc=[], wet=[], wat=[], robotstxt=[], non200responses=[], cdx=[])
files_per_segment = dict()

# Traverse each segment and all the files they contain
for i, segment in enumerate(segments):
  sys.stderr.write('\rProcessing segment {} of {}'.format(i, len(segments)))
  seg = segment.name.split('/')[-2]
  files_per_segment[seg] = defaultdict(int)
  for ftype in ['warc', 'wat', 'wet', 'robotstxt', 'non200responses']:
    path = segment.name + ftype + '/'
    if ftype == 'non200responses':
      # poorly named for historical reasons
      path = segment.name + 'crawldiagnostics/'
    for f in pds.list(path):
      files[ftype].append(f.name)
      size[ftype].append(f.size)
      files_per_segment[seg][ftype] += 1
  for f in cdx_bucket_conn.list(cdx_path + '/segments/' + seg + '/'):
    files['cdx'].append(f.name)
    size['cdx'].append(f.size)
    files_per_segment[seg]['cdx'] += 1
sys.stderr.write('\n')

# Write total size and file paths to files
prefix = 'crawl_stats/{}/'.format(target)
if not os.path.exists(prefix):
  os.makedirs(prefix)
###
f = open(prefix + 'crawl.size', 'w')
for ftype, val in sorted(size.items()):
  f.write('{}\t{}\t{}\n'.format(ftype, sum(val), len(val)))
f.close()
###
for ftype in files:
  f = open(prefix + '{}.paths'.format(ftype), 'w')
  for fn in files[ftype]:
    f.write(fn + '\n')
  f.close()
###
# Kid friendly stats (i.e. console)
for ftype, fsize in size.items():
  sys.stderr.write('{} files contain {} bytes over {} files\n'.format(ftype.upper(), sum(fsize), len(files[ftype])))
###
# To upload to the correct spot on S3
# gzip *.paths
# s3cmd put --acl-public *.paths.gz s3://commoncrawl/crawl-data/CC-MAIN-YYYY-WW/

###
# Plot
#for ftype, fsize in size.items():
#  if not fsize:
#    continue
#  plt.hist(fsize, bins=50)
#  plt.xlabel('Size (bytes)')
#  plt.ylabel('Count')
#  plt.title('Distribution for {}'.format(ftype.upper()))
#  plt.savefig(prefix + '{}_dist.pdf'.format(ftype))
#  #plt.show(block=True)
#  plt.close()
###

# Find missing WAT / WET files
warc = set([x.strip() for x in open(prefix + 'warc.paths').readlines()])
wat = [x.strip() for x in open(prefix + 'wat.paths').readlines()]
wat = set([x.replace('.warc.wat.', '.warc.').replace('/wat/', '/warc/') for x in wat])
wet = [x.strip() for x in open(prefix + 'wet.paths').readlines()]
wet = set([x.replace('.warc.wet.', '.warc.').replace('/wet/', '/warc/') for x in wet])

# Work out the missing files and segments
missing_wat = sorted(warc - wat)
missing_segments = defaultdict(list)
missing_files = 0
missing_cdx = 0

for fn in missing_wat:
  start, suffix = fn.split('/warc/')
  segment = start.split('/')[-1]
  missing_segments[segment].append(fn)
  missing_files += 1

missing_wet = sorted(warc - wet)
unpaired_wat_wet = []
for fn in missing_wet:
  start, suffix = fn.split('/warc/')
  segment = start.split('/')[-1]
  if fn not in missing_segments[segment]:
    missing_files += 1
    missing_segments[segment].append(fn)
    sys.stderr.write('Missing WET file (WAT exists!) in segment {} for WARC: {}\n'.format(segment, fn))
    sys.stderr.write('Remove WAT to resume: {}\n'.format(fn.replace('.warc.', '.warc.wat.').replace('/warc/', '/wat/')))

for fn in missing_wat:
  if fn not in missing_wet:
    start, suffix = fn.split('/warc/')
    segment = start.split('/')[-1]
    sys.stderr.write('Missing WAT file (WET exists!) in segment {} for WARC: {}\n'.format(segment, fn))
    sys.stderr.write('Remove WET to resume: {}\n'.format(fn.replace('.warc.', '.warc.wet.').replace('/warc/', '/wet/')))

# Save the files such that we can run a new WEATGenerator job
prefix += 'weat.queued/'
if not os.path.exists(prefix):
  os.mkdir(prefix)
sys.stderr.write('Total of {} missing/incomplete segments with {} missing parts\n'.format(len(missing_segments), missing_files))
for seg, files in missing_segments.iteritems():
  sys.stderr.write('{} has {} missing parts out of {}\n'.format(seg, len(files), files_per_segment[seg]['warc']))
  f = open(prefix + 'seg_{}'.format(seg), 'w')
  [f.write('s3a://commoncrawl/{}\n'.format(fn)) for fn in files]
  f.close()

for seg in files_per_segment.keys():
  warcs = files_per_segment[seg]['warc']
  warcs += files_per_segment[seg]['robotstxt']
  warcs += files_per_segment[seg]['non200responses']
  cdx = files_per_segment[seg]['cdx']
  if cdx != warcs:
    sys.stderr.write('{} has incorrect number of cdx files ({}, expected {})\n'.format(seg, cdx, warcs))
    missing_cdx += (warcs - cdx)

if len(segments) != 100:
  sys.stderr.write('expected 100 segments, got {}\n'.format(len(segments)))
  sys.exit(1)

if missing_cdx != 0:
  sys.exit(2)

if missing_files != 0:
  sys.exit(3)
