import argparse
import csv, os, time
import psycopg2  # psycopg2 v2.5.1

import sys
sys.path.append('../modules')
from result import Result

__author__ = "Wade Schulz, Donn Felker, Brent Nelson"
__credits__ = ["Wade Schulz", "Donn Felker", "Brent Nelson"]
__license__ = "MIT"
__version__ = "2.0.0"
__maintainer__ = "Wade Schulz"
__email__ = "wade.schulz@gmail.com"
__status__ = "Research"

# Get command line arguments
parser = argparse.ArgumentParser(description='Run PostgreSQL (NoSQL) queries')
parser.add_argument('--db', type=str, help='PostgreSQL database name')
parser.add_argument('--username', type=str, help='PostgreSQL username')
parser.add_argument('--password', type=str, help='PostgreSQL password')
parser.add_argument('--tag', type=str, help='Tag to place in results file')

args = parser.parse_args()

# Set default variables
remote = False
databaseName = 'snp_research'
username = 'dev'
password = ''
tag = ''

# Update any present from CLI
if args.db is not None: # If set, use as database name for Postgres
    databaseName = args.db
if args.username is not None: # Postgres username
    username = args.username
if args.password is not None: # Postgres password
    password = args.password
if args.tag is not None: # Tag to place in results file
    tag = args.tag

# Open results file, print headers
resultsFileName = 'qresults-pgsql-nosql'
if resultsFileName != "":
    resultsFileName += '-' + tag
resultsFileName += '.txt'
resultsFile = open(resultsFileName, 'w')
result = Result()
resultsFile.write(result.toHeader() + '\n')

# Create pgsql connection
postgresConnection = psycopg2.connect("dbname=" + databaseName + " user=" + username)
cursor = postgresConnection.cursor()
    
genes = ["ACSL6","ZDHHC8","TPH1","SYN2","DISC1","DISC2","COMT","FXYD6","ERBB4","DAOA","MEGF10","SLC18A1","DYM","SREBF2","NXRN1","CSF2RA","IL3RA","DRD2"]

for z in range(1,11):
    for g in genes:
        result = Result()
        result.method = "pgsql-jsonb-QrySet" + str(z)
        result.tag = tag + "-" + g + "/" + str(z)
        print "Running queries: " + g + "/" + str(z)
        sys.stdout.flush()

        qryStart = time.time()
        cursor.execute('SELECT count(*) FROM snp WHERE jsondata->\'loci\' @> \'[{"gene":"' + g + '"}]\'')
        qryEnd = time.time()
        result.qryByGene = qryEnd-qryStart        

        resultsFile.write(result.toString() + '\n')
        
print "Run complete!"