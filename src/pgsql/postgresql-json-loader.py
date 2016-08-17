import argparse
import csv, os, time
import psycopg2  # psycopg2 v2.5.1

import sys
sys.path.append('../modules')
from result import Result

# Get command line arguments
parser = argparse.ArgumentParser(description='Load SNP and locus data')
parser.add_argument('--dev', action='store_true', help='Only load chromosome 21 for development testing')
parser.add_argument('--db', type=str, help='Postgres database name')
parser.add_argument('--username', type=str, help='Postgres username')
parser.add_argument('--password', type=str, help='Postgres password')
parser.add_argument('--jsonb', action='store_true', help='Use pgsql binary json type')
parser.add_argument('--pgcopy', action='store_true', help='Load data from file with COPY method')
parser.add_argument('--tag', type=str, help='Tag to place in results file')
parser.add_argument('--path', help='Path to chromosome data')
parser.add_argument('--start', type=str, help='Chromosome to start load from')
parser.add_argument('--indexes', action='store_true', help='Create indexes')
parser.add_argument('--queries', action='store_true', help='Run queries')
args = parser.parse_args()

# Set script version
scriptVersion = "2.0"

# Set default variables
dev = False
pgcopy = False
createIndexes = False
runQueries = False
databaseName = 'snp_research'
username = 'dev'
password = ''
path = ''
tag = ''
start = '1'
jsonb = False

# Update any present from CLI
if args.dev: # If dev mode, only load chr 21
    dev = True
    
if args.path is not None: # If set, use as root path for chromosome data
    path = args.path
if args.db is not None: # If set, use as database name for Postgres
    databaseName = args.db
if args.username is not None: # Postgres username
    username = args.username
if args.password is not None: # Postgres password
    password = args.password
if args.jsonb:
    jsonb = True
if args.pgcopy is not None:
    pgcopy = args.pgcopy
if args.tag is not None: # Tag to place in results file
    tag = args.tag
if args.start is not None:
    start = args.start
if args.indexes is not None:
    createIndexes = args.indexes
if args.queries is not None:
    runQueries = args.queries
    
# Open results file
resultsFileName = 'results-pgsql-json'
if resultsFileName != "":
    resultsFileName += '-' + tag
resultsFileName += '.txt'
resultsFile = open(resultsFileName, 'w')
resultsFile.write(scriptVersion + '\n')
result = Result()
resultsFile.write(result.toHeader() + '\n')

# Data files
snpFilePath = 'snpData-chr{0}.txt'
lociFilePath = 'lociData-chr{0}.txt'

# Chromosome list
chromosomes = ["21"] # dev list

# If not in dev mode, iterate through all chromosomes
if dev is False:
    chromosomes = ["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","X","Y","MT"] # complete list
    if start != "1": # Allow restart from anywhere in chromosome list, sequentially as ordered above
        startList = []
        hitMin = False
        for cur in chromosomes:
            if cur == start:
                hitMin = True
            if hitMin:
                startList.append(cur)
        chromosomes = startList

# Create Postgres database, tables if not exists
# For initial connection, connect to user database and then create the experimental db
postgresConnection = psycopg2.connect("dbname=" + username + " user=" + username)
postgresConnection.autocommit = True
createDbCursor = postgresConnection.cursor()
createDbCursor.execute("DROP DATABASE " + databaseName)
createDbCursor.execute("CREATE DATABASE " + databaseName)
createDbCursor.close()
postgresConnection.close() # Reconnect with database name

# Reopen connection to the experimental database to create tables and begin inserts
postgresConnection = psycopg2.connect("dbname=" + databaseName + " user=" + username)
createDbCursor = postgresConnection.cursor()

TABLES = {}

if jsonb:
    TABLES['snp'] = (
        "CREATE TABLE IF NOT EXISTS snp ("
        "  id serial PRIMARY KEY,"
        "  jsondata jsonb"
        ");")
else:
    TABLES['snp'] = (
        "CREATE TABLE IF NOT EXISTS snp ("
        "  id serial PRIMARY KEY,"
        "  jsondata json"
        ");")

for name, ddl in TABLES.iteritems():
    createDbCursor.execute(ddl)
    postgresConnection.commit()

# Disable triggers/constraints on tables
createDbCursor.execute("ALTER TABLE snp DISABLE trigger ALL;")

createDbCursor.close()

# Dictionaries and arrays for SQL and MongoDB queries
documents = {}     # Dictionary for MongoDB SNP/loci documents

for curChr in chromosomes:
    result = Result()
    result.method = "pgsql-json"
    if jsonb:
        result.method = "pgsql-jsonb"
    result.tag = tag    
    print "Chromosome " + str(curChr)
    result.chromosome = str(curChr)
    
    # Set file paths for current chromosome
    curSnpFilePath = snpFilePath.format(curChr)
    curLociFilePath = lociFilePath.format(curChr)
    
    if len(path) > 0:
        curSnpFilePath = path.rstrip('\\').rstrip('/') + '\\' + curSnpFilePath
        curLociFilePath = path.rstrip('\\').rstrip('/') + '\\' + curLociFilePath
    
    documents.clear()

    print "Chromosome " + str(curChr) + ". Reading SNP Data"
    result.snpLoadStart = time.time()
    sys.stdout.flush()

    # Read in data from SNP file
    with open(curSnpFilePath,'r') as csvfile:
        data = csv.reader(csvfile,delimiter='\t')
        for row in data:
            if(len(row) == 3):
                hasSig = False
                if row[2] != '' and row[2] != 'false':
                    hasSig = True
                documents[row[0]] = {"rsid":row[0], "chr":row[1], "has_sig":hasSig, "loci":[]}

    result.snpLoadEnd = time.time()
           
    print "Chromosome " + str(curChr) + ". Reading loci Data."
    result.lociLoadStart = time.time()
    
    # Now that we have primary keys for each SNP, read in loci data
    with open(curLociFilePath,'r') as csvfile:
        data = csv.reader(csvfile,delimiter='\t')
        for row in data:
            if(len(row) == 4 and row[0] in documents):
                # Load loci in Mongo documents
                curDoc = documents[row[0]]
                if curDoc["loci"] is None:
                    curDoc["loci"] = [{"mrna_acc":row[1],"gene":row[2],"class":row[3]}]
                else:
                    curDoc["loci"].append({"mrna_acc":row[1],"gene":row[2],"class":row[3]})
                documents[row[0]] = curDoc
    
    cursor = postgresConnection.cursor()

    # Data for reporting
    result.lociLoadEnd = time.time()
    result.totalDocuments = len(documents)

    print "Starting to insert " + str(result.totalDocuments) + " documents"
    sys.stdout.flush()

    # Log start time for MongoDB inserts
    result.documentInsertStart = time.time()

    if pgcopy:
        mimpfile = "/home/ec2-user/jsonchr" + str(curChr) + ".json"
        
        print "Writing json file for copy"
        sys.stdout.flush()
        
        fp = open(mimpfile,'w')
        for curDoc in documents.values():
            json.dump(curDoc,fp)
            fp.write('\n')
        fp.close()

        print "Loading json with copy method"
        sys.stdout.flush()
        
        # Restart insert time
        result.documentInsertStart = time.time()
        
        cursor.execute("COPY snp (jsondata) FROM '" + mimpfile + "'")

        os.remove(mimpfile)
    else:
        print "Individual document inserting starting"
        sys.stdout.flush()

        # Insert each document with SNP and loci data
        for v in documents.iteritems():
            cursor.execute("insert into snp (jsondata) values (%s)", [json.dumps(v[1])])

    
    # Commit data to pgsql
    postgresConnection.commit()
    
    # Log end time and total pgsql time
    result.documentInsertEnd = time.time()
    result.calculate()
    
    # Close pgsql cursor
    cursor.close()

    print result.toTerm()
    resultsFile.write(result.toString() + '\n')
    sys.stdout.flush()

# Create new cursor, create indexes and run test queries
cursor = postgresConnection.cursor()    

print "Turning on key checks..."
cursor.execute("ALTER TABLE snp ENABLE trigger ALL;")

if createIndexes:
    result = Result()
    result.method = "pgsql-jsonIdx"
    result.tag = tag

    rsidIndex = "CREATE INDEX idx_rsid ON snp USING GIN ((jsondata -> 'rsid'))"
    clinIndex = "CREATE INDEX idx_clin ON snp USING GIN ((jsondata -> 'has_sig'))"
    geneIndex = "CREATE INDEX idx_gene ON snp USING GIN ((jsondata -> 'loci') jsonb_path_ops)"
    fullIndex = "CREATE INDEX idx_full ON snp USING GIN ((jsondata) jsonb_path_ops)"
    
    print "Creating RSID index..."
    sys.stdout.flush()
    idxStart = time.time()
    cursor.execute(rsidIndex)
    postgresConnection.commit()
    idxEnd = time.time()
    result.idxRsid = idxEnd - idxStart
    
    print "Creating ClinSig index..."
    sys.stdout.flush()
    idxStart = time.time()
    cursor.execute(clinIndex)
    postgresConnection.commit()
    idxEnd = time.time()
    result.idxClinSig = idxEnd - idxStart        

    print "Creating Gene index..."
    sys.stdout.flush()
    idxStart = time.time()
    cursor.execute(geneIndex)
    postgresConnection.commit()
    idxEnd = time.time()
    result.idxGene = idxEnd - idxStart

    print "Creating full GIN index..."
    sys.stdout.flush()
    idxStart = time.time()
    cursor.execute(fullIndex)
    postgresConnection.commit()
    idxEnd = time.time()
    print "Full GIN Index: " + str(idxEnd-idxStart)


    resultsFile.write(result.toString() + '\n')
    sys.stdout.flush()
       
if runQueries:
    for z in range(1,11):
        result = Result()
        result.method = "pgsql-jsonQry" + str(z)
        result.tag = tag
        print "Running queries, count " + str(z)
        sys.stdout.flush()

        idxStart = time.time()
        cursor.execute('SELECT * FROM snp WHERE jsondata @> \'{"rsid" : "rs8788"}\'')
        idxEnd = time.time()
        result.qryByRsid = idxEnd - idxStart

        idxStart = time.time()
        cursor.execute('SELECT count(*) FROM snp WHERE jsondata @> \'{"has_sig":true}\'')
        idxEnd = time.time()
        result.qryByClinSig = idxEnd - idxStart

        idxStart = time.time()
        cursor.execute('SELECT count(*) FROM snp WHERE jsondata->\'loci\' @> \'[{"gene":"GRIN2B"}]\'')
        idxEnd = time.time()
        result.qryByGene = idxEnd - idxStart
    
        idxStart = time.time()
        cursor.execute('SELECT count(*) FROM snp WHERE jsondata->\'loci\' @> \'[{"loci.gene":"GRIN2B"}]\' AND jsondata @> \'{"has_sig":true}\'')
        idxEnd = time.time()
        result.qryByGeneSig = idxEnd - idxStart        

        resultsFile.write(result.toString() + '\n')

# Close pgsql cursor
cursor.close()

resultsFile.close()

postgresConnection.close()
print "Run complete."