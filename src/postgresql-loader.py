import argparse
import csv, os, time
import psycopg2  # psycopg2 v2.5.1
import result
from result import Result
import gspread, getpass # https://pypi.python.org/pypi/gspread/ (v0.1.0)
import sys

# Get command line arguments
parser = argparse.ArgumentParser(description='Load SNP and locus data')
parser.add_argument('--dev', action='store_true', help='Only load chromosome 21 for development testing')
parser.add_argument('--path', help='Path to chromosome data')
parser.add_argument('--db', type=str, help='Postgres database name')
parser.add_argument('--yhost', type=str, help='Postgres host')
parser.add_argument('--username', type=str, help='Postgres username')
parser.add_argument('--password', type=str, help='Postgres password')
parser.add_argument('--tag', type=str, help='Tag to place in results file')
parser.add_argument('--remote', action='store_true', help='Enable remote reporting')
parser.add_argument('--rkey', help='Google document key')
parser.add_argument('--start', type=str, help='Chromosome to start load from')
parser.add_argument('--indexes', action='store_true', help='Create indexes')
parser.add_argument('--queries', action='store_true', help='Run queries')
args = parser.parse_args()

# Set script version
scriptVersion = "2.0"

# Set default variables
dev = False
remote = False
createIndexes = False
runQueries = False
databaseName = 'snp_research'
username = 'dev'
password = ''
sqlHost = '127.0.0.1'
path = ''
tag = ''
docKey = ''
start = '1'

# Update any present from CLI
if args.dev: # If dev mode, only load chr 21
    dev = True
if args.remote and args.rkey is not None: # If set to remote log and document key is present, log to GDocs
    remote = True
    docKey = args.rkey
else:
    remote = False
    
if args.path is not None: # If set, use as root path for chromosome data
    path = args.path
if args.db is not None: # If set, use as database name for Postgres
    databaseName = args.db
if args.username is not None: # Postgres username
    username = args.username
if args.password is not None: # Postgres password
    password = args.password
if args.yhost is not None: # Postgres host name
    sqlHost = args.yhost
if args.tag is not None: # Tag to place in results file
    tag = args.tag
if args.start is not None:
    start = args.start
if args.indexes is not None:
    createIndexes = args.indexes
if args.queries is not None:
    runQueries = args.queries
    
# Open results file
resultsFileName = 'results-postgres'
if resultsFileName != "":
    resultsFileName += '-' + tag
resultsFileName += '.txt'
resultsFile = open(resultsFileName, 'w')
resultsFile.write(scriptVersion + '\n')
result = Result()
resultsFile.write(result.toHeader() + '\n')

if remote:
    gusername = raw_input("Enter Google username: ")
    gpassword = getpass.getpass("Enter Google password: ")    
    gs = gspread.Client(auth=(gusername,gpassword))
    gs.login()
    ss = gs.open_by_key(docKey)
    ws = ss.add_worksheet(tag + "-" + str(time.time()),1,1)
    ws.append_row(result.headerArr())

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
postgresConnection = psycopg2.connect("dbname=" + username + " user=" + username)
postgresConnection.autocommit = True
createDbCursor = postgresConnection.cursor()
createDbCursor.execute("DROP DATABASE " + databaseName)
createDbCursor.execute("CREATE DATABASE " + databaseName)
createDbCursor.close()
postgresConnection.close() # Reconnect with database name

postgresConnection = psycopg2.connect("dbname=" + databaseName + " user=" + username)
createDbCursor = postgresConnection.cursor()

TABLES = {}
TABLES['snp'] = (
    "CREATE TABLE IF NOT EXISTS snp ("
    "  id serial PRIMARY KEY,"
    "  rsid varchar,"
    "  chr varchar,"
    "  has_sig boolean"
    ");")
TABLES['locus'] = (
    "CREATE TABLE IF NOT EXISTS locus("
    "  id serial PRIMARY KEY,"
    "  mrna_acc varchar,"
    "  gene varchar,"
    "  class varchar,"
    "  snp_id integer,"
    "  CONSTRAINT idx_snp FOREIGN KEY (snp_id) REFERENCES snp (id) ON DELETE NO ACTION ON UPDATE NO ACTION"
    ");")

for name, ddl in TABLES.iteritems():
    createDbCursor.execute(ddl)
    postgresConnection.commit()

createDbCursor.execute("ALTER TABLE snp DISABLE trigger ALL;")
createDbCursor.execute("ALTER TABLE locus DISABLE trigger ALL;")

createDbCursor.close()

# Dictionaries and arrays for SQL and MongoDB queries
snpInserts = {}    # Dictionary for rsid/insert for SNP data
lociInserts = []   # Array for loci insert queries
rsidList = {}      # Dictionary of RSIDs that will also hold the 
                   # primary key for each SNP in SQL

for curChr in chromosomes:
    result = Result()
    result.method = "pgsql"
    result.tag = tag    
    print "Chromosome " + str(curChr)
    result.chromosome = str(curChr)
    
    # Set file paths for current chromosome
    curSnpFilePath = snpFilePath.format(curChr)
    curLociFilePath = lociFilePath.format(curChr)
    
    if len(path) > 0:
        curSnpFilePath = path.rstrip('\\') + '\\' + curSnpFilePath
        curLociFilePath = path.rsplit('\\') + '\\' + curLociFilePath
    
    # Clear dictionaries for loading multiple chromosomes
    snpInserts.clear()
    lociInserts = []
    rsidList.clear()

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
                rsidList[row[0]] = 0
                insStr = "INSERT INTO snp (rsid, chr, has_sig) VALUES ('{0}', '{1}', {2}) RETURNING id".format(row[0], row[1], hasSig)
                snpInserts[row[0]] = insStr
    
    # Data for reporting
    result.snpLoadEnd = time.time()
    result.totalSnps = len(snpInserts)
           
    # Insert SNP data into postgres
    createDbCursor = postgresConnection.cursor()

    print "Chromosome " + str(curChr) + ". Inserting SNP Data."
    sys.stdout.flush()

    # Log current run start time
    result.snpInsertStart = time.time()
    
    # For each snp, insert record and then grab primary key
    for rsid,snp in snpInserts.iteritems():
        createDbCursor.execute(snp)
        rsidList[rsid] = createDbCursor.fetchone()[0]
        
    # Commit all inserts to pgsql and grab end time
    postgresConnection.commit()
    
    # Log completed time, close pgsql cursor
    result.snpInsertEnd=time.time()
    createDbCursor.close()

    # Clear list of SNPs to free up memory
    snpInserts.clear()

    print "Chromosome " + str(curChr) + ". Reading loci Data."
    result.lociLoadStart = time.time()
    
    # Now that we have primary keys for each SNP, read in loci data
    with open(curLociFilePath,'r') as csvfile:
        data = csv.reader(csvfile,delimiter='\t')
        for row in data:
            if(len(row) == 4):
                # Load loci in pgsql statements
                if row[0] in rsidList and rsidList[row[0]] > 0: # If RSID value is present, load with PK
                    insStr = "INSERT INTO locus (mrna_acc, gene, class, snp_id) VALUES ('{0}', '{1}', '{2}', {3})".format(row[1], row[2], row[3], rsidList[row[0]])
                    lociInserts.append(insStr)
                
    # Data for reporting
    result.lociLoadEnd = time.time()
    result.totalLoci = len(lociInserts)
    
    # Create new cursor, enter loci data into pgsql
    cursor = postgresConnection.cursor()

    print "Chromosome " + str(curChr) + ". Inserting loci data."

    # Log current run start time and number of loci
    result.lociInsertStart = time.time()
    
    # Insert each locus
    for locus in lociInserts:
        cursor.execute(locus)
    
    # Commit data to pgsql
    postgresConnection.commit()
    
    # Log end time and total pgsql time
    result.lociInsertEnd = time.time()
    
    # Close pgsql cursor
    cursor.close()
    
    print result.toTerm()
    resultsFile.write(result.toString() + '\n')
    sys.stdout.flush()

    if remote:
        try:
            print "Sending to GDocs..."
            gs.login()
            ws.append_row(result.stringArr())
        except:
            print "Unable to send to GDocs, continuing..."

# Create new cursor, create indexes and run test queries
cursor = postgresConnection.cursor()    

print "Turning on key checks..."
cursor.execute("ALTER TABLE snp ENABLE trigger ALL;")
cursor.execute("ALTER TABLE locus ENABLE trigger ALL;")
cursor.execute("CREATE INDEX idx_snpid_fk ON locus (snp_id)")

if createIndexes:
    result = Result()
    result.method = "pgsql-Idx"
    result.tag = tag

    rsidIndex = "CREATE UNIQUE INDEX idx_rsid ON snp (rsid)"
    clinIndex = "CREATE INDEX idx_clin ON snp (has_sig)"
    geneIndex = "CREATE INDEX idx_gene ON locus (gene)"
    
    print "Creating RSID index..."
    idxStart = time.time()
    cursor.execute(rsidIndex)
    cursor.commit()
    idxEnd = time.time()
    result.idxRsid = idxEnd - idxStart
    
    print "Creating ClinSig index..."
    idxStart = time.time()
    cursor.execute(clinIndex)
    cursor.commit()
    idxEnd = time.time()
    result.idxClinSig = idxEnd - idxStart        

    print "Creating Gene index..."
    idxStart = time.time()
    cursor.execute(geneIndex)
    cursor.commit()
    idxEnd = time.time()
    result.idxGene = idxEnd - idxStart

    resultsFile.write(result.toString() + '\n')
    if remote:
        try:
            print "Sending to GDocs..."
            gs.login()
            ws.append_row(result.stringArr()) 
        except:
            print "Unable to send to GDocs, continuing..."
       
if runQueries:
    for z in range(1,101):
        result = Result()
        result.method = "pgsql-Qry" + str(z)
        result.tag = tag
        print "Running queries, count " + str(z)
    
        idxStart = time.time()
        cursor.execute("SELECT * FROM locus l, snp s WHERE l.snp_id = s.id AND s.rsid = 'rs8788'")
        idxEnd = time.time()
        result.qryByRsid = idxEnd - idxStart

        idxStart = time.time()
        cursor.execute("SELECT count(s.id) FROM locus l, snp s WHERE l.snp_id = s.id AND s.has_sig = true")
        idxEnd = time.time()
        result.qryByClinSig = idxEnd - idxStart

        idxStart = time.time()
        cursor.execute("SELECT count(distinct s.rsid) FROM locus l, snp s WHERE l.snp_id = s.id AND l.gene = 'GRIN2B'")
        idxEnd = time.time()
        result.qryByGene = idxEnd - idxStart
    
        idxStart = time.time()
        cursor.execute("SELECT count(distinct s.rsid) FROM locus l, snp s WHERE l.snp_id = s.id AND l.gene = 'GRIN2B' AND s.has_sig = true")
        idxEnd = time.time()
        result.qryByGeneSig = idxEnd - idxStart        

        resultsFile.write(result.toString() + '\n')
        if remote:
            try:
                print "Sending to GDocs..."
                gs.login()
                ws.append_row(result.stringArr()) 
            except:
                print "Unable to send to GDocs, continuing..."

# Close pgsql cursor
cursor.close()

resultsFile.close()

postgresConnection.close()
print "Run complete."