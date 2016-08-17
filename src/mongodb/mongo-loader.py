import argparse
import csv, os, time
from pymongo import MongoClient, ASCENDING # https://pypi.python.org/pypi/pymongo/ (v2.6.3)
from result import Result
import gspread, getpass, json, os # https://pypi.python.org/pypi/gspread/ (v0.1.0)

# Get command line arguments
parser = argparse.ArgumentParser(description='Load SNP and locus data')
parser.add_argument('--dev', action='store_true', help='Only load chromosome 21 for development testing')
parser.add_argument('--path', help='Path to chromosome data')
parser.add_argument('--db', type=str, help='MongoDB database name')
parser.add_argument('--ohost', type=str, help='MongoDB host')
parser.add_argument('--coll', type=str, help='MongoDB collection')
parser.add_argument('--tag', type=str, help='Tag to place in results file')
parser.add_argument('--remote', action='store_true', help='Enable remote reporting')
parser.add_argument('--rkey', help='Google document key')
parser.add_argument('--start', type=str, help='Chromosome to start load from')
parser.add_argument('--bulk', action='store_true', help='Perform bulk insert.')
parser.add_argument('--mongoimport', action='store_true', help='Bulk insert by creating json file and then using mongoimport.')
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
mongoHost = 'mongodb://localhost:27017/'
collectionName = 'snps'
path = ''
tag = ''
docKey = ''
start = '1'
bulk = False
mongoimport = False

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
if args.db is not None: # If set, use as database name for MySQL and MongoDB
    databaseName = args.db
if args.ohost is not None: # MongoDB connection string
    mongoHost = args.ohost
if args.coll is not None: # MongoDB collection name
    collectionName = args.coll
if args.tag is not None: # Tag to place in results file
    tag = args.tag
if args.start is not None:
    start = args.start
if args.bulk:
    bulk = True
if args.mongoimport:
    mongoimport = True
if args.indexes is not None:
    createIndexes = args.indexes
if args.queries is not None:
    runQueries = args.queries

# Open results file, print headers
resultsFileName = 'results-mongo'
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

# Create MongoDB and MySQL connections
mongoClient = MongoClient(mongoHost)
mongoDb = mongoClient[databaseName]
mongoCollection = mongoDb[collectionName]

# Dictionaries and arrays for SQL and MongoDB queries
documents = {}     # Dictionary for MongoDB SNP/loci documents

for curChr in chromosomes:
    result = Result()
    result.method = "Mongo"
    if bulk:
        result.method += "-Bulk"
    if mongoimport:
        result.method += "-jsonImport"
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
    documents.clear()

    print "Chromosome " + str(curChr) + ". Reading SNP data"
    result.snpLoadStart = time.time()
    
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

    print "Chromosome " + str(curChr) + ". Reading loci data."
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

    # Data for reporting
    result.lociLoadEnd = time.time()
    result.totalDocuments = len(documents)

    print "Starting to insert " + str(result.totalDocuments) + " documents"

    # Log start time for MongoDB inserts
    result.documentInsertStart = time.time()

    if bulk:
        print "Bulk insertion starting"
        mongoCollection.insert(documents.values())
    elif mongoimport:
        mimpfile = "jsonchr" + str(curChr) + ".json"
        print "Writing json file for mongoimport"
        fp = open(mimpfile,'w')
        for curDoc in documents.values():
            json.dump(curDoc,fp)
            fp.write('\n')
        fp.close()
        print "Loading json with mongoimport"
        # Restart insert time
        result.documentInsertStart = time.time()
        loadres = os.system("mongoimport --host " + mongoHost.rstrip('/').replace("mongodb://","") + " --db " + databaseName + " --collection " + collectionName + " --file " + mimpfile)
        os.remove(mimpfile)
    else:
        print "Individual document inserting starting"
        # Insert each document with SNP and loci data
        for v in documents.iteritems():
            mongoCollection.insert(v[1])

    # Log end time
    result.documentInsertEnd = time.time()
    result.calculate()
        
    print result.toTerm()
    resultsFile.write(result.toString() + '\n')
    if remote:
        try:
            print "Sending to GDocs..."
            gs.login()
            ws.append_row(result.stringArr())    
        except:
            print "Unable to send to GDocs, continuing..."

if createIndexes:
    result = Result()
    result.method = "Mongo-Idx"
    result.tag = tag
    
    print "Creating RSID index..."
    idxStart = time.time()
    mongoCollection.create_index("rsid", unique=True)
    idxEnd = time.time()
    result.idxRsid = idxEnd - idxStart
        
    print "Creating ClinSig index..."
    idxStart = time.time()
    mongoCollection.create_index("has_sig")
    idxEnd = time.time()
    result.idxClinSig = idxEnd - idxStart        

    print "Creating Gene index..."
    idxStart = time.time()
    mongoCollection.create_index("loci.gene")
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
        result.method = "Mongo-Qry" + str(z)
        result.tag = tag
        print "Running queries, count " + str(z)
        qryStart = time.time()
        mongoCollection.find({"rsid":"rs8788"})
        qryEnd = time.time()
        result.qryByRsid = qryEnd-qryStart
    
        qryStart = time.time()
        temptotal = mongoCollection.find({"has_sig":True}).count()
        qryEnd = time.time()
        result.qryByClinSig = qryEnd-qryStart
    
        qryStart = time.time()
        temptotal = mongoCollection.find({"loci.gene":"GRIN2B"}).count()
        qryEnd = time.time()
        result.qryByGene = qryEnd-qryStart        
    
        qryStart = time.time()
        temptotal = mongoCollection.find({"has_sig":True,"loci.gene":"GRIN2B"}).count()
        qryEnd = time.time()
        result.qryByGeneSig = qryEnd-qryStart     

        resultsFile.write(result.toString() + '\n')
        if remote:
            try:
                print "Sending to GDocs..."
                gs.login()
                ws.append_row(result.stringArr()) 
            except:
                print "Unable to send to GDocs, continuing..."

resultsFile.close()

mongoClient.close()
print "Run complete."