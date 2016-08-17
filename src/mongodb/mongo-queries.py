import argparse
import csv, os, time
from pymongo import MongoClient, ASCENDING # https://pypi.python.org/pypi/pymongo/ (v2.6.3)
from result import Result
import gspread, getpass, json, os # https://pypi.python.org/pypi/gspread/ (v0.1.0)

# Get command line arguments
parser = argparse.ArgumentParser(description='Run MongoDB queries')
parser.add_argument('--db', type=str, help='MongoDB database name')
parser.add_argument('--ohost', type=str, help='MongoDB host')
parser.add_argument('--coll', type=str, help='MongoDB collection')
parser.add_argument('--tag', type=str, help='Tag to place in results file')
parser.add_argument('--remote', action='store_true', help='Enable remote reporting')
parser.add_argument('--rkey', help='Google document key')

args = parser.parse_args()

# Set default variables
remote = False
databaseName = 'snp_research'
mongoHost = 'mongodb://localhost:27017/'
collectionName = 'snps'
tag = ''
docKey = ''

# Update any present from CLI
if args.remote and args.rkey is not None: # If set to remote log and document key is present, log to GDocs
    remote = True
    docKey = args.rkey
else:
    remote = False

if args.db is not None: # If set, use as database name for MySQL and MongoDB
    databaseName = args.db
if args.ohost is not None: # MongoDB connection string
    mongoHost = args.ohost
if args.coll is not None: # MongoDB collection name
    collectionName = args.coll
if args.tag is not None: # Tag to place in results file
    tag = args.tag

# Open results file, print headers
resultsFileName = 'results-mongoqueries'
if resultsFileName != "":
    resultsFileName += '-' + tag
resultsFileName += '.txt'
resultsFile = open(resultsFileName, 'w')
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

# Create MongoDB connection
mongoClient = MongoClient(mongoHost)
mongoDb = mongoClient[databaseName]
mongoCollection = mongoDb[collectionName]
    
genes = ["ACSL6","ZDHHC8","TPH1","SYN2","DISC1","DISC2","COMT","FXYD6","ERBB4","DAOA","MEGF10","SLC18A1","DYM","SREBF2","NXRN1","CSF2RA","IL3RA","DRD2"]

for z in range(1,11):
    for g in genes:
        result = Result()
        result.method = "Mongo-QrySet" + str(z)
        result.tag = tag + "-" + g + "/" + str(z)
        print "Running queries: " + g + "/" + str(z)
    
        qryStart = time.time()
        temptotal = mongoCollection.find({"loci.gene":g}).count()
        qryEnd = time.time()
        result.qryByGene = qryEnd-qryStart        
    
        qryStart = time.time()
        temptotal = mongoCollection.find({"has_sig":True,"loci.gene":g}).count()
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

print "Run complete!"