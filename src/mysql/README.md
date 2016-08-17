research_snpdb - MySQL
==============

<pre>
  usage: mysql-loader.py [-h] [--dev] [--path PATH] [--method METHOD] [--db DB]
                 [--yhost YHOST] [--username USERNAME] [--password PASSWORD]
                 [--ohost OHOST] [--coll COLL]

  Load SNP and locus data

  optional arguments:
    -h, --help           show this help message and exit
    --dev                Only load chromosome 21 for development testing
    --path PATH          Path to chromosome data
    --method METHOD      Load type: M(y)SQL, M(o)ngoDB, or (b)oth
    --db DB              MySQL and MongoDB database name
    --yhost YHOST        MySQL host
    --username USERNAME  MySQL username
    --password PASSWORD  MySQL password
    --ohost OHOST        MongoDB host
    --coll COLL          MongoDB collection
</pre>
