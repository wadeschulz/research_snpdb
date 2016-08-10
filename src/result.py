class Result:
    chromosome = ''
    method = ''
    tag = ''
    
    snpLoadStart = '-'
    snpLoadEnd = '-'
    snpLoadTime = '-'
    totalSnps = '-'
    lociLoadStart = '-'
    lociLoadEnd = '-'
    lociLoadTime = '-'
    totalLoci = '-'
    snpInsertStart = '-'
    snpInsertEnd = '-'
    snpInsertTime = '-'
    lociInsertStart = '-'
    lociInsertEnd = '-'
    lociInsertTime = '-'
    mysqlTotalTime = '-'
    documentInsertStart = '-'
    documentInsertEnd = '-'
    documentInsertTime = '-'
    totalDocuments = '-'
    
    idxRsid = '-'
    idxClinSig = '-'
    idxGene = '-'
    
    qryByRsid = '-'
    qryByClinSig = '-'
    qryByGene = '-'
    qryByGeneSig = '-'
    
    def __init__(self):
        return
    
    def headerArr(self):
        return ["Chromosome", "Method", "Tag", 
                         "SNP Load Start", "SNP Load End", "SNP Load Time", 
                         "Loci Load Start", "Loci Load End", "Loci Load Time",
                         "SNP Insert Start", "SNP Insert End", "SNP Insert Time", "Total SNPs",
                         "Loci Insert Start", "Loci Insert End", "Loci Insert Time", "Total Loci",
                         "Total MySQL/pgsql Time",
                         "Document Insert Start", "Document Insert End", "Document Insert Time", "Total Documents",
                         "Idx-RSID", "Idx-ClinSig", "Idx-Gene", 
                         "Qry-RSID", "Qry-ClinSig", "Qry-Gene", "Qry-Gene/Sig"]
    
    def stringArr(self):
        self.calculate()
        return [self.chromosome, self.method, self.tag, 
                                 str(self.snpLoadStart), str(self.snpLoadEnd), str(self.snpLoadTime),
                                 str(self.lociLoadStart), str(self.lociLoadEnd), str(self.lociLoadTime),
                                 str(self.snpInsertStart), str(self.snpInsertEnd), str(self.snpInsertTime), str(self.totalSnps),
                                 str(self.lociInsertStart), str(self.lociInsertEnd), str(self.lociInsertTime), str(self.totalLoci),
                                 str(self.mysqlTotalTime),
                                 str(self.documentInsertStart), str(self.documentInsertEnd), str(self.documentInsertTime), str(self.totalDocuments),
                                 str(self.idxRsid), str(self.idxClinSig), str(self.idxGene),
                                 str(self.qryByRsid), str(self.qryByClinSig), str(self.qryByGene), str(self.qryByGeneSig)]        
    
    def toString(self):
        self.calculate()
        return '\t'.join([self.chromosome, self.method, self.tag, 
                         str(self.snpLoadStart), str(self.snpLoadEnd), str(self.snpLoadTime),
                         str(self.lociLoadStart), str(self.lociLoadEnd), str(self.lociLoadTime),
                         str(self.snpInsertStart), str(self.snpInsertEnd), str(self.snpInsertTime), str(self.totalSnps),
                         str(self.lociInsertStart), str(self.lociInsertEnd), str(self.lociInsertTime), str(self.totalLoci),
                         str(self.mysqlTotalTime),
                         str(self.documentInsertStart), str(self.documentInsertEnd), str(self.documentInsertTime), str(self.totalDocuments),
                         str(self.idxRsid), str(self.idxClinSig), str(self.idxGene),
                         str(self.qryByRsid), str(self.qryByClinSig), str(self.qryByGene), str(self.qryByGeneSig)])
    
    def toHeader(self):
        return '\t'.join(["Chromosome", "Method", "Tag", 
                         "SNP Load Start", "SNP Load End", "SNP Load Time", 
                         "Loci Load Start", "Loci Load End", "Loci Load Time",
                         "SNP Insert Start", "SNP Insert End", "SNP Insert Time", "Total SNPs",
                         "Loci Insert Start", "Loci Insert End", "Loci Insert Time", "Total Loci",
                         "Total MySQL/pgsql Time",
                         "Document Insert Start", "Document Insert End", "Document Insert Time", "Total Documents",
                         "Idx-RSID", "Idx-ClinSig", "Idx-Gene", 
                         "Qry-RSID", "Qry-ClinSig", "Qry-Gene", "Qry-Gene/Sig"])    
    def toTerm(self):
        self.calculate()
        return '\n'.join(["Chromosome: " + str(self.chromosome),
                         "\tMethod: " + str(self.method) + ", Tag: " + str(self.tag),
                         "\tSNP Load Time: " + str(self.snpLoadTime) + 's',
                         "\tLoci Load Time: " + str(self.lociLoadTime) + 's',
                         "\tSNP Insert Time: " + str(self.snpInsertTime) + 's',
                         "\t\tTotal SNPs: " + str(self.totalSnps),
                         "\tLoci Insert Time: " + str(self.lociInsertTime) + 's',
                         "\t\tTotal Loci: " + str(self.totalLoci),
                         "\tMySQL/pgsql Total Time: " + str(self.mysqlTotalTime) + 's',
                         "\tDocument Insert Time: " + str(self.documentInsertTime) + 's',
                         "\t\tTotal Documents: " + str(self.totalDocuments)])
    
    def calculate(self):
        if self.snpLoadEnd != '-':
            self.snpLoadTime = self.snpLoadEnd-self.snpLoadStart
        if self.lociLoadEnd != '-':        
            self.lociLoadTime = self.lociLoadEnd-self.lociLoadStart
        if self.snpInsertEnd != '-':
            self.snpInsertTime = self.snpInsertEnd-self.snpInsertStart
        if self.lociInsertEnd != '-':
            self.lociInsertTime = self.lociInsertEnd-self.lociInsertStart
        if self.lociInsertTime != '-' and self.snpInsertTime != '-':
            self.mysqlTotalTime = self.lociInsertTime+self.snpInsertTime
        if self.documentInsertEnd != '-':
            self.documentInsertTime = self.documentInsertEnd-self.documentInsertStart        