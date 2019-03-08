import urllib.request
import pdb
import json
import dml
import random
import prov.model
import datetime
import uuid

class countClusterMembers(dml.Algorithm):
    contributor = 'idesta'
    reads = ['idesta.1ahw_clus']
    writes = ['idesta.1ahw_clus_count']

    
    
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
    # --------------------------------------------------------------------------Relational building blocks ---------------------------------------------------------------------#
    def select(R,s):
        return [t for t in R if s(t)]

    def project(R,s):
        return [s(t) for t in R]

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]

    def union(R,S):
        return R + S

    def product(R,S):
        return [(t,u) for t in R for u in S]


    
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
    # ----------------------------------------------------------------------------- Transformation 3 ---------------------------------------------------------------------------#
    # projector function for the counting transformation on the cluster file
    # it creates a list of tuples with the first element being the cluster centers, 
    # and the second element being the member
    def proj_ct(d):
        s = []
        for i in range(len(d)-1):
            if d[str(i)].startswith('Radius'): continue
            elif d[str(i)].startswith('Center'): curr_center = d[str(i)]
            else: s.append((curr_center, d[str(i)]))
        return s

    # Actual transformation function that counts the number of members in each cluster of the given file
    def ct_mem(clus_1ahw):
        # project and aggregate the cluster file of 1ahw
        project = countClusterMembers.project
        aggregate = countClusterMembers.aggregate

        mem_tup = project(clus_1ahw, countClusterMembers.proj_ct)
        mem_1 = [project(prot, lambda t: (t[0], 1)) for prot in mem_tup]
        mem_ct = [aggregate(prot, sum) for prot in mem_1]
        for clus_file in mem_ct:
            mem_ct_json = [{k:v} for (k,v) in clus_file]
        return mem_ct_json


    @staticmethod
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
    # --------------------------------------------------------------------------Retrive data from MONGODB ----------------------------------------------------------------------#
    # retrieve data sets from local MONGODB
    def execute(trial = False):
        '''Retrieve some data sets from Mongo for transformation'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('idesta', 'idesta')

        clus_1ahw = [i for i in repo['idesta.1ahw_clus'].find()]
        count_mem = countClusterMembers.ct_mem(clus_1ahw)
        #pdb.set_trace()

        repo.dropPermanent('1ahw_clus_count')
        repo.createPermanent('1ahw_clus_count')
        repo['idesta.1ahw_clus_count'].insert_many(count_mem)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('idesta', 'idesta')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.


        this_script = doc.agent('alg:idesta#countClusterMembers', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        clus = doc.entity('dat:idesta#1ahw_clus', {prov.model.PROV_LABEL:'cluster file for 1AHW', prov.model.PROV_TYPE:'ont:DataSet'})
        get_clus = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get the cluster file for 1AHW'})
        doc.wasAssociatedWith(get_clus, this_script)
        doc.used(get_clus, clus, startTime)
        doc.wasAttributedTo(clus, this_script)
        doc.wasGeneratedBy(clus, get_clus, endTime)
        

        cluscount = doc.entity('dat:idesta#1ahw_clus_count', {prov.model.PROV_LABEL:'count of cluster members for 1AHW docking', prov.model.PROV_TYPE:'ont:DataSet'})
        get_cluscount = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'count members of each cluster'})
        doc.wasAssociatedWith(get_cluscount, this_script)
        doc.used(get_cluscount, cluscount, startTime)
        doc.wasAttributedTo(cluscount, this_script)
        doc.wasGeneratedBy(cluscount, get_cluscount, endTime)
        doc.wasDerivedFrom(cluscount, clus, get_cluscount, get_cluscount, get_cluscount)
 
        #repo.record(doc.serialize())
        repo.logout()
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
countClusterMembers.execute()
doc = countClusterMembers.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
