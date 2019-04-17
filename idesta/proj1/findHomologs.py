import urllib.request
import pdb
import json
import dml
import random
import prov.model
import datetime
import uuid

class findHomologs(dml.Algorithm):
    contributor = 'idesta'
    reads = ['idesta.pdb70']
    writes = ['idesta.1ahw_homologs']

    
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
    # ----------------------------------------------------------------------------- Transformation 1 ---------------------------------------------------------------------------#
    def get_kv(dictionary):
        p, vs, keys, f = (), (), [], {}
        key = 'structureId'
        vs_list = ['chainId', 'chainLength', 'clusterNumber70', 'sequence', 'classification', 'macromoleculeType', 'resolution', 'taxonomy']
        f[dictionary[key]] = {}
        for val in vs_list:
            f[dictionary[key]][val] = dictionary[val]
        return f

    def selector(d):
        ans = True
        clus_nums = [1,2,1434]
        if d['clusterNumber70'] == '': 
            ans = False
        elif int(d['clusterNumber70']) not in clus_nums: 
            ans = False
        if d['resolution'] == '':
            ans = False
        elif float(d['resolution']) > 3.0:
            ans = False
        return ans
    
    def get_homologs(rcsb_data):
        # use selection and projection by chain to find sequence homologs of 1ahw
        sel = findHomologs.select(rcsb_data, findHomologs.selector)
        pr = findHomologs.project(sel, findHomologs.get_kv)

        return pr


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('idesta', 'idesta')

        pdb70 = [i for i in repo['idesta.pdb70'].find()]
        results = findHomologs.get_homologs(pdb70)
 
        repo.dropPermanent('1ahw_homologs')
        repo.createPermanent('1ahw_homologs')
        repo['idesta.1ahw_homologs'].insert_many(results)

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

        this_script = doc.agent('alg:idesta#findHomologs', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        pdb70 = doc.entity('dat:idesta#pdb70', {prov.model.PROV_LABEL:'pdb70', prov.model.PROV_TYPE:'ont:DataSet'})
        get_pdb70 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get the pdb70 set from local DB'})
        doc.wasAssociatedWith(get_pdb70, this_script)
        doc.used(get_pdb70, pdb70, startTime)
        doc.wasAttributedTo(pdb70, this_script)
        doc.wasGeneratedBy(pdb70, get_pdb70, endTime)


        homologs = doc.entity('dat:idesta#1ahw_homologs', {prov.model.PROV_LABEL:'1ahw homologs', prov.model.PROV_TYPE:'ont:DataSet'})
        get_homologs = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get homologs for 1ahw'})
        doc.wasAssociatedWith(get_homologs, this_script)
        doc.used(get_homologs, homologs, startTime)
        doc.wasAttributedTo(homologs, this_script)
        doc.wasGeneratedBy(homologs, get_homologs, endTime)
        doc.wasDerivedFrom(homologs, pdb70, get_homologs, get_homologs, get_homologs)
        
        #repo.record(doc.serialize())
        repo.logout()
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
findHomologs.execute()
doc = findHomologs.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
