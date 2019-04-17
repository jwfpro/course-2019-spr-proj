import urllib.request
import pdb
import json
import dml
from subprocess import check_output, check_call
import pandas as pd
import requests
import xlrd
import prov.model
import datetime
import uuid

class loadData2(dml.Algorithm):
    contributor = 'idesta'
    reads = []
    writes = ['idesta.dockq_res']

    @staticmethod
    def execute(trial = False):
        # Retrieve some data sets.
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('idesta', 'idesta')
        
         
        # data set: text file outlining the 4 different scores for each case
        # the text file has an identifier, and 3 scores that somehow combine to get the 4th score
        url = 'http://datamechanics.io/data/idesta/newpip_oldgrp_iter1_4eigs_dockqres'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        scorelist = response.strip().split('\n')
        #pdb.set_trace()
        
        coll = []
        for line in scorelist:
            if line.startswith("JOBNUM"):
                continue
            else:
                info = tuple(line.strip().split(","))
                coll.append(info)

        #entries = project(coll, lambda t: {(t[0], int(t[2]), int(t[3]), int(t[4])): [float(t[5]), float(t[6]), float(t[7]), float(t[8])]})
        #entries = [{("_").join([t[0], t[2], t[3], t[4]]): [float(t[5]), float(t[6]), float(t[7]), float(t[8])]} for t in coll]
        entries = [{"_id": ("_").join([t[0], t[2], t[3], t[4]]), "vs": [float(t[5]), float(t[6]), float(t[7]), float(t[8])]} for t in coll]
        repo.dropPermanent("dockq_res")
        repo.createPermanent("dockq_res")
        repo["idesta.dockq_res"].insert_many(entries)
         
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

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') 
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('dtm', 'http://datamechanics.io/data/')

        this_script = doc.agent('alg:idesta#loadData_2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_dtm = doc.entity('dtm:idesta', {'prov:label':'DockQ program output: evaluation scores', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txt'})
        get_dockq = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get the 4 eval scores from dockq result file'})
        doc.wasAssociatedWith(get_dockq, this_script)
        
        doc.usage(get_dockq, resource_dtm, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'newpip_oldgrp_iter1_4eigs_dockqres'
                  }
                  )


        dockq = doc.entity('dat:idesta#dockq_res', {prov.model.PROV_LABEL:'evaluation scores of antibody-antigen compelxes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(dockq, this_script)
        doc.wasGeneratedBy(dockq, get_dockq, endTime)
        doc.wasDerivedFrom(resource_dtm, dockq, get_dockq, get_dockq, get_dockq)
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
loadData2.execute()
doc = loadData2.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
