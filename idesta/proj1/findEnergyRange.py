import urllib.request
import pdb
import json
import dml
import random
import prov.model
import datetime
import uuid

class findEnergyRange(dml.Algorithm):
    contributor = 'idesta'
    reads = ['idesta.1ahw_ft']
    writes = ['idesta.1ahw_ft_ranges']
    
    
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
    # ----------------------------------------------------------------------------- Transformation 4 ---------------------------------------------------------------------------#
    # projector function for the ft file transformation
    # this function creates a tuple with the dictionary key as the first element
    # and the values of those keys as the second element
    # note that the ones I am projecting are only the energy values
    def proj_ft(d):
        en = []
        title_list = ['rot_ind', 'x-coor', 'y-coor', 'z-coor', 'energy_tot', 'vdw_rep', 'vdw_att', 'estat', 'born', 'dars']
        for key in d:
            if key != '_id':
                for ind,t in enumerate(title_list[4:]):
                    en.append((t,float(d[key][ind+3])))
        return en 


    # Actual transformation function that ounts the number of members in each cluster of the given file
    def range_ft(ft_1ahw):
        project = findEnergyRange.project
        aggregate = findEnergyRange.aggregate
        union = findEnergyRange.union
        
        #pdb.set_trace()
        energies = project(ft_1ahw, findEnergyRange.proj_ft)[0]
        MIN = aggregate(energies, min)
        MAX = aggregate(energies, max)
        MIN_NEG = project(MIN, lambda t: (t[0], -1*(t[1])))
        RESULT = aggregate(union(MIN_NEG, MAX), sum)
        res_json = [{k:v} for (k,v) in RESULT]
        return res_json


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

        ft_1ahw = [i for i in repo['idesta.1ahw_ft'].find()]
        energy_ranges = findEnergyRange.range_ft(ft_1ahw)
        #pdb.set_trace()
        
        repo.dropPermanent('1ahw_ft_ranges')
        repo.createPermanent('1ahw_ft_ranges')
        repo['idesta.1ahw_ft_ranges'].insert_many(energy_ranges)

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

        this_script = doc.agent('alg:idesta#findEnergyRange', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


        ft = doc.entity('dat:idesta#1ahw_ft', {prov.model.PROV_LABEL:'ftfile for 1ahw', prov.model.PROV_TYPE:'ont:DataSet'})
        get_ft = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get the ft file from local DB'})
        doc.wasAssociatedWith(get_ft, this_script)
        doc.used(get_ft, ft, startTime)
        doc.wasAttributedTo(ft, this_script)
        doc.wasGeneratedBy(ft, get_ft, endTime)


        ranges = doc.entity('dat:idesta#1ahw_ft_ranges', {prov.model.PROV_LABEL:'1ahw energy ranges', prov.model.PROV_TYPE:'ont:DataSet'})
        get_ranges = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'energy ranges of 1ahw ft'})
        doc.wasAssociatedWith(get_ranges, this_script)
        doc.used(get_ranges, ranges, startTime)
        doc.wasAttributedTo(ranges, this_script)
        doc.wasGeneratedBy(ranges, get_ranges, endTime)
        doc.wasDerivedFrom(ranges, ft, get_ranges, get_ranges, get_ranges)
        
        #repo.record(doc.serialize())
        repo.logout()
                  
        return doc


'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
findEnergyRange.execute()
doc = findEnergyRange.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
