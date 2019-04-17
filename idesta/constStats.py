import urllib.request
import pdb
import json
import dml
import random
from random import shuffle
import prov.model
import datetime
import uuid
import numpy as np
from z3 import *

class constStats(dml.Algorithm):
    contributor = 'idesta'
    reads = ['idesta.dockq_res']
    writes = ['idesta.dockq_constats']

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
    
    def permute(x):
        shuffled = [xi for xi in x]
        shuffle(shuffled)
        return shuffled

    def cov(x,y):
        return sum([(xi-np.average(x))*(yi-np.average(y)) for (xi,yi) in zip(x,y)])/len(x)

    def corr(x,y):
        if np.std(x) * np.std(y) != 0:
            return constStats.cov(x,y)/(np.std(x) * np.std(y))

    def optimizeIJ(entries):
        fs = [i['vs'][0] for i in entries]
        irs = [i['vs'][1] for i in entries]
        lrs = [i['vs'][2] for i in entries]
        ds = [i['vs'][3] for i in entries]
        num_i = len(fs)
        i = Real('i')
        j = Real('j')
        S = Solver()
        set_option(rational_to_decimal=True)
        # fs, irs, lrs and ds are all entries fnat, irmsd, lrmsd and dockq respectively
        # i and j are the two parameters that I want to optimize
        # the constriaints are as follows
        #for (i, j, f, ir, lr, d) in zip(vars_i, vars_j, fs, irs, lrs, ds):
        S.add(i>0.5, i<5)
        S.add(j>0.5, j<10)
        for (f, ir, lr, d) in zip(fs, irs, lrs, ds):
            S.add(d - 0.01 <= (f + ((1 + (ir/i)**2)**(-1)) + ((1 + (lr/j)**2)**(-1)))/3, (f + ((1 + (ir/i)**2)**(-1)) + ((1 + (lr/j)**2)**(-1)))/3  <= d + 0.01)

        print(S.check())
        if str(S.check())=='unsat':
            return None
        else:
            return S.model()

    def pval_scores(entries, trial):
        fs = [i['vs'][0] for i in entries]
        irs = [i['vs'][1] for i in entries]
        lrs = [i['vs'][2] for i in entries]
        ds = [i['vs'][3] for i in entries]

        scores_names = ['fnat', 'irmsd', 'lrmsd', 'dockq']
        all_names_prod = constStats.product(scores_names, scores_names)
        names_prod = constStats.select(all_names_prod, lambda t: t[0] != t[1])
        scores_tup = (fs, irs, lrs, ds)
        scores_prod = constStats.product(scores_tup, scores_tup)
        index, pvals = -1, {}
        for i in scores_prod:
            if i[0] != i[1]:
                index += 1
                corr_name = "_".join(names_prod[index])
                c0 = constStats.corr(i[0], i[1])
                corrs = []
                if trial:
                    NUM = 50
                else:
                    NUM = 2000
                for k in range(NUM):
                    y_permuted = constStats.permute(i[1])
                    corrs.append(constStats.corr(i[0], y_permuted))
                pval = len([c for c in corrs if abs(c) >= abs(c0)])/len(corrs)
                pvals[corr_name] = pval

        return pvals



    

    @staticmethod
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
    # --------------------------------------------------------------------------Retrive data from MONGODB ----------------------------------------------------------------------#
    # retrieve data sets from local MONGODB
    def execute(trial=False):
        '''Retrieve some data sets from Mongo for transformation'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('idesta', 'idesta')

        abag_dockq = [i for i in repo['idesta.dockq_res'].find()]
        if trial:
            model = constStats.optimizeIJ(abag_dockq[:300])
            stats = constStats.pval_scores(abag_dockq[:300], trial)
            #pdb.set_trace()
        else:
            model = constStats.optimizeIJ(abag_dockq)
            stats = constStats.pval_scores(abag_dockq, trial)

        #pdb.set_trace()
        model_dict = {'i': str(model).split(",")[0].split("= ")[1], 'j': str(model).split(",")[1].split("= ")[1].strip("]")}
        repo.dropPermanent('dockq_constats')
        repo.createPermanent('dockq_constats')
        repo['idesta.dockq_constats'].insert_many([model_dict, stats])

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


        this_script = doc.agent('alg:idesta#constStats', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        dockq = doc.entity('dat:idesta#dockq_res', {prov.model.PROV_LABEL:'evaluation results file of all proteins', prov.model.PROV_TYPE:'ont:DataSet'})
        get_dockqres = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'obtain the dockq res data from repo'})
        doc.wasAssociatedWith(get_dockqres, this_script)
        #doc.used(get_dockqres, dockq, startTime)
        doc.usage(get_dockqres, dockq, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        

        constats = doc.entity('dat:idesta#dockq_constats', {prov.model.PROV_LABEL:'i and j vars satisfying the constraint and corr_coefs of fnat, irms and lrms with dockq', prov.model.PROV_TYPE:'ont:DataSet'})
        get_constats = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get vals that sat the constraints and corr_coefs of fnat, irms, lrms with dockq'})
        doc.wasAssociatedWith(get_constats, this_script)
        #doc.used(get_constats, constats, startTime)
        doc.usage(get_constats, constats, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'
                  }
                  )
        doc.wasAttributedTo(constats, this_script)
        doc.wasGeneratedBy(constats, get_constats, endTime)
        doc.wasDerivedFrom(dockq, constats, get_constats, get_constats, get_constats)
 
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
constStats.execute()
doc = constStats.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
