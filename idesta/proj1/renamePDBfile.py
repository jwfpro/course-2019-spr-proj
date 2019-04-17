import urllib.request
import pdb
import json
import dml
import random
import prov.model
import datetime
import uuid

class renamePDBfile(dml.Algorithm):
    contributor = 'idesta'
    reads = ['idesta.bm5', 'idesta.lb_1ahw', 'idesta.lu_1ahw', 'idesta.rb_1ahw', 'idesta.ru_1ahw']
    writes = ['idesta.bnd_pro_1ahw', 'idesta.unbnd_pro_1ahw']
    
    
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
    # ----------------------------------------------------------------------------- Transformation 2 ---------------------------------------------------------------------------#
    # Here are the functions that are needed for the clustering and the projection for transformation 2
    
    def proj_bmmap(d):
        if d['Cat'] == '':
            return None
        else:
            pdbid, chains = d['Complex'].split('_')
            rec_chains, lig_chains = chains.split(':')
            irmsd, del_asa = float(d['I-RMSD (Å)']), float(d['ΔASA(Å2)'])
            cat = d['Cat']
            s = [pdbid, cat, rec_chains, lig_chains, irmsd, del_asa]
            return s
    
    # transformation #2
    def get_map(tablebm5):
        # use k-means to get a pdbid to cluster the 230 proteins into 3 classes (potentially difficult, medium and easy)
        project = renamePDBfile.project
        select = renamePDBfile.select
        bm_map = project(tablebm5, renamePDBfile.proj_bmmap)
        bmmap = select(bm_map, lambda t : type(t) == list)
        return bmmap


    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
    # ----------------------------------------------------------------------------- Transformation 5 ---------------------------------------------------------------------------#
    def rename_pdbfile(recjson, ligjson, bm5map):
        # bm5map is the map that provides chains of bound proteins from zping's bm5 data set

        project = renamePDBfile.project
        select = renamePDBfile.select
        union = renamePDBfile.union

        rec_chains, lig_chains = [(c,d) for [a,b,c,d,e,f] in select(bm5map, lambda t: t[0]=='1AHW')][0]
        target_rec = list({d['chainID'] for d in recjson})
        target_lig = list({d['chainID'] for d in ligjson})
        rec_true, lig_true = list(rec_chains), list(lig_chains)
        #pdb.set_trace()
        if len(rec_true) != len(target_rec) or len(lig_true) != len(target_lig):
            return 'the length of the receptor and ligand chains don\'t match'
        else:
            for d in recjson:
                ind = target_rec.index(d['chainID'])
                d['chainID'] = rec_true[ind]
            for d in ligjson:
                ind = target_lig.index(d['chainID'])
                d['chainID'] = lig_true[ind]
            prot = union(recjson, ligjson)
            return prot


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
        
        bm5 = [i for i in repo['idesta.bm5'].find()]
        bm5map = renamePDBfile.get_map(bm5)

        lb_1ahw = [i for i in repo['idesta.lb_1ahw'].find()]
        lu_1ahw = [i for i in repo['idesta.lu_1ahw'].find()]
        rb_1ahw = [i for i in repo['idesta.rb_1ahw'].find()]
        ru_1ahw = [i for i in repo['idesta.ru_1ahw'].find()]
        bnd_1ahw_pro = renamePDBfile.rename_pdbfile(rb_1ahw, lb_1ahw, bm5map)
        unbnd_1ahw_pro = renamePDBfile.rename_pdbfile(ru_1ahw, lu_1ahw, bm5map)
        #pdb.set_trace()
        
        repo.dropPermanent('bnd_pro_1ahw')
        repo.createPermanent('bnd_pro_1ahw')
        repo['idesta.bnd_pro_1ahw'].insert_many(bnd_1ahw_pro)
        
        repo.dropPermanent('unbnd_pro_1ahw')
        repo.createPermanent('unbnd_pro_1ahw')
        repo['idesta.unbnd_pro_1ahw'].insert_many(unbnd_1ahw_pro)
        
        
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

        this_script = doc.agent('alg:idesta#renamePDBfile', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})


    #reads = ['idesta.bm5', 'idesta.lb_1ahw', 'idesta.lu_1ahw', 'idesta.rb_1ahw', 'idesta.ru_1ahw']
    #writes = ['idesta.bnd_pro_1ahw', 'idesta.unbnd_pro_1ahw']
        bm5 = doc.entity('dat:idesta#bm5', {prov.model.PROV_LABEL:'zping benchmark5', prov.model.PROV_TYPE:'ont:DataSet'})
        get_bm5 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get benchmark5'})
        doc.wasAssociatedWith(get_bm5, this_script)
        doc.used(get_bm5, bm5, startTime)
        doc.wasAttributedTo(bm5, this_script)
        doc.wasGeneratedBy(bm5, get_bm5, endTime)

        lb_1ahw = doc.entity('dat:idesta#lb_1ahw', {prov.model.PROV_LABEL:'bound 1ahw ligand', prov.model.PROV_TYPE:'ont:DataSet'})
        get_lb_1ahw = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get bnd 1ahw lig'})
        doc.wasAssociatedWith(get_lb_1ahw, this_script)
        doc.used(get_lb_1ahw, lb_1ahw, startTime)
        doc.wasAttributedTo(lb_1ahw, this_script)
        doc.wasGeneratedBy(lb_1ahw, get_lb_1ahw, endTime)

        lu_1ahw = doc.entity('dat:idesta#lu_1ahw', {prov.model.PROV_LABEL:'unbound 1ahw ligand', prov.model.PROV_TYPE:'ont:DataSet'})
        get_lu_1ahw = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get unbnd 1ahw lig'})
        doc.wasAssociatedWith(get_lu_1ahw, this_script)
        doc.used(get_lu_1ahw, lu_1ahw, startTime)
        doc.wasAttributedTo(lu_1ahw, this_script)
        doc.wasGeneratedBy(lu_1ahw, get_lu_1ahw, endTime)
        
        rb_1ahw = doc.entity('dat:idesta#rb_1ahw', {prov.model.PROV_LABEL:'bound 1ahw rec', prov.model.PROV_TYPE:'ont:DataSet'})
        get_rb_1ahw = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get bnd 1ahw rec'})
        doc.wasAssociatedWith(get_rb_1ahw, this_script)
        doc.used(get_rb_1ahw, rb_1ahw, startTime)
        doc.wasAttributedTo(rb_1ahw, this_script)
        doc.wasGeneratedBy(rb_1ahw, get_rb_1ahw, endTime)
        
        ru_1ahw = doc.entity('dat:idesta#ru_1ahw', {prov.model.PROV_LABEL:'unbound 1ahw rec', prov.model.PROV_TYPE:'ont:DataSet'})
        get_ru_1ahw = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get unbnd 1ahw rec'})
        doc.wasAssociatedWith(get_ru_1ahw, this_script)
        doc.used(get_ru_1ahw, ru_1ahw, startTime)
        doc.wasAttributedTo(ru_1ahw, this_script)
        doc.wasGeneratedBy(ru_1ahw, get_ru_1ahw, endTime)
        
        bnd_1ahw = doc.entity('dat:idesta#bnd_pro_1ahw', {prov.model.PROV_LABEL:'bound 1ahw', prov.model.PROV_TYPE:'ont:DataSet'})
        get_bnd_1ahw = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'rename and unite bnd 1ahw rec and lig'})
        doc.wasAssociatedWith(get_bnd_1ahw, this_script)
        doc.used(get_bnd_1ahw, bnd_1ahw, startTime)
        doc.wasAttributedTo(bnd_1ahw, this_script)
        doc.wasGeneratedBy(bnd_1ahw, get_bnd_1ahw, endTime)
        doc.wasDerivedFrom(bnd_1ahw, lb_1ahw, get_bnd_1ahw, get_bnd_1ahw, get_bnd_1ahw)
        doc.wasDerivedFrom(bnd_1ahw, rb_1ahw, get_bnd_1ahw, get_bnd_1ahw, get_bnd_1ahw)
        
        unbnd_1ahw = doc.entity('dat:idesta#unbnd_pro_1ahw', {prov.model.PROV_LABEL:'unbound 1ahw', prov.model.PROV_TYPE:'ont:DataSet'})
        get_unbnd_1ahw = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'rename and unite unbnd 1ahw rec and lig'})
        doc.wasAssociatedWith(get_unbnd_1ahw, this_script)
        doc.used(get_unbnd_1ahw, unbnd_1ahw, startTime)
        doc.wasAttributedTo(unbnd_1ahw, this_script)
        doc.wasGeneratedBy(unbnd_1ahw, get_unbnd_1ahw, endTime)
        doc.wasDerivedFrom(unbnd_1ahw, lu_1ahw, get_unbnd_1ahw, get_unbnd_1ahw, get_unbnd_1ahw)
        doc.wasDerivedFrom(unbnd_1ahw, ru_1ahw, get_unbnd_1ahw, get_unbnd_1ahw, get_unbnd_1ahw)
        
        #repo.record(doc.serialize())
        repo.logout()
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
renamePDBfile.execute()
doc = renamePDBfile.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
