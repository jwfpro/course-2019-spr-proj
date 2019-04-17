import urllib.request
import pdb
import json
import dml
import random
import prov.model
import datetime
import uuid

class clusterBM5(dml.Algorithm):
    contributor = 'idesta'
    reads = ['idesta.bm5']
    writes = ['idesta.bm5_diff_clusters', 'idesta.bm5map']

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

    def dist(p,q):
        (x1,y1), (x2, y2) = p, q
        return (x1-x2)**2 + (y1-y2)**2

    def plus(args):
        p = [0,0]
        for (x,y) in args:
            p[0] += x
            p[1] += y
        return tuple(p)

    def scale(p,c):
        (x,y) = p
        return (x/c, y/c)

    
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
    def clus_diff(tablebm5):
        # use k-means to get a pdbid to cluster the 230 proteins into 3 classes (potentially difficult, medium and easy)
        project = clusterBM5.project
        select = clusterBM5.select
        aggregate = clusterBM5.aggregate
        product = clusterBM5.product
        proj_bmmap, dist, scale, plus = clusterBM5.proj_bmmap, clusterBM5.dist, clusterBM5.scale, clusterBM5.plus

        bm_map = project(tablebm5, proj_bmmap)
        bmmap = select(bm_map, lambda t : type(t) == list)
        P = project(bmmap, lambda t: (t[-2], t[-1]))
        M = random.sample(P,3)

        old = []
        while old != M:
            old = M

            MPD = [(m, p, dist(m,p)) for (m, p) in product(M, P)]
            PDs = [(p, dist(m,p)) for (m, p, d) in MPD]
            PD = aggregate(PDs, min)
            MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
            MT = aggregate(MP, plus)

            M1 = [(m, 1) for (m, _) in MP]
            MC = aggregate(M1, sum)

            M = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]

        MP_dict = {}
        for ind,k in enumerate(M): MP_dict['key {}'.format(ind)] = []
        for (k,v) in MP:
            ind = M.index(k)
            MP_dict['key {}'.format(ind)].append(v)
        MP_json = [MP_dict]
        bm5map_dict = {}
        for i in bmmap:
            bm5map_dict[i[0]] = i[1:]
        bm5map_json = [bm5map_dict]


        return MP_json, bm5map_json


    #-------------------------------------------------------------------------------#
    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets from Mongo for transformation'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('idesta', 'idesta')

        bm5 = [i for i in repo['idesta.bm5'].find()]
        clus_results, bmmap = clusterBM5.clus_diff(bm5)
        
        repo.dropPermanent('bm5_diff_clusters')
        repo.createPermanent('bm5_diff_clusters')
        repo['idesta.bm5_diff_clusters'].insert_many(clus_results)

        repo.dropPermanent('bm5map')
        repo.createPermanent('bm5map')
        repo['idesta.bm5map'].insert_many(bmmap)

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


        this_script = doc.agent('alg:idesta#clusterBM5', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        bm5 = doc.entity('dat:idesta#bm5', {prov.model.PROV_LABEL:'Zpig benchmark 5', prov.model.PROV_TYPE:'ont:DataSet'})
        get_bm5 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get the 230 BM5 complexes'})
        doc.wasAssociatedWith(get_bm5, this_script)
        doc.used(get_bm5, bm5, startTime)
        doc.wasAttributedTo(bm5, this_script)
        doc.wasGeneratedBy(bm5, get_bm5, endTime)
        

        bm5map = doc.entity('dat:idesta#bm5map', {prov.model.PROV_LABEL:'benchmark 5 and chains', prov.model.PROV_TYPE:'ont:DataSet'})
        get_bm5map = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get the BM5 proteins and chains map'})
        doc.wasAssociatedWith(get_bm5map, this_script)
        doc.used(get_bm5map, bm5map, startTime)
        doc.wasAttributedTo(bm5map, this_script)
        doc.wasGeneratedBy(bm5map, get_bm5map, endTime)
        doc.wasDerivedFrom(bm5map, bm5, get_bm5map, get_bm5map, get_bm5map)


        bm5clus = doc.entity('dat:idesta#bm5_diff_clusters', {prov.model.PROV_LABEL:'benchmark 5 clusters by ASA and iRMSD', prov.model.PROV_TYPE:'ont:DataSet'})
        get_bm5clus = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'cluster the proteins by ASA and iRMSD'})
        doc.wasAssociatedWith(get_bm5clus, this_script)
        doc.used(get_bm5clus, bm5clus, startTime)
        doc.wasAttributedTo(bm5clus, this_script)
        doc.wasGeneratedBy(bm5clus, get_bm5clus, endTime)
        doc.wasDerivedFrom(bm5clus, bm5, get_bm5clus, get_bm5clus, get_bm5clus)
        
        #repo.record(doc.serialize())
        repo.logout()
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
clusterBM5.execute()
doc = clusterBM5.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
