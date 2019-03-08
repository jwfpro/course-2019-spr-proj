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

class loadData(dml.Algorithm):
    contributor = 'idesta'
    reads = []
    writes = ['idesta.pdb70', 'idesta.bm5', 'idesta.lb_1ahw', 'idesta.lu_1ahw', 'idesta.rb_1ahw', 'idesta.ru_1ahw', 'idesta.1ahw_clus', 'idesta.1ahw_ft']

    @staticmethod
    def execute(trial = False):
        # Retrieve some data sets.
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('idesta', 'idesta')
        
        
        # data set 1: RCSB PDB data of all structures
        # for this data set, we will filter(selection) for ones that are in the same cluster as 1AHW and project it in a usable form
        url = 'http://www.rcsb.org/pdb/rest/customReport.xml?pdbids=*&customReportColumns=chainLength,clusterNumber70,sequence,taxonomy,resolution,classification,,macromoleculeType&service=wsfile&format=csv'
        response = urllib.request.urlopen(url).read().decode("utf-8")

        # clean up data
        resp_list = response.replace('"','').strip().split("\n")
        title_list = resp_list[0].split(",")
        resp_json = []
        for item in resp_list[1:]:
            structure = item.split(",")
            str_dict, cln_structure = {}, []
            for i in structure: cln_structure.append(i)
            if len(structure) > len(title_list):
                classification = (" ").join(item.split(",")[7:-1])
                del cln_structure[7:-1]
                cln_structure.insert(-1,classification)
            for ind,obj in enumerate(cln_structure):
                str_dict[title_list[ind]] = obj
            resp_json.append(str_dict)
        rcsb_pdb = json.dumps(resp_json,sort_keys=True, indent=2)
        rcsb = json.loads(rcsb_pdb)
        repo.dropPermanent("pdb70")
        repo.createPermanent("pdb70")
        repo['idesta.pdb70'].insert_many(rcsb)
        

        # data set 2: Zhiping's BM5 excel data set
        # for this data set, I will be projecting to a text file that will be used for transforming data set 3 (might do aggregation as well by class)
        url = 'https://zlab.umassmed.edu/benchmark/Table_BM5.xlsx'
        tablebm5 = requests.get(url)
        workbook = xlrd.open_workbook(file_contents=tablebm5.content)
        worksheet = workbook.sheet_by_index(0)
        title_list = [item.value.strip('.') for item in worksheet.row(2)]
        bm5json = []
        for i in range(4,worksheet.nrows):
            row = worksheet.row(i)
            curr_dict = {}
            for ind,item in enumerate(row):
                curr_dict[title_list[ind]] = item.value
            bm5json.append(curr_dict)
        repo.dropPermanent("bm5")
        repo.createPermanent("bm5")
        repo["idesta.bm5"].insert_many(bm5json)

        # data set 3: PDB files from Zping's BM
        # This will be selection and projection to substitute name
        dict_1ahw = {'lb':'http://datamechanics.io/data/idesta/1AHW_l_b.pdb','lu':'http://datamechanics.io/data/idesta/1AHW_l_u.pdb','rb':'http://datamechanics.io/data/idesta/1AHW_r_b.pdb','ru':'http://datamechanics.io/data/idesta/1AHW_r_u.pdb'}
        pdbFields = {'atom':(1,6), 'atom_ser_num':(7,11), 'atom_name':(13,16), 'altLoc':(17,17), 'resName':(18,20), 'chainID':(22,22), 'resNum':(23,26), 'ins_code':(27,27), 'x_coor':(31,38), 'y_coor':(39,46), 'z_coor':(47,54), 'occup':(55,60), 'temp_factor':(61,66), 'element':(77,78), 'charge':(79,80)}
        
        for key in dict_1ahw:
            #print (key)
            response = urllib.request.urlopen(dict_1ahw[key]).read().decode("utf-8")
            resp_list = response.strip().split("\n")
            resp_json = []
            for atom in resp_list:
                if atom.startswith('ATOM'):
                    atom_dict = {}
                    for field in pdbFields:
                        ran = pdbFields[field]
                        atom_dict[field] = atom[ran[0]-1:ran[1]]
                    resp_json.append(atom_dict)
            dump_1ahw = json.dumps(resp_json,sort_keys=True, indent=2)
            json_1ahw = json.loads(dump_1ahw)
            #pdb.set_trace()
            repo.dropPermanent(key+'_1ahw') 
            repo.createPermanent(key+'_1ahw')
            repo['idesta.{}'.format(key+'_1ahw')].insert_many(json_1ahw)

        
        # data set 4: clusterfile with clusters and members for 1AHW docking
        # here I will do projection and aggregation to count members of each cluster
        url = 'http://datamechanics.io/data/idesta/clustermat.000.00.clusters'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        cluslist = response.strip().split('\n')
        clus_dict = {}
        for i in range(len(cluslist)):
            clus_dict[str(i)] = cluslist[i]
        repo.dropPermanent("1ahw_clus")
        repo.createPermanent("1ahw_clus")
        repo["idesta.1ahw_clus"].insert_many([clus_dict])
        
        
        # data set 5: ft file after docking 1AHW
        # here I will be calculating the ranges of the five energy potentials 1AHW's ft file (maybe top1000 range)
        url = 'http://datamechanics.io/data/idesta/ft.000.00'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        ftlist = response.strip().split('\n')
        ftjson, curr_dict = [], {}
        for i in ftlist:
            info = i.split()
            curr_dict[info[0]] = []
            for item in info[1:]: curr_dict[info[0]].append(item)
        ftjson.append(curr_dict)
        repo.dropPermanent("1ahw_ft")
        repo.createPermanent("1ahw_ft")
        repo["idesta.1ahw_ft"].insert_many(ftjson)
        
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

        doc.add_namespace('rcsb', 'http://www.rcsb.org/pdb/rest/')
        doc.add_namespace('dtm', 'http://datamechanics.io/data/')
        doc.add_namespace('zpg', 'https://zlab.umassmed.edu/')

        this_script = doc.agent('alg:idesta#loadData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_rcsb = doc.entity('rcsb:customReport', {'prov:label':'All proteins in PDB in clusterNumber70', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_rcsb = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get proteins in clusterNumber70'})
        doc.wasAssociatedWith(get_rcsb, this_script)
        doc.usage(get_rcsb, resource_rcsb, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?pdbids=*&customReportColumns=chainLength,clusterNumber70,sequence,taxonomy,experimentalTechnique,resolution,classification,,macromoleculeType&service=wsfile&format=csv'
                  }
                  )

        resource_wengLab = doc.entity('zpg:benchmark', {'prov:label':'Zping protein complex benchmark5', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'xlsx'})
        get_wengLab = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Obtain protein complex BM5 details'})
        doc.wasAssociatedWith(get_wengLab, this_script)
        doc.usage(get_wengLab, resource_wengLab, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'Table_BM5.xlsx'
                  }
                  )


        resource_dtm = doc.entity('dtm:idesta', {'prov:label':'PDB files and ClusPro outputs', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txt'})
        get_ft = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get the ft files from ClusPro docking of 1AHW'})
        get_clus = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get the cluster file from docking 1AHW with ClusPro'})
        get_lbpdb = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get the bound ligand pdb file'})
        get_lupdb = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get the unbound ligand pdb file'})
        get_rbpdb = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get the bound receptor pdb file'})
        get_rupdb = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get the unbound receptor pdb file'})
        doc.wasAssociatedWith(get_ft, this_script)
        doc.wasAssociatedWith(get_clus, this_script)
        doc.wasAssociatedWith(get_lbpdb, this_script)
        doc.wasAssociatedWith(get_lupdb, this_script)
        doc.wasAssociatedWith(get_rbpdb, this_script)
        doc.wasAssociatedWith(get_rupdb, this_script)
        doc.usage(get_ft, resource_dtm, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'ft.000.00'
                  }
                  )
        doc.usage(get_clus, resource_dtm, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'clustermat.000.00.clusters'
                  }
                  )
        doc.usage(get_lbpdb, resource_dtm, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'1AHW_l_b.pdb'
                  }
                  )
        doc.usage(get_lupdb, resource_dtm, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'1AHW_l_u.pdb'
                  }
                  )
        doc.usage(get_rbpdb, resource_dtm, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'1AHW_r_b.pdb'
                  }
                  )
        doc.usage(get_rupdb, resource_dtm, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'1AHW_r_u.pdb'
                  }
                  )

        pdb70 = doc.entity('dat:idesta#pdb70', {prov.model.PROV_LABEL:'clusterNumber70 of PDB', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(pdb70, this_script)
        doc.wasGeneratedBy(pdb70, get_rcsb, endTime)
        doc.wasDerivedFrom(pdb70, resource_rcsb, get_rcsb, get_rcsb, get_rcsb)

        bm5 = doc.entity('dat:idesta#bm5', {prov.model.PROV_LABEL:'benchmark5 from Weng Lab', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bm5, this_script)
        doc.wasGeneratedBy(bm5, get_wengLab, endTime)
        doc.wasDerivedFrom(bm5, resource_wengLab, get_wengLab, get_wengLab, get_wengLab)

        lb_1ahw = doc.entity('dat:idesta#lb_1ahw', {prov.model.PROV_LABEL:'bound ligand of 1ahw', prov.model.PROV_TYPE:'ont:DataSet'})
        lu_1ahw = doc.entity('dat:idesta#lu_1ahw', {prov.model.PROV_LABEL:'unbound ligand of 1ahw', prov.model.PROV_TYPE:'ont:DataSet'})
        rb_1ahw = doc.entity('dat:idesta#rb_1ahw', {prov.model.PROV_LABEL:'bound receptor of 1ahw', prov.model.PROV_TYPE:'ont:DataSet'})
        ru_1ahw = doc.entity('dat:idesta#ru_1ahw', {prov.model.PROV_LABEL:'unbound receptor of 1ahw', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lb_1ahw, this_script)
        doc.wasAttributedTo(lu_1ahw, this_script)
        doc.wasAttributedTo(rb_1ahw, this_script)
        doc.wasAttributedTo(ru_1ahw, this_script)
        doc.wasGeneratedBy(lb_1ahw, get_lbpdb, endTime)
        doc.wasGeneratedBy(lu_1ahw, get_lupdb, endTime)
        doc.wasGeneratedBy(rb_1ahw, get_rbpdb, endTime)
        doc.wasGeneratedBy(ru_1ahw, get_rupdb, endTime)
        doc.wasDerivedFrom(lb_1ahw, resource_dtm, get_lbpdb, get_lbpdb, get_lbpdb)
        doc.wasDerivedFrom(lu_1ahw, resource_dtm, get_lupdb, get_lupdb, get_lupdb)
        doc.wasDerivedFrom(rb_1ahw, resource_dtm, get_rbpdb, get_rbpdb, get_rbpdb)
        doc.wasDerivedFrom(ru_1ahw, resource_dtm, get_rupdb, get_rupdb, get_rupdb)

        clus = doc.entity('dat:idesta#1ahw_clus', {prov.model.PROV_LABEL:'clusterfile of 1ahw', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(clus, this_script)
        doc.wasGeneratedBy(clus, get_clus, endTime)
        doc.wasDerivedFrom(clus, resource_dtm, get_clus, get_clus, get_clus)

        ft = doc.entity('dat:idesta#1ahw_ft', {prov.model.PROV_LABEL:'ftfile of 1ahw', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(ft, this_script)
        doc.wasGeneratedBy(ft, get_ft, endTime)
        doc.wasDerivedFrom(ft, resource_dtm, get_ft, get_ft, get_ft)

        #repo.record(doc.serialize()) # Recording the prov document

        repo.logout()
                  
        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
loadData.execute()
doc = loadData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof
