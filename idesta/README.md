Contributor: Israel Desta

## Definitions
complex - when 2 or more individual proteins bind or interact to create a single entity, it is called a complex.
	- generally, we deal with only 2 proteins as it is simple enough for our computational capabilities
	- one of the proteins (usually the larger one) is called the receptor and the smaller is called ligand 
dock    - is the process by which one simulates how or in what manner/shape 2 or more proteins interact with eachother
1AHW    - is an antibody-antigen protein complex that I randomly selected for this project

##Introduction and Background
I work in a structural bioinformatics laboratory in the biomedical engieering department. My work involves understanding how two proteins interact.
For this work, I use a program called ClusPro that 'docks' these two proteins and gives out a several thousand possibilities and the top 10 most
likely candidates from those. However, there are a few steps that need to be done before and after docking. In this project I perform the pre and
post processing of results from ClusPro. 

##Workflow of project #1
0. loadData.py loads all 5 data sets from the 3 data portals: RSCB PDB, Weng Lab at Umass, and datamechanics.io
1. Obtain the well accepted complex benchmark set from Zhiping's lab website. This benchmark set contains 230 complexes of different type. The 
information obtained also includes the category of the complex, the chains for the receptor and the ligand, the change area of interface (ASA), 
and the interface root mean square distance between bound and unbound complexes (iRMSD). # clusterBM5.py 
	a. I use k-means clustering to see if the 230 complexes can be divided into different groups by ASA and iRMSD
	b. I also get the mapping of protein ID to chain ID of each complex
2. I use the Protein ID to chain ID map to change the chain IDs of the unbound proteins that make up 1AHW. (I also do that same for the bound
just in case the chain IDs are different). # renamePDBfile.py
(3. I dock the unbound proteins using ClusPro (this id done outside of the project and involves no transformations))
4. I use the major output of the docking step which is an ft file. This file outlines the coordinates and the different types of energy values.
I find the range of the energy values from the 70,000 rotation lines. # findEnergyRange.py
5. The other output from the docking step is the cluster file which gives the cluster centers and members of the top 1000 rotation lines in the 
ft file mentioned in (4). For this file, I count the number of members in each cluster. # countClusterMembers.py
6. Finally, after docking, we need to check if our simulation is correct or not. For that, one way is to check the bound 1AHW. Another way is to
see how homologous complexes interact. So, for that I pulled proteins with similar sequences from the protein data bank and selected for similar
complexes. # findHomologs.py

-----------------------------------------------------------------Project #2-------------------------------------------------------
# Reminder: 
ClusPro is a software used in our lab that predicts how two proteins interact. It spits out thousands of predictions which may or maynot be accurate.

# Motivation: 
In project 1, I utilized relational building blocks to pre-process protein submissions to ClusPro and analyze the ClusPro outputs like energy, cluster member counts. In our lab, we use an evaluative program called DockQ that scores how good ClusPro's predictions are. In this project, I try to understand what exactly this DockQ score is in the hope to learn what important features are there to improve on ClusPro's prediction accuracies. 

# Aim 1: 
the DockQ program gives four different scores for a single prediction from ClusPro: fnat, irms, Lrms and dockQ. fnat, irms and Lrms are somehow manipulated to obtain the final dockQ score which is used to tell how accurate the prediction is. In the first part, I try to find the scaling factors for irms and Lrms that enables us to obtain dockQ from fnat, irms and Lrms by framing the problem as a constaint satisfaction problem.  The most important constraint is {realDockQ - 0.05 <= (f + ((1 + (ir/i)**2)**(-1)) + ((1 + (lr/j)**2)**(-1)))/3 >= realDockQ + 0.05} where f is fnat, ir is irms, lr is lrms while i and j are the scaling factors for the latter two.

# Aim 2: 
The second goal is to find which of the three fundamental scores are the most important for better dockQ scores. To that end, I will be calculating the correlation coefficients and the corresponding p-values of fnat with dockQ, irmsd with dockQ, and Lrmsd with DockQ. Only the p-values are deposited in the repo as that tells all. 

# Workflow:
0. loadData2.py: loads the evaluation results from DockQ program that has the fnat, irmsd, lrmsd, and dockq scores of each of ClusPro's predictions. Then it uses relational building blocks to filter only the 4 scores and the identifying keys (proteinName, predictionRank, energyRank and coefficientNumber). 
1. constStats.py: retrieves the filtered dockq dataset from repo and computes the following things:
	1.a. Finds the scaling factors of irmsd and lrmsd that yields the dockQ score that satisfy the constraint outlined above in [Aim1] for all 96000+ predictions
	1.b. Finds the p-value for the correlation coefficients of each of the 4 scores with eachother. 
