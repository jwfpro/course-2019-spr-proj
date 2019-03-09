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
