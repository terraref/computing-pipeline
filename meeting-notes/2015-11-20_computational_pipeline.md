# TERRAref Genomics Pipeline Meeting

* Date: Friday, Nov 20, 2015
* Time: 11:00-12:00 pm CST

## Participants:

* David LeBauer (TERRAref & TERRA cat 1, Illinois)
* Vicor Jongeneel (HPCBio, Illinois)
* Matt Hudson (Illinois)
* Chris Fields (HPCBio, Illinois)
* Noah Fahlgren (Danforth)
* Rob Alba (Danforth)

## Agenda

1. Review proposal terraref/reference-data#19
2. Determine roles / interests / contributions of each participating group
  *  TERRA REF
  *  TERRA MEPP
  *  HPCBio
3. Estimate data volumes (order of magnitude?) for each step
4. Define Use cases

## Teams

### TERRA REF

Will provide central repository and computing resources for genomics pipeline.

Core data set; sequencing to be done by Jeremy Schmutz at HudsonAlpha 

* 75 shared lines
* 40 de novo sequences, 
 * 15 by PacBio (? I infer this is a sort of 'gold standard'?)
* 400 resequences  (enough coverage with enough coverage to do assembly 

Estimated completion date: Mid 2016


### TERRA MEPP 


Amount of sequencing is to be determined, unclear from programmatic level what will be required. Pending discussions like this.


### HPC Bio

Can support this project; they need clear specifications and funding 

## Data volume and compute needs

### Sequence alignments

* BAM can be very large (TBs); can be visualized in Jbrowse. 
  * Sorted BAM is smaller (compressed). 
  * Only need to keep sorted BAM. Can drop raw SAM and BAM intermediate files. 
  * 1000 genomes, 30x ea. 4TB of BAMs + 4TB fastQ, no more than 20 TB. 

* CPU time: 
 * alignment depends on aligner efficency; runs 20x faster than BWA. 10k - 100k CPU-h for re-aligning 1000 genomes
 * denovo assembly: depends on context

### Genome Assembly

* assembling Cassava genome 5k CPU-h per genome. This will be more time than the re-alignment. 
* Define Use cases

## Open Questions to be addressed by larger TERRA program

### Shared germplasm

* where to partition 15, 40, 400 lines in genomics diversity
* What will be the coordinated germplasm used for cross-site G x E analysis?
* ED: should spread 15 PacBiom through diversity of germplam + more coverage on more likely to be commercial lines


### Define data flows for the entire project

* makes sense to analyze data at same place it is generated. Then deposit both raw data and derived products in common repository.
* compute, browsing, visualization should be done where data is
* a core standardized pipeline would be of value to individual teams  


### Define Collaborations among teams (with germplasm, informatics, cyberinfrastrucutre)

* **TODO** Survey of theteams to be coordinated by Rob, Rachel, and David

What can teams collaborate on?
 * what people are required to do according to milestones, and what are the overlaps?
 * how can the reference team help? to what degree are teams interested in using a centralized pipeline, and in sharing data at different points in the pipeline?


#### Summary

The Cat5 reference platform was not designed to develop bioinformatics tools. Indeed, the focus of TERRA is to develop the technology of phenomics that has fallen behind that of genomics. 

HPCBio provides a wide range of bioinformatics computing services [1] and the cluster they use is biocluster [2]. The costs of using Biocluster are reasonable and both the HPCBio and IGB teams are exceptional. In addition, IGB have Galaxy and KBase available. Fees are reasonable and the quality of their work is high. The HPCBio team is enthusiastic about our project and available to assist.
Access to the expertise and services of HPCBio provides a major added value to users of the reference pipeline, but has not been budgeted as an essential feature. 

Because the forcus of the TERRA program is to advance phenomics, the UI / NCSA team has been built around relevant expertise in ecophysiology, high performance computing, GIS, and computational workflows.
The TERRA program has many experts in bioinformatics. We can thus provide a common collaborative space for the implementation of cutting edge pipelines while also supporting the use of existing and familiar tools and substantial computing power.


NCSA is very generous and supportive of our efforts, and have committed to removing limitations imposed by computing power or storage space. The limiting factor will be the number of contributors to a shared infrastructure and the efficiency with which they can collaborate. 

#### For individual teams, protected IP

We have a core set of computing allocated for use by independent researchers and teams without any requirement that the teams share code or data. Indeed, secure computing and storage is an important feature of our platform. While this resource is more limited, NCSA has staff to help researchers apply for as well as use and optimize code for HPC allocations (through xsede.org). XSEDE allocations must be renewed annually, but usually the challenge is getting people to use them.

#### For collaboration and open science objectives

We can provide an unprecedented level of support for open science aimed at sharing data and computing infrastructure. 

For more details about the computing that has been allocated, the CyberGIS group has committed 1 PB of online storage and a million compute hours on a dedicated node, plus access to many times that much computing on a shared queue [3]. 
Additional requests to support the 10x increase in data volume that has occurred during construction of the Lemnatec field system [4].


[1] http://hpcbio.illinois.edu/content/services-and-fees 
[2] http://help.igb.illinois.edu/Biocluster
[3] https://wiki.ncsa.illinois.edu/display/ROGER/ROGER+Technical+Summary
[4] http://terraref.ncsa.illinois.edu/articles/spectral-imaging-data-volume-compression/#on-the-upcoming-data-deluge

