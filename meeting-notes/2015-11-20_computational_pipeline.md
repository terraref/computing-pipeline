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


Need to figure out data flows for the entire project

* **Victor**: makes sense to analyze data at same place it is generated. Then deposit both raw data and derived products in common repository.
* **Chris Fields**: browsing and visualization 


* What can teams collaborate on?
  * TODO Survey to be coordinated by Rob, Rachel, and David
    * what people are required to do according to milestones, and what are the overlaps?
    * how can the reference team help? to what degree are teams interested in using a centralized pipeline, and in sharing data at different points in the pipeline?




