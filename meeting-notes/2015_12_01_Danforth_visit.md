# TERRAref Danforth visit
Date:	Tuesday, Dec 1, 2015  
Time:	9:00-3:30 pm  
Location:	2000 NCSA  


## Phenomics pipeline 
Rob Alba, Noah Fahlgren, David LeBauer, Rachel Shekar, Yan Liu, Rob Kooper, Max Burnette, David Raila, Dan Lapine

  
 1.	Overview of phonemics pipeline: what we have so far

  •	Clowder 
  •	CMS for data sharing with privacy control.  Originally started with Medichi 6 years ago, but metadata extraction with tagging and commenting was data agnostic and not scalable and difficult for others to update and change.  Clowder was developed to scale both horizontally (multiple servers to share and balance load) and vertically (more memory) and easily be able to add code to change extractors.  It is no SQL.  Extractors distributed across servers that extract metadata.  We are learning from NSF Datanet project looks at long term data storage and publishing using standards.  NSF Browndog project also uses Clowder.  It is about how to extract information from files (metadata) and alter file formats.  All of this software is Open Source.
  •	PlantCV VM, Jupyter notebook   
  •	PlantCV is now running on Clowder.  Files can be uploaded from the archive and other images and metadata will be created.  API can push the data as well. We still need to determine how AZ data will be pushed to Clowder. “Collections” can be created using metadata.  Noah and everyone else needs to know how to update and add extractors to pipeline.  VM allows software to be run by anyone who has open stack.  Initial setup only requires a few lines of code.  
  •	In the next 6 months we need to determine what data we want to show.  There is common data that everyone will want to access that always needs to be updated real-time.    
  •	Metadata is stored in MongoDB (no SQL).  Every PlantCV file will trigger extractors.  This information will be stored within Clowder. In the future, the architecture will be expanded.  As files are added, individual extractors will process them, but the extractors can also interact with each other.  
  •	Make contact with algorithm experts at WUSL: Robert Pless (data collection) and Roman Garnett (prediction)  
  •	We are currently testing different storage options before choosing the best for our use and scalable.  Right now, we only have 5 PB at Roger and Blue Waters has 10PB. We need to determine where to invest money to add storage.  
  •	Implicit parallelization? Recoding can reduce load by 10%. We have architectural parallelization.  1million CPU hours from xsede so it is capable of parallelization at the level we need. A lot of images can be handled simultaneously and individually in parallel.
  •	We not want to run Matlab as a service.  We do not have licensing for this.  However, people can use it if they have the license.  
  •	The developer and end user can go to clowder to open Jupyter and play with data and alter parameters.  Each user would be authentic with a local account and could have their own configuration with customizable datasets.  They could launch their own VM and share access.  Settings could also be shared with rest of group.  
2.	The developer and end user can go to clowder to open Jupyter and play with data and alter parameters.  Each user would be authentic with a local account and could have their own configuration with customizable datasets.  They could launch their own VM and share access.  Settings could also be shared with rest of group.  
3.	BETYdb
  •	BETYdb has 8 instances and data access can be controlled.  TERRA has three versions of BETYdb running.  Ref, MEPP, test.  This will fit within original EBI BETYdb paradigm, but with only one species. Data can be downloaded.  This differs from Clowder in that it has hierarchical data structure for plant traits.  Clowder will be capable of adding metadata to BETYdb.    
  •	Kresovich wants the word “accession” to be used.  “Lines” is for inbred.  Rob will ask Cat 1 PM Barry Flynn will ask for a terminology document to be put together.   
  •	Ropensci/traits, url-based API
4.	What else do we need, what are priorities for pipeline from Danforth?
5.	How will the field system differ?
•	One of Lemnatec’s software programs is proprietary and runs the sensors.  Lemnatec will give 3 software packages to us that are Open Source.  This was not done with Danforth sensors – they gave software but it was not Open Source so PlantCV was created.  These software packages are Lemna minor (like PlantCV), Lemna grid, and xx.  These will be available on github to Cat5 team. 
•	A Lemnatec agreement for shared IP will be sent to U of I for signature later. This will include patenting guidelines.
6.	Review core features so that priorities can be identified
7.	Other
•	Clowder has the API
•	Where in the pipeline does Lemnatec see their software installed?  A full-time Lemnatec coder will be hired and working in St. Louis to work with developers.



## Technical discussion
  *
1.	e.g. how to add a Clowder extractor
2.	how to query data from BETYdb
3.	review of plans for PlantCV development, field system challenges 
4.	Other
•	How do people use spectral reflectance?  They look for indicators using principle components.  
•	Need laser to correct for distance of leaf to camera, for example in correcting fluorescence. 
•	Proposal including 3D reconstruction 
•	Sorghum to be planted at Danforth in May.   The system will be available after Jan 4 or brachpodium data from current experiment can be shared now.  We need to practice live to determine who we will work with at Danforth and how the servers will be connected.
•	Combine metadata with image file or keep separate?
•	Max and David both suggest to embed metadata into image files to reduce the number of files and potential for orphaned files.  Another option would be image files and metadata files zipped together.  This would also work if it is a single upload.  
•	Dataset is a logical grouping of files that belong together.  They don’t make much sense separately.
•	Collection is a random group of files.  We will have hierarchical collections




## Discussion of Genomics Pipeline 
Rob Alba, Noah Fahlgren, David LeBauer, Matt Hudson, Rachel Shekar, Rob Kooper, David Raila, Max 

  
1.	Rob A. and Noah: overview of TERRA wide experimental design and prioritize 
•	Genomics data is higher than phenomics because the data will be produced sooner.  TERRA wants the data available as soon as possible.
•	Longer term add reference or pan genomics and realign against references and combining genotype with phenotype data
2.	What, would a shared data management and computing infrastructure look like, what can we do to support the TERRA program (e.g. what are pain points for teams that we can solve; where is a standardized pipeline useful?)
•	We need to developer a pipeline, but we do not have to develop resources for everyone – if we wish, we can do that for a fee. Texas Advanced Computing Center already has a standard set of software.  This also exists on HPC Bio at IGB.  Automation would need to be set up if we use NCSA servers, and HPC can set this up as fee for service.
•	Genomics data requirements are not met by NCSA system
•	Xsede may provide this resource for free.
•	Luda Mainzer has plug and play already running on Blue Waters and iforge for May Clinic via NCSA.  This is a huge scale project. 
•	Iforge is an HPC cluster set up for private sector set up through NCSA.  This restricts access
•	We need to be aware of vision of Joe Cornelius – Luda’s setup may work better, it can be easily altered
3.	Discussion
•	Set up the pipeline with Mike Gore, this process is now fairly established.  We need to choose a good place to put it so that there is adequate compute resources.
•	Can genomics data be put on VM?



## Roadmap: next six months and beyond for phenomics pipeline:
Entire TERRAref team

  *
1.	Where do we need to be in 6 months?  
 *	Genomics pipeline port  
 *	Flow of images from Lemnatec at Danforth to UIUC  
 •	Clowder extractors – make sure that they can handle multiple image types; metadata transfer workflow
•	Put data from Danforth Lemnatec images into database
•	Progress on compression
•	Determine how to handle data flow from MAC, MC state, HudsonAlpha, and KSU (drones)
•	Rob to try to get Kresovich to upload zipped file containing each pair of Illumina FastQ files to Clowder
2.	Setting up Danforth and MAC data flows
3.	Prioritize use cases and features
•	Combining genomics and phenomics data, mapping (e.g. gwas)
•	Provide guidance for genome-wide selection
•	Breeders will want to rank traits 
•	Noah to provide sample data to David
•	Show image related to data extracted from it
•	Data from certain sensors, replicate (individual plants)
•	Prioritize easy and functional first 
4.	Summary and Next steps
•	Keep copy of calculations on xsede?  Not for permanent storage
•	Ramon will help determine which sensors we really need so we can learn about the traits that people really care about
•	Rob to talk to Jeff about who is responsible for genomics pipeline
•	Have genomics pipeline set up in next 6 months
•	Max reviewed “transferring data to clowder using API” readme
•	What does Plant CV/Danforth do with their data?
•	Normalization
1.	Zoom correction, color histogram relative to plant size
•	Linear modelling some traits
•	Downstream statistics
•	We must incorporate hand collected and weather data.  Lemnatec will set up micromet sensors; some will be stationary, some will move with the system
•	Are there public genomics datasets that we can leverage?
•	We could do brachia but this may not be good use of time because data will start coming in in 3 months.  Rob will talk to Stephen Kresovich to get his GBS and phenotype data for use as baseline data before it is published.


