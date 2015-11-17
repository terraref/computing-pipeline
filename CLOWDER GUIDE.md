Clowder Installation & Development
==================================

Clowder setup
-------------
An online overview of installing Clowder is available [here](https://opensource.ncsa.illinois.edu/projects/artifacts/CATS/0.9.1/documentation/manual/).

Requirements for Clowder:
o	Java JDK and JRE
o	MongoDB
•	RabbitMQ is required for extractor functionality/development.
•	Clowder source code is available as a [git repository](https://opensource.ncsa.illinois.edu/stash/projects/CATS). 

Developing extractors
---------------------
Coming soon
https://opensource.ncsa.illinois.edu/confluence/display/CATS/Deploying+Windows+Extractors


Transferring data into Clowder using API
========================================
Two example sources that will be pushing high data volumes into Clowder:
* LemnaTec indoor system at Danforth (running)
* LemnaTec outdoor system at Maricopa (in progress)

File delivery
Each file has some metadata associated with it. Generally, sources will write scripts against the Clowder API to post files to the database. Metadata can potentially be associated a few different ways.
A generic workflow:
1. Source has one or more files (e.g. a JPG) and metadata associated with each in some format.

2. Source creates a Dataset in Clowder to hold the file(s). This allows related files to be stored together.
* POST to /api/datasets/createempty returns a new empty Dataset ID.

3.	Source uploads file(s) to new Dataset.
* POST to /api/uploadToDataset/:id where :id is the Dataset ID from Step 2. This will return the new File ID.
* This initiates extractors to derive information from the file (e.g. PlantCV, EXIF metadata). Derived information is automatically added to file’s metadata. 
* Certain metadata/extractor outputs will be added to a BETYdb database as well.

4.	Source associates metadata with files. Several options here:
* POST to /api/files/:id/metadata where :id is the File ID from Step 3, upon receiving successful response. Metadata can be individual fields or a JSON object.
* Embed all desired metadata in the image itself, using something like [ExifTool](http://www.sno.phy.queensu.ca/~phil/exiftool/). Our extractors can parse this metadata.
* POST a .JSON file with the same name as the primary file to the same Dataset – we write an extractor to search for an existing file to associate the parsed JSON data with. This may be less desirable if it doubles the number of files to transfer.
It is desirable for end users to have a means to export available metadata for a file as JSON, XML, YAML, etc. 
