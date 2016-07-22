Demosaic Extractor
=======================

# Dependencies

* All the Python scripts syntactically support Python 2.7 and above. Please make sure that the Python in the running environment is in appropriate version.

* All the Python scripts also rely on the third-party library including: PIL, scipy, numpy and osgeo.

# Usage

* Run this extractor pointing at rabbitMQ, create a dataset and upload left/right/metadata to that dataset in Clowder pointing at same rabbitMQ, and extractor will add two demosaic'ed JPGs and two GEOTIFFs to the dataset.

* After converting bin-->jpg-->geoTiff, we may integrate the geoTiff files and create the multi-resolution tiles(full field stitched mosaic).

# Notice

* Since we don't have the true value of field of view in 2 meter, we use some parameters to estimate fov, once we get the true value of fov, we should update this code.
