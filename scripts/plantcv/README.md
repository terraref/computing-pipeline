# TERRAClowderUploadPython.py

This is an example script showing how one could iterate through a folder of images, create a Clowder dataset to contain them, and post all JPG images in the folder to that dataset.

In this example:
* **clowder_url** contains the URL of the Clowder target instance.
* **directory_path** contains the folder to iterate through.
* **line 21** ("name": "Test Dataset") defines the name of the dataset the images will be loaded into.

This script could be modified to write filenames matching specific patterns to particular datasets.

Coming soon, an example of loading associated .json files with images into their EXIF metadata to bundle image+metadata into one file.
