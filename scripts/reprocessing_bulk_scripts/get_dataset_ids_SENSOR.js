/*
Template for script to generate a CSV containing list of dataset IDs from MongoDB.

Usage:
    mongo clowder --quiet get_dataset_ids_SENSOR.js > list_SENSOR_YEAR.csv
Output:
    dataset_id,dataset_name

The regex is used to filter by dataset name. Using the ^ character for start of name
speeds things up considerably.

Sensor reference:
    stereoTop - 2018
    RGB GeoTIFFs - 2018
    flirIrCamera - 2018
    Thermal IR GeoTIFFs - 2018
    scanner3DTop - 2018
    Laser Scanner 3D LAS - 2018
    VNIR - 2018
    EnvironmentLogger netCDFs - 2018

After generating the CSV, submit it to a Clowder instance for extraction using:
    submit_datasets_by_list.py
*/

db.datasets.find({"name": {$regex: /^SENSOR - YEAR.*/}}).forEach(function(ds){
    print(ds._id.valueOf()+","+ds.name);
});
