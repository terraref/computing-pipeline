/*
    Script to clean Mongo database of duplicate records and repair hierarchy.
*/

function main() {
    // Set true to avoid actually modifying db
    DRY_RUN = true;

    // List of preferred content creators
    OFFICIAL_OWNERS = [
        {"author._id": ObjectId("57adcb81c0a7465986583df1")}, // Maricopa Site
        {"author._id": ObjectId("5808d84864f4455cbe16f6d1")}, // Danforth Site
        {"author._id": ObjectId("5900decc0fbf573767aecd8b")}, // KSU Site
        {"author._id": ObjectId("58f63e700fbf5737679186a4")}  // UIUC Energy Farm
    ];
    // List of primary spaces for reference
    RAW_DATA_SPACE = "571fb3e1e4b032ce83d95ecf";
    PROCESSED_DATA_SPACE = "5bdc8f174f0cb2fdaaf3148e";
    PREFERRED_SPACES = [
        {"spaces": ObjectId(PROCESSED_DATA_SPACE)}
    ];

    MARICOPA_AUTHOR = {
        "_typeHint" : "models.MiniUser",
        "_id" : ObjectId("57adcb81c0a7465986583df1"),
        "fullName" : "Maricopa Site",
        "avatarURL" : "http://www.gravatar.com/avatar/8a6619a4e7837b2d915bb49b82af5538?s=256&d=mm",
        "email" : "terrarefglobus+uamac@ncsa.illinois.edu"
    };




    print("NAME,TYPE,STATUS,ID,OWNER,PARENT_TARGET,PARENT_ACTUAL,NOTES");

    // WARNING: The first two Files methods are slow on a large db
    cleanFiles();
    removeMissingFiles();
    cleanCollections();
    cleanDatasets();
    healMisnamed();
    healOrphans();
}

function cleanFiles() {
    /*
     1. find all files occurring more than once
     2. for each duplicate name, choose one collection to be the Master and others Duplicates
     3. delete each Duplicate
     */

    // Get counts of file names that appear more than once in the database
    duplicates = db.uploads.aggregate([{
        "$group": {"_id": "$filename", "count": {"$sum": 1}}}, {
        "$match": {"_id": {"$ne": null}, "count": {"$gt": 1}}}, {
        "$project": {"name": "$_id", "_id": 0}
    }], {allowDiskUse: true});

    while (duplicates.hasNext()) {
        dupe = duplicates.next().name;

        // Officially owned?
        bigboss = db.uploads.findOne({"filename": dupe, "$or": OFFICIAL_OWNERS});
        status = "OFFICIAL_OWNER";
        if (!bigboss) {
            // Just gimme the first one!
            bigboss = db.uploads.findOne({"filename": dupe});
            status = "LAST_RESORT"
        }

        // Handle duplicates besides the boss we've chosen
        print(dupe + ",FILE,MASTER," + bigboss._id+","+status);
        subs = db.uploads.find({"filename": dupe, "_id": {"$ne": bigboss._id}});
        while (subs.hasNext()) {
            subdoc = subs.next();
            print(dupe + ",FILE,DUPLICATE," + subdoc._id+",");

            if (!DRY_RUN) {
                deleteFileEntry(file_obj._id);
            }
        }
    }
}

function removeMissingFiles() {
    file_list = db.uploads.find();

    while (file_list.hasNext()) {
        file_obj = file_list.next();

        try {
            md5sumFile(file_obj.loader_id);
        } catch(error) {
            print("FILE NOT FOUND: "+file_obj.loader_id);
            if (!DRY_RUN) {
                deleteFileEntry(file_obj._id);
            }
        }
    }
}

function deleteFileEntry(file_id) {
    // Remove attached things first
    db.previews.remove({"file_id": file_id});
    db.metadata.remove({"attachedTo._id": file_id});
    db.extractions.remove({"file_id": file_id});
    db.sections.remove({"file_id": file_id});

    // Remove actual file
    db.uploads.remove({"_id": file_id});
}

function cleanCollections() {
    /*
    1. Find all collection names occurring more than once
    2. For each duplicate name, choose one collection to be the Master and others Duplicates
    3. For each Duplicate, migrate relationships and associated datasets to Master and delete
     */

    // Get counts of collection names that appear more than once in the database
    duplicates = db.collections.aggregate([{
        "$group": {"_id": "$name", "count": {"$sum": 1}}}, {
        "$match": {"_id": {"$ne": null}, "count": {"$gt": 1}}}, {
        "$project": {"name": "$_id", "_id": 0}
    }], {allowDiskUse: true});

    // Try to determine what parent should be for each collection (not applicable to sensor/experiment/season level)
    while (duplicates.hasNext()) {
        dupe = duplicates.next().name;
        if (dupe.indexOf("ddpsc") > -1)
            continue;

        if (dupe.indexOf("-") > -1) {
            sensor = dupe.split(" - ")[0];
            split = dupe.split(" - ")[1].split("-");
            if (split.length == 3) {
                month = split[1];
                year = split[0];
                parent = sensor + " - " + year + "-" + month
            } else if (split.length == 2) {
                year = split[0];
                parent = sensor + " - " + year
            } else if (split.length == 1) {
                parent = sensor;
            }
        } else {
            parent = ""
        }

        /*
        This block attempts to categorize the duplicate and determine whether it should be the master copy,
        or if another duplicate is preferred to absorb this one.
         */

        // Is this dupe owned by a preferred owner and within a parent?
        bigboss = db.collections.findOne({"name": dupe, "$or": OFFICIAL_OWNERS, "parent_collection_ids": {$size: 1}});
        status = "OFFICIAL_OWNER_HAS_PARENT";
        if (!bigboss) {
            // Is this dupe within a parent?
            bigboss = db.collections.findOne({"name": dupe, "parent_collection_ids": {$size: 1}});
            status = "HAS_PARENT";
            if (!bigboss) {
                // Is this dupe owned by a preferred owner?
                bigboss = db.collections.findOne({"name": dupe, "$or": OFFICIAL_OWNERS});
                status = "OFFICIAL_OWNER";
                if (!bigboss) {
                    // Is this dupe contained in any space?
                    bigboss = db.collections.findOne({"name": dupe, "spaces": {$size: 1}});
                    status = "IN_SPACE";
                    if (!bigboss) {
                        // None of these dupes seem great, just gimme the first one I find!
                        bigboss = db.collections.findOne({"name": dupe});
                        status = "LAST_RESORT"
                    }
                }
            }
        }

        ds_count = bigboss.datasetCount;
        child_count = bigboss.NOT;

        // If has parent, get information
        colls = bigboss.parent_collection_ids;
        has_parent = true;
        if (bigboss.parent_collection_ids.length > 0) {
            parent_id = bigboss.parent_collection_ids[0];
            parent_real_name = db.collections.findOne({"_id": parent_id});
            if (!parent_real_name)
                parent_id = bigboss.parent_collection_ids[0] + " (NOT FOUND)";
            else
                parent_id = parent_real_name._id;
        }
        else {
            // Otherwise try to find suitable parent
            parent_suggested = db.collections.findOne({"name": parent});
            if (parent_suggested) {
                parent_id = parent_suggested._id;
                status += " (NEW PARENT)";
                has_parent = false
            }
        }

        // Handle duplicates besides the master we've chosen
        print(dupe + ",COLLECTION,MASTER," + bigboss._id+","+bigboss.author.fullName+","+parent+","+parent_id+","+status);
        subs = db.collections.find({"name": dupe, "_id":{"$ne": bigboss._id}});
        while (subs.hasNext()) {
            subdoc = subs.next();
            print(dupe + ",COLLECTION,DUPLICATE," + subdoc._id+","+subdoc.author.fullName+",,,");

            if (!DRY_RUN) {
                // For all collections with Dupe as child, replace with Master as child
                child_count += db.collections.count({"child_collection_ids": subdoc._id});
                db.collections.update({"child_collection_ids": subdoc._id},
                    {"$addToSet": {"child_collection_ids": bigboss._id}}, {"multi": true});
                db.collections.update({"child_collection_ids": subdoc._id},
                    {"$pull": {"child_collection_ids": subdoc._id}}, {"multi": true});

                // For all collections with Dupe as parent, replace with Master as parent
                db.collections.update({"parent_collection_ids": subdoc._id},
                    {"$addToSet": {"parent_collection_ids": bigboss._id}}, {"multi": true});
                db.collections.update({"parent_collection_ids": subdoc._id},
                    {"$pull": {"parent_collection_ids": subdoc._id}}, {"multi": true});

                // For all datasets with Dupe as parent, replace with Master as parent
                ds_count += db.datasets.count({"collections": subdoc._id});
                db.datasets.update({"collections": subdoc._id},
                    {"$addToSet": {"collections": bigboss._id}}, {"multi": true});
                db.datasets.update({"collections": subdoc._id},
                    {"$pull": {"collections": subdoc._id}}, {"multi": true});

                // Master gets Dupe children, parents, spaces and followers to complete transition
                db.collections.update({"_id": bigboss._id},
                    {
                        "$addToSet": {
                            "child_collection_ids": {"$each": subdoc.child_collection_ids},
                            "parent_collection_ids": {"$each": subdoc.parent_collection_ids},
                            "spaces": {"$each": subdoc.spaces},
                            "root_spaces": {"$each": subdoc.root_spaces},
                            "followers": {"$each": subdoc.followers},
                            "author": MARICOPA_AUTHOR
                        }
                    });

                // Try to assign Master to correct parent if it doesn't have one
                if (!has_parent) {
                    db.collections.update({"_id": bigboss._id},
                        {"$addToSet": {"parent_collection_ids": parent_id}}, {"multi": true});
                    db.collections.update({"_id": parent_id},
                        {"$addToSet": {"child_collection_ids": bigboss._id}}, {"multi": true});
                }

                // Delete Dupe
                db.collections.remove({"_id": subdoc._id});
            }
        }

        if (!DRY_RUN) {
            // After all migrations for this Master copy, fix relationship counts
            new_dscount = db.datasets.count({"collections": bigboss._id});
            counter = db.collections.findOne({"_id": bigboss._id});
            if (counter) {
                db.collections.update({"_id": bigboss._id},
                    {
                        "$set": {
                            "datasetCount": new_dscount,
                            "childCollectionsCount": counter.child_collection_ids.length
                        }
                    });
            }
        }
    }
}

function removeReplacedCollections() {
    /*
    This should typically be run manually. Remove all collections that have been replaced in reprocessing
    by filtering according to preferred space.
     */

    db.collections.remove({
       "name": {"$regex": /^Thermal IR GeoTIFFs.*/},
        "spaces": {"$ne": ObjectId("5bdc8f174f0cb2fdaaf3148e")}
    });
}

function cleanDatasets() {
    /*
     1. find all dataset names occurring more than once
     2. for each duplicate name, choose one dataset to be the Master and others Duplicates
     3. for each Duplicate, migrate relationships and associated files+metadata to Master and delete
     */

    // Get counts of dataset names that appear more than once in the database
    duplicates = db.datasets.aggregate([{
        "$group": {"_id": "$name", "count": {"$sum": 1}}}, {
        "$match": {"_id": {"$ne": null}, "count": {"$gt": 1}}}, {
        "$project": {"name": "$_id", "_id": 0}
    }], {allowDiskUse: true});

    while (duplicates.hasNext()) {
        dupe = duplicates.next().name;
        if (dupe.indexOf("ddpsc") > -1)
            continue;

        if (dupe.indexOf("-") > -1) {
            sensor = dupe.split(" - ")[0];
            if (dupe.indexOf("__") > -1) {
                date = dupe.split(" - ")[1].split("__")[0].split("-")[2];
                month = dupe.split(" - ")[1].split("__")[0].split("-")[1];
                year = dupe.split(" - ")[1].split("__")[0].split("-")[0];
                parent = sensor + " - " + year + "-" + month + "-" + date;
                timestamp = true
            } else {
                month = dupe.split(" - ")[1].split("-")[1];
                year = dupe.split(" - ")[1].split("-")[0];
                parent = sensor + " - " + year + "-" + month;
                timestamp = false
            }
        } else {
            parent = "";
            timestamp = false
        }

        // Officially owned in collection?
        bigboss = db.datasets.findOne({"name": dupe, "$or": OFFICIAL_OWNERS, "collections": {$size: 1}});
        status = "OFFICIAL_OWNER_HAS_PARENT";
        if (!bigboss) {
            // Contained in collection?
            bigboss = db.datasets.findOne({"name": dupe, "collections": {$size: 1}});
            status = "HAS_PARENT";
            if (!bigboss) {
                // Officially owned?
                bigboss = db.datasets.findOne({"name": dupe, "$or": OFFICIAL_OWNERS});
                status = "OFFICIAL_OWNER";
                if (!bigboss) {
                    // Contained in space?
                    bigboss = db.datasets.findOne({"name": dupe, "spaces": {$size: 1}});
                    status = "IN_SPACE";
                    if (!bigboss) {
                        // Just gimme the first one!
                        bigboss = db.datasets.findOne({"name": dupe});
                        status = "LAST_RESORT";
                    }
                }
            }
        }

        meta_count = bigboss.metadataCount;

        // If has parent, get information
        colls = bigboss.collections;
        if (bigboss.collections.length > 0) {
            parent_id = bigboss.collections[0];
            parent_real_name = db.collections.findOne({"_id": parent_id});
            if (!parent_real_name)
                parent_id = bigboss.collections[0] + " (NOT FOUND)";
            else
                parent_id = parent_real_name.name
        } else {
            status = +" (NO PARENT LISTED)";
            parent_id = ""
        }

        // Handle duplicates besides the boss we've chosen
        print(dupe + ",DATASET,MASTER," + bigboss._id+","+bigboss.author.fullName+","+parent+","+parent_id+","+status)
        subs = db.datasets.find({"name": dupe, "_id":{"$ne": bigboss._id}});
        while (subs.hasNext()) {
            subdoc = subs.next();
            print(dupe + ",DATASET,DUPLICATE," + subdoc._id+","+subdoc.author.fullName+","+parent+",,");

            if (!DRY_RUN) {
                // Assign Dupe files & folders to Master
                subfolders = db.folders.find({"parentType": "dataset", "parentId": subdoc._id});
                folderset = [];
                while (subfolders.hasNext())
                    folderset.append(subfolders.next()._id);
                db.datasets.update({"_id": bigboss._id},
                    {
                        "$addToSet": {
                            "folders": {"$each": folderset},
                            "files": {"$each": subdoc.files}
                        }
                    });
                db.folders.update({"parentId": subdoc._id},
                    {"$set": {"parentId": bigboss._id, "parentDatasetId": bigboss._id}}, {"multi": true});

                // Assign Dupe metadata to Master
                meta_count += db.metadata.count({"attachedTo._id": subdoc._id});
                db.metadata.update({"attachedTo._id": subdoc._id},
                    {"$set": {"attachedTo": bigboss._id}}, {"multi": true});

                // Assign Dupe events to Master
                db.events.update({"object_id": subdoc._id},
                    {"$set": {"object_id": bigboss._id}}, {"multi": true});

                // Assign Dupe extractions to Master
                db.extractions.update({"file_id": subdoc._id},
                    {"$set": {"file_id": bigboss._id}}, {"multi": true});

                // Delete Dupe
                db.datasets.remove({"_id": subdoc._id});
            }
        }

        if (!DRY_RUN) {
            // After all migrations for this Master copy, fix metadata counts
            db.datasets.update({"_id": bigboss._id},
                {
                    "$set": {
                        "metadataCount": NumberLong(meta_count),
                        "author": MARICOPA_AUTHOR
                    }
                }, {"multi": true});
        }
    }
}

function healOrphans() {
    /*
    1. find all datasets & collections without a parent
    2. determine expected parent name and check for existence
    3. if found, create relationship
     */

    // First, collections
    orphans = db.collections.find({"parent_collection_ids": {$size: 0}});
    while (orphans.hasNext()) {
        lonely = orphans.next();
        if (lonely.name.indexOf("ddpsc") > -1)
            continue;

        // Determine logical parent name if possible
        if (lonely.name.indexOf(" - ") > -1) {
            sensor = lonely.name.split(" - ")[0];
            split = lonely.name.split(" - ")[1].split("-");
            if (split.length == 3) {
                month = split[1];
                year = split[0];
                parent = sensor+" - "+year+"-"+month;
            } else if (split.length == 2) {
                year = split[0];
                parent = sensor+" - "+year;
            } else if (split.length == 1) {
                parent = sensor;
            }

            // Does the parent exist? If so, heal both ends of relationship
            new_parent = db.collections.findOne({"name": parent});
            if (new_parent) {
                status = "ASSIGNED ORPHAN TO PARENT";
                print(lonely.name + ",COLLECTION,ORPHAN," + lonely._id+","+lonely.author.fullName+","+parent+","+new_parent._id+","+status)

                if (!DRY_RUN) {
                    db.collections.update({"_id": lonely._id}, {
                        "$addToSet": {"parent_collection_ids": new_parent._id},
                        "$set": {"author": MARICOPA_AUTHOR}
                    });
                    db.collections.update({"_id": new_parent._id}, {
                        "$addToSet": {"child_collection_ids": lonely._id},
                        "$set": {"childCollectionsCount": new_parent.childCollectionsCount + 1}
                    });
                }
            } else {
                status = "ERR: UNABLE TO FIND EXISTING PARENT";
                print(lonely.name + ",COLLECTION,ORPHAN," + lonely._id+","+lonely.author.fullName+","+parent+",,"+status)
            }
        }
    }

    // Second, datasets
    orphans = db.datasets.find({"collections": {$size: 0}, "$or": OFFICIAL_OWNERS});
    while (orphans.hasNext()) {
        lonely = orphans.next();
        if (lonely.name.indexOf("ddpsc") > -1)
            continue;

        // Determine logical parent name if possible
        if (lonely.name.indexOf(" - ") > -1) {
            dupe = lonely.name;
            sensor = dupe.split(" - ")[0];
            if (dupe.indexOf("__") > -1) {
                date = dupe.split(" - ")[1].split("__")[0].split("-")[2];
                month = dupe.split(" - ")[1].split("__")[0].split("-")[1];
                year = dupe.split(" - ")[1].split("__")[0].split("-")[0];
                parent = sensor + " - " + year + "-" + month + "-" + date
            } else {
                month = dupe.split(" - ")[1].split("-")[1];
                year = dupe.split(" - ")[1].split("-")[0];
                parent = sensor + " - " + year + "-" + month
            }

            // Does the parent exist? If so, heal both ends of relationship
            new_parent = db.collections.findOne({"name": parent});
            if (new_parent) {
                status = "ASSIGNED ORPHAN TO PARENT";
                print(lonely.name + ",DATASET,ORPHAN," + lonely._id+","+lonely.author.fullName+","+parent+","+new_parent._id+","+status)
                if (!DRY_RUN) {
                    db.datasets.update({"_id": lonely._id}, {
                        "$addToSet": {"collections": new_parent._id},
                        "$set": {"author": MARICOPA_AUTHOR}
                    });
                    db.collections.update({"_id": new_parent._id},
                        {"$addToSet": {"child_collection_ids": lonely._id}});
                }
            } else {
                status = "ERR: UNABLE TO FIND EXISTING PARENT";
                print(lonely.name + ",DATASET,ORPHAN," + lonely._id+","+lonely.author.fullName+","+parent+",,"+status)
            }
        }
    }
}

function healMisnamed() {
    /*
     1. find all datasets & collections with malformed name (2016-2016-01-2016-01-01)
     2. rename
     */

    misnamed = db.collections.find({"name": {"$regex": "2016-2016.*"}});
    while (misnamed.hasNext()) {
        target = misnamed.next();
        // "name" : "VNIR Hyperspectral NetCDFs - 2016-2016-11"
        // "name" : "VNIR Hyperspectral NetCDFs - 2016-2016-11-2016-11-02"
        fixname = target.name.replace("2016-2016-11-", "").replace("2016-2016", "2016");
        status = "NAME CORRECTION";
        print(target.name + ",COLLECTION,MISNAMED," + target._id+","+target.author.fullName+",,,"+status);
        if (!DRY_RUN) {
            db.collections.update({"_id": target._id}, {
                "$set": {
                    "name": fixname,
                    "author": MARICOPA_AUTHOR
                }
            });
        }

    }
}

main();
