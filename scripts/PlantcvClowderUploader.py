#!/usr/bin/env python

import os
import argparse
import requests


# Parse command-line arguments
###########################################
def options():
    """Parse command line options.

    Args:

    Returns:
        argparse object

    Raises:
        IOError: if dir does not exist.
        IOError: if the metadata file SnapshotInfo.csv does not exist in dir.
    """

    parser = argparse.ArgumentParser(description="PlantCV dataset Clowder uploader.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--dir", help="Input directory containing image snapshots.", required=True)
    parser.add_argument("-l", "--dataset", help="Clowder dataset name.", required=True)
    parser.add_argument("-u", "--url", help="Clowder URL.", required=True)
    parser.add_argument("-U", "--username", help="Clowder username.", required=True)
    parser.add_argument("-p", "--password", help="Clowder password.", required=True)

    args = parser.parse_args()

    if not os.path.exists(args.dir):
        raise IOError("Directory does not exist: {0}".format(args.dir))
    if not os.path.exists(args.dir + '/SnapshotInfo.csv'):
        raise IOError("The snapshot metadata file SnapshotInfo.csv does not exist in {0}".format(args.dir))

    return args


###########################################

# Main
###########################################
def main():
    """Main program.
    """

    # Get options
    args = options()


###########################################

if __name__ == '__main__':
    main()
