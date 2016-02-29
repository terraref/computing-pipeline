#!/usr/bin/env python
from __future__ import print_function
import argparse
import requests
import json

# Parse command-line arguments
###########################################
def options():
    """Parse command line options.

    Args:

    Returns:
        argparse object

    Raises:

    """

    parser = argparse.ArgumentParser(description="Share Clowder Datasets by associating them with a Clowder Space.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-s", "--space", help="Clowder Space ID.", required=True)
    parser.add_argument("-c", "--collection",
                        help="Clowder Collection ID. This collection should contain the Datasets to be shared.",
                        required=True)
    parser.add_argument("-u", "--url", help="Clowder URL.", required=True)
    parser.add_argument("-U", "--username", help="Clowder username.", required=True)
    parser.add_argument("-p", "--password", help="Clowder password.", required=True)
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-n", "--dryrun", help="Dry run, do not update Datasets.", action="store_true")

    args = parser.parse_args()

    return args


###########################################

# Main
###########################################
def main():
    """Main program.
    """

    # Get options
    args = options()

    # Create new session
    session = requests.Session()
    session.auth = (args.username, args.password)

    # Does the Space exist?
    space = session.get(args.url + 'api/spaces/' + args.space)
    if space.status_code == 200:
        if args.verbose:
            print("The Space {0} returns:".format(args.space))
            print(json.dumps(json.loads(space.text), indent=4, separators=(',', ': ')))
    else:
        raise StandardError("Getting Space {0} failed: Return value = {1}".format(args.space, space.status_code))

    # Does the Collection exist?
    collection = session.get(args.url + 'api/collections/' + args.collection)
    if collection.status_code == 200:
        if args.verbose:
            print("The Collection {0} returns:".format(args.collection))
            print(json.dumps(json.loads(collection.text), indent=4, separators=(',', ': ')))
    else:
        raise StandardError("Getting Collection {0} failed: Return value = {1}".format(args.collection,
                                                                                       collection.status_code))

    # Get all the Datasets in the Collection
    datasets = session.get(args.url + 'api/collections/' + args.collection + '/getDatasets')
    if datasets.status_code == 200:
        ds = json.loads(datasets.text)
        for dataset in ds:
            if args.verbose:
                print(json.dumps(dataset, indent=4, separators=(',', ': ')))

            # Add Dataset to Space
            if args.dryrun is False:
                response = session.post(args.url + 'api/spaces/' + args.space + '/addDatasetToSpace/' + dataset['id'])
                if response.status_code != 200:
                    raise StandardError("Adding Dataset {0} to Space {1} failed: Response value = {2}".format(
                        dataset['id'], args.space, response.status_code))
    else:
        raise StandardError("Getting Datasets for Collection {0} failed: Return value = {1}".format(
            args.collection, datasets.status_code))


###########################################


if __name__ == '__main__':
    main()