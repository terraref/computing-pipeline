#!/usr/bin/env python

from pyclowder.utils import CheckMessage
from pyclowder.files import download_metadata, upload_metadata
from pyclowder.datasets import submit_extraction
from terrautils.extractors import TerrarefExtractor, is_latest_file, load_json_file, \
    build_metadata, build_dataset_hierarchy
from terrautils.betydb import add_arguments, get_sites, get_sites_by_latlon, submit_traits, \
    get_site_boundaries, get_bety_api
from terrautils.metadata import get_extractor_metadata, get_terraref_metadata


def add_local_arguments(parser):
    # add any additional arguments to parser
    add_arguments(parser)

class BetyDBUploader(TerrarefExtractor):
    def __init__(self):
        super(BetyDBUploader, self).__init__()

        add_local_arguments(self.parser)

        # parse command line and load default logging configuration
        self.setup(sensor='stereoTop_canopyCover')

        # assign other argumentse
        self.bety_url = self.args.bety_url
        self.bety_key = self.args.bety_key

    def check_message(self, connector, host, secret_key, resource, parameters):
        self.start_check(resource)

        md = download_metadata(connector, host, secret_key, resource['id'])
        if get_extractor_metadata(md, self.extractor_info['name']) and not self.overwrite:
            self.log_skip(resource,"metadata indicates it was already processed")
            return CheckMessage.ignore
        return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message(resource)

        csv_path = resource['local_paths'][0]

        # if CSV file is a canopyheight file, verify we have the correct contents before sending
        if csv_path.find("_canopyheight") > -1:
            needs_fix = False
            needs_resubmit = False

            # Get CSV contents
            with open(csv_path, 'r') as csv_file:
                lines = csv_file.readlines()
            for line in lines:
                if line.find("canopy_cover") > -1:
                    needs_fix = True
                if line.find(",[],") > -1:
                    needs_resubmit = True

            if needs_resubmit:
                # If actual value was written erroneously, need to redo las2height
                parent_dsid = resource['parent']['id']
                self.log_info(resource, "triggering las2height extractor on %s" % parent_dsid)
                submit_extraction(connector, host, secret_key, parent_dsid, "terra.betydb")
                return
            elif needs_fix:
                # If strings are wrong we can fix in-place
                self.log_info(resource, "attempting to fix canopy_cover references")
                with open(csv_path, 'w') as csv_file:
                    for line in lines:
                        csv_file.write(line.replace("canopy_cover", "canopy_height")
                                       .replace("Canopy Height Estimation from Field Scanner Laser 3D scans", "Scanner 3d ply data to height"))


        # submit CSV to BETY
        self.log_info(resource, "submitting CSV to bety")
        trait_ids = submit_traits(csv_path, betykey=self.bety_key)
        root_url = get_bety_api('traits')
        trait_urls = [(root_url+"?id="+str(tid)) for tid in trait_ids]

        # Add metadata to original dataset indicating this was run
        self.log_info(resource, "updating file metadata (%s)" % resource['id'])
        ext_meta = build_metadata(host, self.extractor_info, resource['id'], {
            "betydb_link": trait_urls
        }, 'file')
        upload_metadata(connector, host, secret_key, resource['id'], ext_meta)

        self.end_message(resource)

if __name__ == "__main__":
    extractor = BetyDBUploader()
    extractor.start()
