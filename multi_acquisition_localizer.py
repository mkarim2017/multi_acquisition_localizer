#!/usr/bin/env python

import os, sys, time, json, requests, logging
import sling_acquisitions
import acquisition_localizer_multi

def main():

    
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    #sling_acquisitions.resolve_source(context_file)    
    acquisition_localizer_multi.resolve_source(context_file)
if __name__ == "__main__":
    sys.exit(main())
