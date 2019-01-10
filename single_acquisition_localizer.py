#!/usr/bin/env python

import os, sys, time, json, requests, logging
import acquisition_localizer_single

def main():

    
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise(RuntimeError("Context file doesn't exist."))
    #sling_acquisitions.resolve_source(context_file)    
    acquisition_localizer_single.resolve_source_from_ctx_file(context_file)
if __name__ == "__main__":
    sys.exit(main())
