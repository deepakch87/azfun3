import logging

import azure.functions as func
import os, json


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    env = os.environ
    env_dict = dict(env)
    env_json = json.dumps(env_dict)
    #os.environ['FIRSTNAME']
    FIRST_NAME = os.getenv('FIRSTNAME')

    if not FIRST_NAME:
        raise Exception('No name found')
    else:
        return func.HttpResponse(f"ENVVARS:\n{env_json}")
    