#!/usr/bin/env python3
import logging
from libiec61850client import iec61850client
from typing import Tuple, Optional, Dict
import sys
import time

# Konfigurasi logging
logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('switch_control.log')
    ]
)
logger = logging.getLogger(__name__)

class IEDControlManager:

    def read_value(data):
        logger.debug("read value:" + str(data['id']))
        return  iec61850client.ReadValue(data['id'])

    # control model, only supports control object ref. e.g. LogicalDevice/CSWI.Pos
    def operate(data):
        logger.debug("operate:" + str(data['id']) + " v:" + str(data['value'])  )
        return iec61850client.operate(str(data['id']),str(data['value']))

    #supports both SBO and SBOw
    def select(data):
        logger.debug("select:" + str(data['id'])  )
        return iec61850client.select(str(data['id']),str(data['value']))

    #cancel selection
    def cancel(data):
        logger.debug("cancel:" + str(data['id'])  )
        return iec61850client.cancel(str(data['id']))
    

if __name__ == "__main__":
    client = iec61850client()

    # Lakukan select terlebih dahulu
    error, addCause = client.select("iec61850://127.0.0.1:10102/CSWI_IEDCSWI_Control/CSWI1.Pos", "True")
    if error == 1:
        print("Selected successfully")
        # Jika select berhasil, lakukan operate
        error, addCause = client.operate("iec61850://127.0.0.1:10102/CSWI_IEDCSWI_Control/CSWI1.Pos", "True")
        if error == 1:
            print("Operated successfully")
        else:
            print(f"Failed to operate: {addCause}")
    else:
        print(f"Failed to select: {addCause}")