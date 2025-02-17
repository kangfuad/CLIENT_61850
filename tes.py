import logging
import time
from libiec61850client import iec61850client

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def main():
    try:
        client = iec61850client()
        
        # Connection parameters
        host = "10.14.152.233"
        port = 10102
        
        # iec61850://10.0.0.2:102/IED1_XCBRGenericIO/CSWI2.Pos
        # Build reference
        ref = f"iec61850://{host}:{port}/IED1_XCBRGenericIO/CSWI1.Pos"
        
        # Test connection
        logger.info(f"Testing connection to {host}:{port}")
        err = client.getIED(host, port)
        if err != 0:
            logger.error(f"Failed to connect to IED")
            return

        # Read current value to verify connection and reference
        logger.info("Reading current value")
        value, err = client.ReadValue(ref)
        if err != 0:
            logger.error(f"Failed to read value")
            return
            
        # Verify control model
        if ('ctlModel' in value and 
            'value' in value['ctlModel'] and 
            value['ctlModel']['value'] == '4'):
            logger.info("Control model verified: sbo-with-enhanced-security")
        else:
            logger.error("Unexpected control model")
            return

        # Get current position
        current_pos = "0"
        if 'stVal' in value and 'value' in value['stVal']:
            current_pos = value['stVal']['value']
        logger.info(f"Current position: {current_pos}")

        # Calculate target value - invert current position
        target_value = "1" if current_pos == "1" else "0"
        logger.info(f"Target value for select: {target_value}")

        # Try select with more diagnostic info
        logger.info("Attempting select operation")
        try:
            error, addCause = client.select(ref, target_value)
            logger.info(f"Select returned: error={error}, addCause={addCause}")
        except Exception as e:
            logger.error(f"Exception during select: {e}", exc_info=True)
            return

        if error == 1:
            logger.info("Select successful")
            time.sleep(1)
            
            # Try operate
            logger.info("Attempting operate operation")
            try:
                error, addCause = client.operate(ref, target_value)
                logger.info(f"Operate returned: error={error}, addCause={addCause}")
            except Exception as e:
                logger.error(f"Exception during operate: {e}", exc_info=True)
                return

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        logger.info("Cleanup complete")

if __name__ == "__main__":
    main()