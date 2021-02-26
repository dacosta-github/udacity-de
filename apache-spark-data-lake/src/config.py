"""
    This module allows to manage config file
"""
import os
import sys
import configparser
from datetime import datetime

def update_cfg_file(config_file, section, key, value):
    """
    This function:
        - Writes to an existing config file
    
    Args:
        - param: config_file (object): description
        - param: section (string): The section on the config file to write
        - param: key (string): The key to write
        - param: value (string): The value to write
    
    Returns:
        - None
    """
    
    try:
        ## Reading cfg file
        config = configparser.ConfigParser()
        config.read(config_file)

        ## Setting  Section, Key and Value to be write on the cfg file
        config.set(section, key, value)

        ## Writting to cfg file
        with open(config_file, 'w') as f:
            config.write(f)
    
    except:
        print('Error when trying to update the configuration file.')