from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin


import operators
import helpers


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.S3ToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.CreateS3BucketOperator,
        operators.CheckS3FileCount,
        operators.UploadFilesToS3Operator
    ]
    helpers = [
        helpers.SqlLoadFacts,
        helpers.SqlLoadDimensions,    
        helpers.SqlLoadStaging, 
        helpers.SqlDdl 
    ]