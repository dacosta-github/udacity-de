#!/bin/bash
export AIRFLOW_HOME=./airflow

airflow scheduler &
airflow webserver -p 8080 &
