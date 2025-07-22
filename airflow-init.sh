#!/usr/bin/env bash

# Wait for the database to be ready
sleep 10

# Initialize Airflow DB
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname payal \
    --lastname navsupe \
    --role Admin \
    --email payalsnavsupe@gmail.com \
    --password stream@123
