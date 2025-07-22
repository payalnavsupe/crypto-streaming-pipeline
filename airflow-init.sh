#!/usr/bin/env bash

# Wait for the database to be ready
sleep 10

# Initialize Airflow DB
airflow db init

# Create admin user
airflow users create \
    --username yourusername \
    --firstname yourfirstname \
    --lastname lastname \
    --role yourrole \
    --email youregmail@gmail.com \
    --password yourpassword
