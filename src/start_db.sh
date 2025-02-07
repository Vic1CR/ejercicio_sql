#!/bin/bash

# Starts posgreSQL service, creates gitpod user and creates database 'sample_db'
sudo service postgresql start
psql -U postgres -c "CREATE USER gitpod;"
psql -U postgres -c "CREATE DATABASE sample_db OWNER gitpod;"