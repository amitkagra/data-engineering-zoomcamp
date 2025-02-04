Workflow orchestration using Kestra on LinuxONE community cloud: 

LinuxOne Community Cloud is a free VPS service that doesn't require a credit card. You can use it for about 90 days with a limit of 1 VPS. The VPS comes with 4GB of RAM and 2 VCPUs. 

1. Follow this deployment giude to steps Virtual Server on LinuxONE community cloud
https://github.com/linuxone-community-cloud/technical-resources/blob/master/faststart/deploy-virtual-server.md

2. Follow this to access created LinuxONE virtual server From Windows using PuTTY:
https://github.com/linuxone-community-cloud/technical-resources/blob/master/faststart/PUTTY_Set_up.pdf

3. Install Docker:
	3a. sudo apt-get update 
	3b. sudo apt-get upgrade  
	3c. sudo apt-get install docker.io

4. To run docker without sudo:
	4a. sudo groupadd docker
	4b. sudo gpasswd -a $USER docker
	4c. sudo service docker restart

5. Check docker installation, using command 
	docker run hello-world
In case error> docker: Got permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock

Run command> sudo chmod 666 /var/run/docker.sock

6. Install docker-compose:
	mkdir bin
	cd bin
	wget https://github.com/docker/compose/releases/download/v2.32.4/docker-compose-linux-$(uname -m) -O docker-compose 
	chmod +x docker-compose

7. Add the bin folder to the path:
	Go to the home directory: cd
	Open the .bashrc file: nano .bashrc
	Paste the following to the end of the .bashrc file:
      export PATH="${HOME}/bin:${PATH}"
	Press Ctrl+O > enter > Ctrl+X
	Run source .bashrc for the changes to take effect.
	
8. Install Pgcli:
pip install pgcli
connect to a Postgres database using the command: pgcli -h <hostname> -u <username> -d <database-name>

9. Setup folder structure for Homework 2:
	9a. mkdir 02-workflow-orchestration
	9b. cd 02-workflow-orchestration
	9c. Create file docker-compose.yml using command nano docker-compose.yml
	9d. docker-compose up

10. Open Kestra interface in browser using <created server IP>:8080

11. Open pgadmin interface in browser using <created server IP>:8090, create server using parameter as per parameter given in file docker-compose.yml, pgdatabase/root/root/5432

12. Quiz Questions
Complete the Quiz shown below. It’s a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra and ETL pipelines for data lakes and warehouses.

01: Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?
128.3 MB
134.5 MB
364.7 MB
692.6 MB

Sol: 
1. run flow 02_postgres_taxi.yaml for Yellow Taxi, year 2020 and month 12.
2. Outputs -> extract -> outputfiles -> yellow_tripdata_2020-12.csv
3. Check file size below Debug Outputs.
> 128.3 MB

02: What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?
{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv
green_tripdata_2020-04.csv
green_tripdata_04_2020.csv
green_tripdata_2020.csv

Sol: green_tripdata_2020-04.csv

03: How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
13,537.299
24,648,499
18,324,219
29,430,127

Sol: Import all the Yellow taxi data for year 2020 using flow 02_postgres_taxi_scheduled.yaml with backfill, and run query in pgadmin.

SELECT count(1) FROM yellow_tripdata
WHERE filename LIKE '%2020%';

>24648499

04: How many rows are there for the Green Taxi data for all CSV files in the year 2020?
5,327,301
936,199
1,734,051
1,342,034

Sol: Import all the Green taxi data for year 2020 using flow 02_postgres_taxi_scheduled.yaml with backfill, and run query in pgadmin.

SELECT count(1) FROM green_tripdata
WHERE filename LIKE '%2020%';

>1734051

05: How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
1,428,092
706,911
1,925,152
2,561,031

Sol: Import all the Yellow taxi data for year 2021 using flow 02_postgres_taxi_scheduled.yaml with backfill, and run query in pgadmin.

SELECT count(1) FROM yellow_tripdata
WHERE filename LIKE '%2021-03%';

>1925152

06: How would you configure the timezone to New York in a Schedule trigger?
Add a timezone property set to EST in the Schedule trigger configuration
Add a timezone property set to America/New_York in the Schedule trigger configuration
Add a timezone property set to UTC-5 in the Schedule trigger configuration
Add a location property set to New_York in the Schedule trigger configuration

Sol: Add a timezone property set to America/New_York in the Schedule trigger configuration
Refere: 
https://kestra.io/plugins/core/triggers/io.kestra.plugin.core.trigger.schedule
https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
