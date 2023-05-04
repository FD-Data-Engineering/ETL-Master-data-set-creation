
 - This folder contains an explanation to run Notebooks in Jupyter DataScience

 1. In the Airflow Node set a job to ssh into the Jupyter DataScience node and run a bash script
 2. Setup a bash scrit to source the service account ( /home/notebookuser/ ) profile environment variables
 3. Run the automation of Jupyter following examples  ( follow example run-automation-Folium-Notebook.sh )  and save its output in a daily file
 4. All runs should generate a log file under folder ( follow example run-automation-Folium-Notebook.sh ) /home/notebookuser/notebooks/crontab