clean:
	rm -f venv 


venv:
	python3 -m venv venv            


install: venv
	export AIRFLOW_HOME=./airflow;  \
	export SLUGIFY_USES_TEXT_UNIDECODE=yes \
	export LC_ALL=”en_US.UTF-8" \
	export LC_CTYPE=”en_US.UTF-8" \
	export AIRFLOW_GPL_UNIDECODE=yes \
	source python-venv/bin/activate; \
	python3 -m pip install --upgrade pip; 
	pip install -r requirements.txt; 

	# initialize the database
	airflow db init

	airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin2@admin.org


composer: venv
	docker-compose up
	source venv/bin/activate 
	python3 -m pip install --upgrade pip
	pip install -r requirements.txt  


local:
	# airflow needs a home, ~/airflow is the default,
	# but you can lay foundation somewhere else if you prefer
	# (optional)
	export AIRFLOW_HOME=./airflow
	#~/airflow

	#AIRFLOW_VERSION=2.0.1
	#PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
	# For example: 3.6
	#CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
	# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.0.1/constraints-3.6.txt
	#pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

	pip install -r requirements.txt  

	# initialize the database
	airflow db init

	airflow users create \
		--username admin \
		--firstname admin \
		--lastname admin \
		--role Admin \
		--email admin@admin.org

	# start the web server, default port is 8080
	#export AIRFLOW_HOME=./airflow
	#airflow webserver --port 8080

	# start the scheduler
	# open a new terminal or else run webserver with ``-D`` option to run it as a daemon
	#export AIRFLOW_HOME=./airflow
	#airflow scheduler

	# visit localhost:8080 in the browser and use the admin account you just
	# created to login. Enable the example_bash_operator dag in the home page

