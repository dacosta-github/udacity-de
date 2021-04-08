clean:
	rm -f venv 


venv:
	python3 -m venv venv           

airflow_home:
	export AIRFLOW_HOME=./airflow   

install: venv
	# airflow needs a home, ~/airflow is the default,
	# but you can lay foundation somewhere else if you prefer
	# (optional)
	airflow_home
	export SLUGIFY_USES_TEXT_UNIDECODE=yes \
	export LC_ALL=”en_US.UTF-8" \
	export LC_CTYPE=”en_US.UTF-8" \
	export AIRFLOW_GPL_UNIDECODE=yes \
	source venv/bin/activate; \
	python3 -m pip install --upgrade pip \
	
	# requirements
	pip install -r requirements.txt  

	# initialize the database
	airflow db init

	# create a admin user
	airflow users create \
		--username admin \
		--firstname admin \
		--lastname admin \
		--role Admin \
		--email admin@admin.org

local: airflow_home

	pip install -r requirements.txt  

	pip install apache-airflow[postgres,gcp] --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.7.txt"


	# initialize the database
	airflow db init

	airflow users create \
		--username admin \
		--firstname admin \
		--lastname admin \
		--role Admin \
		--email admin@admin.org


