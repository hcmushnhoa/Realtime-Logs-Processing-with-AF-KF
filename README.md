# Realtime-Logs-Processing-with-AF-KF

# Install python3.10 
sudo apt update
sudo apt install software-properties-common -y 
sudo add-apt-repository ppa:deadsnakes/ppa -y 
sudo apt update 
sudo apt install python3.10 python3.10-venv python3.10-dev -y

# setup enviroment venv for python
python3.10 -m venv .venv
source .venv/bin/activate
# install airflow 2.10.1
mkdir airflow
- create .bashrc file to export variable
source .bashrc
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install boto3 faker confluent_kafka elasticsearch

# run airflow 
airflow standalone
# get user name and password in 'standalone_admin_password.txt'
# change WTF_CSRF_ENABLED -> False,WTF_CSRF_ENABLED trong file 'webserver_config.py' để không bị bad request
# airflow mặc đỉnh chỉ đọc trong /airflow/dags/. Nếu không load được dags thì check dags_folder trong airflow.config nếu chưa đúng thì sửa path lại 
# dags_folder = /workspaces/Realtime-Logs-Processing-with-AF-KF/dags hoặc khi tạo mkdir airflow thì tạo dags bên trong luôn

###
chạy action to upload file and dags to S3
tạo maa(tạo vpc) sau đó tạo enviroment maa -> open airflow UI and check error -> check permission and add role: 'SecretsManagerReadWrite'

vào cloudwatch check log groups để tìm log khi run dags: ...-production-task 

