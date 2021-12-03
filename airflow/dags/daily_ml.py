import os
import boto3
import pandas as pd

from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from decimal import Decimal
from io import StringIO

DAG_NAME = os.path.basename(__file__).replace(".py", "")  # Le nom du DAG est le nom du fichier

AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET_NAME_ML = Variable.get("AWS_S3_BUCKET_NAME_ML")
AWS_S3_BUCKET_NAME = Variable.get("AWS_S3_BUCKET_NAME")

default_args = {
    'owner': 'tom',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

@dag(DAG_NAME, default_args=default_args, schedule_interval="0 0 * * *", start_date=days_ago(2))
def dag_projet():
    """
    Ce DAG est notre réponse à la première problématique du sujet # TODO update
    """

    # Charge les données depuis S3
    @task()
    def extract(date):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name="eu-west-3"
        )
        month = date[-5:-3]
        year = date[0:4]
        s3_filename = 'yellow_tripdata_' + year + '-' + month + '.csv'
        local_filename = '/tmp/' + s3_filename
        print("s3_filename",s3_filename)
        if not os.path.isfile(local_filename): # TODO remove after testings
            s3.download_file('esginyprojectrawdata', s3_filename,
                             local_filename)
        return dict(local_filename=local_filename)

    @task()
    def transform(date,paths=None): # TODO faire uniquement pour 1 fichier, renommer les variables
        if paths is None:
            paths = dict(january="/home/noobzik/Documents/ESGI/5A/S1/5-AWS/projet/NY_Project/yellow_tripdata_2019-02.csv",
                         february="/home/noobzik/Documents/ESGI/5A/S1/5-AWS/projet/NY_Project/yellow_tripdata_2019-02.csv")
        print("date", date)
        print(type(date))
        day = int(date[-2:])
        print("day : ", day)
        print("paths", paths)
        month = int(date[-5:-3])
        monthly_data = pd.read_csv(paths["local_filename"], sep=",", header=0)
        # On passe les colonnes en date
        monthly_data["tpep_pickup_datetime"] = pd.to_datetime(monthly_data["tpep_pickup_datetime"])
        monthly_data["day"] = monthly_data["tpep_pickup_datetime"].dt.day

        daily_data = monthly_data[monthly_data["day"]==day]

        daily_data["tpep_pickup_datetime"] = pd.to_datetime(daily_data["tpep_pickup_datetime"])
        daily_data["day"] = daily_data["tpep_pickup_datetime"].dt.day
        daily_data["month"] = month

        res = daily_data.groupby(
            by=["day", 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count','PULocationID', 'DOLocationID','improvement_surcharge', 'total_amount', 'congestion_surcharge', 'month']).size().reset_index(name='counts')
        res = res[res["month"]==int(month)]
        res.to_csv("/tmp/yellow_cab_3_"+str(month)+"_"+str(day)+".csv", index=False)
        return "/tmp/yellow_cab_3_"+str(month)+"_"+str(day)+".csv"

    @task()
    def load(date, filepath): #
        bucket = AWS_S3_BUCKET_NAME_ML
        csv_buffer = StringIO()
        df = pd.read_csv(filepath)
        df.to_csv(csv_buffer)
        s3_filename = date[:4]+"_"+date[-5:-3]+"_"+date[-2:]+"_"+"df.csv"
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, s3_filename).put(Body = csv_buffer.getvalue())
        return filepath

    @task()
    def clean(filepath):
        os.remove(filepath)
        return 1

        

    paths = extract("{{ yesterday_ds }}")
    filepath = transform("{{ yesterday_ds }}",paths)
    tmp_files = load("{{ yesterday_ds }}",filepath)
    cleaning = clean(tmp_files)


dag_projet_instances = dag_projet()  # Instanciation du DAG
