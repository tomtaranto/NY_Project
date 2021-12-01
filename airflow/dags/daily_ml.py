import os
import boto3
import pandas as pd

from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from decimal import Decimal

DAG_NAME = os.path.basename(__file__).replace(".py", "")  # Le nom du DAG est le nom du fichier

AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

default_args = {
    'owner': 'tom',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

@dag(DAG_NAME, default_args=default_args, schedule_interval="0 0 * * *", start_date=days_ago(2)) # TODO start date to change
def dag_projet():
    """
    Ce DAG est notre réponse à la première problématique du sujet # TODO update
    """

    # Charge les données depuis S3
    @task()
    def extract(): # TODO passer la date en parametre, charger uniquement le fichier du mois
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name="eu-west-1"
        )
        s3.download_file('projet-airflow-airbnb-esgi-2021', 'yellow_tripdata_2019-01.csv',
                         '/tmp/yellow_tripdata_2019-01.csv')  # TODO change bucket name
        s3.download_file('projet-airflow-airbnb-esgi-2021', 'yellow_tripdata_2019-02.csv',
                         '/tmp/yellow_tripdata_2019-02.csv')  # TODO change bucket name
        return dict(january="/tmp/yellow_tripdata_2019-01.csv", february="/tmp/yellow_tripdata_2019-02.csv")

    @task()
    def transform(date,paths=None): # TODO faire uniquement pour 1 fichier, renommer les variables
        if paths is None:
            paths = dict(january="/home/noobzik/Documents/ESGI/5A/S1/5-AWS/projet/NY_Project/yellow_tripdata_2019-02.csv",
                         february="/home/noobzik/Documents/ESGI/5A/S1/5-AWS/projet/NY_Project/yellow_tripdata_2019-02.csv")
        print("date", date)
        print(type(date))
        day = date[-2:]
        print("day : ", day)
        print("paths", paths)
        month = date[-5:-3]
        january = pd.read_csv(paths["january"], sep=",", header=0)
        february = pd.read_csv(paths["february"], sep=",", header=0)
        print(february)
        february["tpep_pickup_datetime"] = pd.to_datetime(february["tpep_pickup_datetime"])
        february["day"] = february["tpep_pickup_datetime"].dt.day
        february["month"] = 2
        

        grouped_id_day_february = february.groupby(by=["day", 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count','PULocationID', 'DOLocationID','improvement_surcharge', 'total_amount', 'congestion_surcharge', 'month']).size().reset_index(name='counts')

        january["tpep_pickup_datetime"] = pd.to_datetime(january["tpep_pickup_datetime"])
        january["day"] = january["tpep_pickup_datetime"].dt.day
        january["month"] = 1

        grouped_id_day_january = january.groupby(
            by=["day", 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count','PULocationID', 'DOLocationID','improvement_surcharge', 'total_amount', 'congestion_surcharge', 'month']).size().reset_index(name='counts')
        res = pd.concat([grouped_id_day_january, grouped_id_day_february], ignore_index=True)
        res=res[res["day"]==int(day)]
        res = res[res["month"]==int(month)]
        res.to_csv("/tmp/yellow_cab_3_"+str(month)+"_"+str(day)+".csv", index=False)
        return "/tmp/yellow_cab_3_"+str(month)+"_"+str(day)+".csv"

    @task()
    def load(filepath): # TODO tout changer pour load dans s3. Passer la date en parametre et utiliser la date pour le nom du fichier
        df = pd.read_csv(filepath).head(n=100)
        dynamodb = boto3.resource(
            "dynamodb",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name="eu-west-1"
        )
        table = dynamodb.Table("listings_prices")

        def put_row(row, batch):
            batch.put_item(
                Item={
                    'listing_id': int(row["listing_id"]),
                    'week': int(row["week"]),
                    'price': Decimal(str(row["price"]))
                }
            )

        with table.batch_writer() as batch:
            df.apply(lambda x: put_row(x, batch), axis=1)

    paths = extract()
    filepath = transform("{{ ds }}",paths)
    load(filepath)


dag_projet_instances = dag_projet()  # Instanciation du DAG
