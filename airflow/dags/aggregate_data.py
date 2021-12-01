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


# TODO


@dag(DAG_NAME, default_args=default_args, schedule_interval="0 0 * * *", start_date=days_ago(2))
def dag_projet():
    """
    Ce DAG est notre réponse à la première problématique du sujet
    """

    # Charge les données depuis S3
    @task()
    def extract():
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
    def transform(paths=None): # TODO Ajouter la deuxieme partie de la question
        if paths is None:
            paths = dict(january="/home/noobzik/Documents/ESGI/5A/S1/5-AWS/projet/NY_Project/yellow_tripdata_2019-02.csv",
                         february="/home/noobzik/Documents/ESGI/5A/S1/5-AWS/projet/NY_Project/yellow_tripdata_2019-02.csv")
        print("paths", paths)
        january = pd.read_csv(paths["january"], sep=",", header=0)
        february = pd.read_csv(paths["february"], sep=",", header=0)
        print(february)
        february["tpep_pickup_datetime"] = pd.to_datetime(february["tpep_pickup_datetime"])
        february["day"] = february["tpep_pickup_datetime"].dt.day

        grouped_id_day_february = february[["VendorID", "day", "total_amount"]].groupby(
            by=["VendorID", "day"]).mean().reset_index()

        january["tpep_pickup_datetime"] = pd.to_datetime(january["tpep_pickup_datetime"])
        january["day"] = january["tpep_pickup_datetime"].dt.day

        grouped_id_day_january = january[["VendorID", "day", "total_amount"]].groupby(
            by=["VendorID", "day"]).mean().reset_index()

        res = pd.concat([grouped_id_day_january, grouped_id_day_february], ignore_index=True)
        res.to_csv("/tmp/yellow_cab.csv", index=False)
        return "/tmp/yellow_cab.csv"

    @task()
    def load(filepath): # TODO faire l'insertion dans la table dynamoDB
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
    filepath = transform(paths)
    load(filepath)


@dag(DAG_NAME, default_args=default_args, schedule_interval="0 0 * * *", start_date=days_ago(2))
def dag_projet_2():
    """
    Ce DAG est notre réponse à la première problématique du sujet
    """

    # Charge les données depuis S3
    @task()
    def extract_2():
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
    def transform_2(paths=None): # TODO Ajouter la deuxieme partie de la question
        if paths is None:
            paths = dict(january="/home/noobzik/Documents/ESGI/5A/S1/5-AWS/projet/NY_Project/yellow_tripdata_2019-02.csv",
                         february="/home/noobzik/Documents/ESGI/5A/S1/5-AWS/projet/NY_Project/yellow_tripdata_2019-02.csv")
        print("paths", paths)
        january = pd.read_csv(paths["january"], sep=",", header=0)
        february = pd.read_csv(paths["february"], sep=",", header=0)
        print(february)
        february["tpep_pickup_datetime"] = pd.to_datetime(february["tpep_pickup_datetime"])
        february["day"] = february["tpep_pickup_datetime"].dt.day
        print(february.PULocationID.max())
        print("----------------------")
        print(february.DOLocationID.max())
        
        january["tpep_pickup_datetime"] = pd.to_datetime(january["tpep_pickup_datetime"])
        january["day"] = january["tpep_pickup_datetime"].dt.day
        
        grouped_id_day_january_1 = january[["DOLocationID", "day"]].groupby(
            by=["DOLocationID", "day"]).size().reset_index(name='counts1')
        
        grouped_id_day_january_2 = january[["PULocationID", "day"]].groupby(
            by=["PULocationID", "day"]).size().reset_index(name='counts2')
        
        grouped_id_day_january_3 = january.loc[january['DOLocationID'] == january['PULocationID']]
        grouped_id_day_january_3 = grouped_id_day_january_3[["PULocationID", "day"]].groupby(
            by=["PULocationID", "day"]).size().reset_index(name='counts3')
        
        grouped_id_day_january_1 = grouped_id_day_january_1.rename({'DOLocationID': 'PULocationID'}, axis=1)
        
        new_df = grouped_id_day_january_1.merge(grouped_id_day_january_2, how='inner', on=['PULocationID', 'day'])
        new_df = new_df.merge(grouped_id_day_january_3, how='inner', on=['PULocationID', 'day'])
        
        new_df['true_count'] = new_df['counts1'] + new_df['counts2'] - new_df['counts3']
        

        print(grouped_id_day_january_1)
        print(grouped_id_day_january_2)
        print(grouped_id_day_january_3)
        print(new_df)
        new_df = new_df[["day","PULocationID","true_count"]].rename({'PULocationID':'LocationID','true_count':'count'},axis=1)
        #res = pd.concat([grouped_id_day_january, grouped_id_day_february], ignore_index=True)
        new_df.to_csv("/tmp/yellow_cab_2.csv", index=False)
        return "/tmp/yellow_cab_2.csv"

    @task()
    def load_2(filepath): # TODO faire l'insertion dans la table dynamoDB
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

    paths = extract_2()
    filepath = transform_2(paths)
    load_2(filepath)
    
    
dag_projet_instances = dag_projet()  # Instanciation du DAG
dag_projet_instances = dag_projet_2() 
