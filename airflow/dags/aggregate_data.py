import os
import boto3
import botocore
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
AWS_S3_BUCKET_NAME = Variable.get("AWS_S3_BUCKET_NAME")
default_args = {
    'owner': 'tom',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}


@dag(DAG_NAME, default_args=default_args, schedule_interval="0 0 * * *", start_date=days_ago(2))
def dag_projet():
    """
    Ce DAG est permet de calculer de manière journaliere le CA moyen des chauffeurs de taxi par jour
    """

    # Charge les données depuis S3
    @task()
    def extract(
            date):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name="eu-west-1"
        )
        month = date[-5:-3]
        year = date[0:5]
        s3_filename = 'yellow_tripdata_' + year + '-' + month + '.csv'
        local_filename = '/tmp/' + s3_filename
        s3.download_file('projet-airflow-airbnb-esgi-2021', s3_filename,
                         local_filename)  # TODO change bucket name and file name, utiliser un try,except pour dl le fichier?
        try:
            s3.Bucket(AWS_S3_BUCKET_NAME).download_file('projet-airflow-airbnb-esgi-2021', s3_filename,
                                                        local_filename)
        except botocore.exceptions.ClientError as e:
            print("Returned error S3 : " + e.response['Error']['Code'])

        return dict(local_filename=local_filename)

    @task()
    def transform(date,
                  paths=None):
        if paths is None:  # Pour tester
            paths = dict(local_filename='/tmp/yellow_tripdata_2019-01.csv')
        print("paths", paths)
        # On charge le fichier en local
        monthly_data = pd.read_csv(paths["local_filename"], sep=",", header=0)

        # On passe les colonnes en date
        monthly_data["tpep_pickup_datetime"] = pd.to_datetime(monthly_data["tpep_pickup_datetime"])
        monthly_data["day"] = monthly_data["tpep_pickup_datetime"].dt.day

        # On recupere uniquement le jour qui nous interesse
        day = date[-2:]
        daily_data = monthly_data[monthly_data["day"] == day]
        daily_data["month"] = int(date[-5:-3])

        # Finalement, on groupe par vendeur et jour
        res = daily_data[["VendorID", "day", "month", "total_amount"]].groupby(
            by=["VendorID", "day"]).mean().reset_index()

        res.to_csv("/tmp/yellow_cab.csv", index=False)
        return "/tmp/yellow_cab.csv"

    @task()
    def load(filepath):  # TODO faire l'insertion dans la table dynamoDB
        # On lit le csv temporaire
        df = pd.read_csv(filepath).head(n=100)

        # On instancie notre table dynamoDB
        dynamodb = boto3.resource(
            "dynamodb",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name="eu-west-1"
        )
        table = dynamodb.Table("daily_ca")

        def put_row(row, batch):
            batch.put_item(
                Item={
                    'vendorid': int(row["VendorID"]),
                    'day': int(row["day"]),
                    'month': int(row["month"]),
                    'total_amount': Decimal(str(row["total_amount"]))
                }
            )

        with table.batch_writer() as batch:
            df.apply(lambda x: put_row(x, batch), axis=1)

    paths = extract("{{ yesterday_ds }}")
    filepath = transform("{{ yesterday_ds }}", paths)
    load(filepath)


DAG_NAME_2 = DAG_NAME + "_2"


@dag(DAG_NAME, default_args=default_args, schedule_interval="0 0 * * *",
     start_date=days_ago(2))  # TODO changer le start date
def dag_projet_2():
    """
    Ce DAG est notre réponse à la première problématique du sujet # TODO update ça
    """

    # Charge les données depuis S3
    @task()
    def extract_2():  # TODO passer la date en parametre et recuperer uniquement le fichier du mois
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
    def transform_2(paths=None):  # TODO supprimer le deuxieme mois, enlever le concat, rename les variables
        if paths is None:
            paths = dict(
                january="/home/noobzik/Documents/ESGI/5A/S1/5-AWS/projet/NY_Project/yellow_tripdata_2019-02.csv",
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
        new_df = new_df[["day", "PULocationID", "true_count"]].rename(
            {'PULocationID': 'LocationID', 'true_count': 'count'}, axis=1)
        # res = pd.concat([grouped_id_day_january, grouped_id_day_february], ignore_index=True)
        new_df.to_csv("/tmp/yellow_cab_2.csv", index=False)
        return "/tmp/yellow_cab_2.csv"

    @task()
    def load_2(filepath):  # TODO faire l'insertion dans la table dynamoDB
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
dag_projet_instances_2 = dag_projet_2()
