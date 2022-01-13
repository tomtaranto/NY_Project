import os
import boto3
import botocore
import pandas as pd
import pathlib

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
AWS_S3_BUCKET_RAW_NAME = Variable.get("AWS_S3_BUCKET_NAME")
AWS_S3_REGION = Variable.get("AWS_S3_REGION")

default_args = {
    'owner': 'tom',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}
current_dir = pathlib.Path.cwd()
print(current_dir)

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
            region_name=AWS_S3_REGION
        )
        month = date[-5:-3]
        year = date[0:4]
        s3_filename = 'yellow_tripdata_' + year + '-' + month + '.csv'
        local_filename = '/tmp/' + s3_filename
        print("s3_filename",s3_filename)
        if not os.path.isfile(local_filename): # TODO remove after testings
            s3.download_file(AWS_S3_BUCKET_RAW_NAME, s3_filename,
                             local_filename)
        return dict(local_filename=local_filename)

    @task()
    def transform(date,
                  paths=None):
        if paths is None:  # Pour tester
            paths = dict(
                local_filename='~/Documents/ESGI/5A/S1/5-AWS/projet/NY_Project/data/yellow_tripdata_2019-01.csv')
        print("paths", paths)
        # On charge le fichier en local
        monthly_data = pd.read_csv(paths["local_filename"], sep=",", header=0)

        # On passe les colonnes en date
        monthly_data["tpep_pickup_datetime"] = pd.to_datetime(monthly_data["tpep_pickup_datetime"])
        monthly_data["day"] = monthly_data["tpep_pickup_datetime"].dt.day

        # On recupere uniquement le jour qui nous interesse
        day = int(date[-2:])
        print("day : ", day)
        print(monthly_data)
        daily_data = monthly_data[monthly_data["day"] == day]
        daily_data["month"] = int(date[-5:-3])

        # Finalement, on groupe par vendeur et jour
        res = daily_data[["VendorID", "day", "month", "total_amount"]].groupby(
            by=["VendorID", "day"]).mean().reset_index()

        res.to_csv("/tmp/yellow_cab_1_"+str(day)+".csv", index=False)
        return "/tmp/yellow_cab_1_"+str(day)+".csv"

    @task()
    def load(filepath):  # TODO faire l'insertion dans la table dynamoDB
        if filepath is None:
            filepath = '~/PycharmProjects/NY_Project/data/yellow_tripdata_2019-01.csv'
        # On lit le csv temporaire
        df = pd.read_csv(filepath).head(n=100)

        # On instancie notre table dynamoDB
        dynamodb = boto3.resource(
            "dynamodb",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_S3_REGION
        )
        table = dynamodb.Table("esginyprojectdaily1")

        def put_row(row, batch):
            id = str(int(row.name)+int(row["month"])*100+int(float(row["day"]))+1000*int(row["VendorID"]))
            batch.put_item(
                Item={
                    'vendorid': int(row["VendorID"]),
                    'day': int(row["day"]),
                    'month': int(row["month"]),
                    'total_amount': Decimal(str(row["total_amount"])),
                    'id' : id
                }
            )

        with table.batch_writer() as batch:
            df.apply(lambda x: put_row(x, batch), axis=1)
        return filepath

    @task()
    def clean(paths):
        os.remove(paths)
        return 1

    paths = extract("{{ yesterday_ds }}")
    filepath = transform("{{ yesterday_ds }}", paths)
    tmp_files = load(filepath)
    cleaning = clean(tmp_files)
    #paths >> filepath >> load >> cleaning



DAG_NAME_2 = DAG_NAME + "_2"


@dag(DAG_NAME_2, default_args=default_args, schedule_interval="0 0 * * *",
     start_date=days_ago(2))  # TODO changer le start date
def dag_projet_2():
    """
    Ce DAG est notre réponse à la première problématique du sujet # TODO update ça
    """

    # Charge les données depuis S3
    @task()
    def extract_2(
            date):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_S3_REGION
        )
        month = date[-5:-3]
        year = date[0:4]
        s3_filename = 'yellow_tripdata_' + year + '-' + month + '.csv'
        local_filename = '/tmp/' + s3_filename
        print("s3_filename : ",s3_filename)

        if not os.path.isfile(local_filename): # TODO remove after testings
            s3.download_file(AWS_S3_BUCKET_RAW_NAME, s3_filename,
                             local_filename)
        return dict(local_filename=local_filename)

    @task()
    def transform_2(date, paths=None):  # TODO supprimer le deuxieme mois, enlever le concat, rename les variables
        if paths is None:  # Pour tester
            paths = dict(
                local_filename='~/Documents/ESGI/5A/S1/5-AWS/projet/NY_Project/data/yellow_tripdata_2019-01.csv')

        print("paths", paths)
        monthly_data = pd.read_csv(paths["local_filename"], sep=",", header=0)

        monthly_data["tpep_pickup_datetime"] = pd.to_datetime(monthly_data["tpep_pickup_datetime"])
        monthly_data["day"] = monthly_data["tpep_pickup_datetime"].dt.day
        print("date : ", date, date[-2:])
        daily_data = monthly_data[monthly_data["day"] == int(date[-2:])]

        # Recherche des zones de départ
        grouped_id_day_monthly_1 = daily_data[["DOLocationID", "day"]].groupby(
            by=["DOLocationID", "day"]).size().reset_index(name='counts1')

        # REcherche des zones d'arrivée
        grouped_id_day_monthly_2 = daily_data[["PULocationID", "day"]].groupby(
            by=["PULocationID", "day"]).size().reset_index(name='counts2')

        # Recherche des zones avec depart == arrive
        grouped_id_day_monthly_3 = daily_data.loc[daily_data['DOLocationID'] == daily_data['PULocationID']]
        grouped_id_day_monthly_3 = grouped_id_day_monthly_3[["PULocationID", "day"]].groupby(
            by=["PULocationID", "day"]).size().reset_index(name='counts3')

        grouped_id_day_monthly_1 = grouped_id_day_monthly_1.rename({'DOLocationID': 'PULocationID'}, axis=1)

        new_df = grouped_id_day_monthly_1.merge(grouped_id_day_monthly_2, how='inner', on=['PULocationID', 'day'])
        new_df = new_df.merge(grouped_id_day_monthly_3, how='inner', on=['PULocationID', 'day'])

        new_df['true_count'] = new_df['counts1'] + new_df['counts2'] - new_df['counts3']

        new_df = new_df[["day", "PULocationID", "true_count"]].rename(
            {'PULocationID': 'LocationID', 'true_count': 'count'}, axis=1)
        new_df["month"] = int(date[-5:-3])
        new_df.to_csv("/tmp/yellow_cab_2_"+date[-2:]+".csv", index=False)
        return "/tmp/yellow_cab_2_"+date[-2:]+".csv"

    @task()
    def load_2(filepath):  # TODO faire l'insertion dans la table dynamoDB
        if filepath is None:
            filepath = '/tmp/yellow_cab_2.csv'
        df = pd.read_csv(filepath, sep=',', header=0).head(n=100)
        dynamodb = boto3.resource(
            "dynamodb",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_S3_REGION
        )
        table = dynamodb.Table("esginyprojectdaily2")
        def put_row(row, batch):
            index = row.index
            month = row["month"]
            day = row["day"]
            res = batch.put_item(
                Item={
                    'month': int(row["month"]),
                    'day': int(row["day"]),
                    'location': int(row["LocationID"]),
                    'count': Decimal(str(row["count"])),
                    'id' : str(int(row.name)+int(row["month"])*100+int(float(row["day"])))
                }
            )

        with table.batch_writer() as batch:
            df.apply(lambda x: put_row(x, batch), axis=1)
        return filepath

    @task()
    def clean_2(paths):
        os.remove(paths)
        return 1

    paths = extract_2("{{ yesterday_ds }}")
    filepath = transform_2("{{ yesterday_ds }}",paths)
    tmp_files = load_2(filepath)
    cleaning = clean_2(tmp_files)
    #paths >> filepath >> load >> cleaning
    


dag_projet_instances = dag_projet()  # Instanciation du DAG
dag_projet_instances_2 = dag_projet_2()


# Pour run:
#airflow dags backfill --start-date 2019-01-02 --end-date 2019-01-03 --reset-dagruns daily_ml
# airflow tasks test aggregate_data_2 2019-01-02
