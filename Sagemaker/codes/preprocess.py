import os
import pandas as pd
import numpy as np
import tarfile
import joblib
import json


from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer

try:
    from sagemaker_containers.beta.framework import (
    content_types, encoders, env, modules, transformer, worker, server)
except ImportError:
    pass

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

#label_column = 'spam'
label_column = 'duration'

base_dir = "/opt/ml/processing"
base_output_dir = "/opt/ml/output/"
    
if __name__ == "__main__":
    print("ls",os.listdir("/opt/ml/processing/input"))
    dataset = pd.read_csv(f"{base_dir}/input/primary.csv",low_memory=False)

    #X = dataset["sms"]
    #y = dataset["spam"]
    print("dataset : ", dataset)
    print( pd.to_timedelta(dataset["duration"]))
    dataset["duration"] = pd.to_timedelta(dataset["duration"]).dt.total_seconds()
    

    numeric_features = ["improvement_surcharge","duration"]
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())])
    categorical_features=["day","PULocationID","DOLocationID","month","pickup_hour","dropoff_hour"]
    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='most_frequent')),
        ('onehot', OneHotEncoder(handle_unknown='ignore'))])

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features)],
        remainder="drop")
    preprocessor.fit(dataset)
    print("dataset after preprocessor : ", dataset)
    X = dataset[["day","PULocationID","DOLocationID","month","improvement_surcharge","pickup_hour","dropoff_hour"]]
    y = dataset["duration"]
    print("X : ",X)
    print("y : ",y)
    #X[["day","PULocationID","DOLocationID","month","pickup_hour","dropoff_hour"]] = tf.keras.utils.to_categorical(X[["day","PULocationID","DOLocationID","month","pickup_hour","dropoff_hour"]])
    #y = dataset["duration"]

    """
    MAX_WORDS = 2000  # On ne sélectionne que les 2000 mots les plus fréquents dans X
    MAX_LEN = 60  # Un maximum de 60 mots par SMS

    tokenizer = CountVectorizer(max_features=MAX_WORDS)
    # On calibre en encodage par dictionnaire avec le tokenizer
    tokenizer.fit(X)

    sequences = []

    def construct_sequences(text):
        tokens = text.split()
        seq = [0] * MAX_LEN
        for i, token in enumerate(tokens):
            if i >= MAX_LEN:
                break
            seq[i] = tokenizer.vocabulary_[token] if token in tokenizer.vocabulary_ else 0
        sequences.append(seq)

    X.apply(construct_sequences)

    sequences = np.asarray(sequences)
    print("Première ligne de la matrice :\n", sequences[0])
    print("Taille de la matrice :", sequences.shape)
    
    X_train, X_test, y_train, y_test = train_test_split(
        sequences, y, test_size=0.3, random_state=40
    )
    """
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=40
    )
    pd.DataFrame(X_train).to_csv(f"{base_dir}/datasets/X_train.csv", index=False)
    pd.DataFrame(y_train).to_csv(f"{base_dir}/datasets/y_train.csv", index=False)
    pd.DataFrame(X_test).to_csv(f"{base_dir}/datasets/X_test.csv", index=False)
    pd.DataFrame(y_test).to_csv(f"{base_dir}/datasets/y_test.csv", index=False)

    
    joblib.dump(preprocessor, "tokenizer.joblib")
    with tarfile.open(f"{base_dir}/tokenizer/tokenizer.tar.gz", "w:gz") as tar_handle:
        tar_handle.add(f"tokenizer.joblib")
        
def input_fn(input_data, content_type):
    """Parse input data payload

    We currently only take csv input. Since we need to process both labelled
    and unlabelled data we first determine whether the label column is present
    by looking at how many columns were provided.
    """
    if content_type == 'application/json':
        # Read the raw input data as CSV.
        parsed_data = json.loads(input_data)
        return parsed_data["samples"]
    else:
        raise ValueError("{} not supported by script !".format(content_type))


def output_fn(prediction, accept):
    """Format prediction output

    The default accept/content-type between containers for serial inference is JSON.
    We also want to set the ContentType or mimetype as the same value as accept so the next
    container can read the response payload correctly.
    """
    if accept == "application/json":
        return worker.Response(json.dumps(prediction, cls=NpEncoder), mimetype=accept)
    else:
        raise RuntimeException("{} accept type is not supported by this script.".format(accept))


def predict_fn(input_data, model):
    """Preprocess input data

    We implement this because the default predict_fn uses .predict(), but our model is a preprocessor
    so we want to use .transform().

    The output is returned in the following order:

        rest of features either one hot encoded or standardized
    """
    MAX_LEN = 60
    sequences = []
    
    for sample in input_data:
        print("sample : ",sample)
        """
        tokens = sample.split()
        seq = [0] * MAX_LEN
        for i, token in enumerate(tokens):
            if i >= MAX_LEN:
                break
            seq[i] = model.vocabulary_[token] if token in model.vocabulary_ else 0
        sequences.append(seq)
        """
        seq = model.transform(sample)
        print("seq : ",seq)
        sequences.append(seq)
    
    return sequences


def model_fn(model_dir):
    """Deserialize fitted model
    """
    preprocessor = joblib.load(os.path.join(model_dir, "tokenizer.joblib"))
    return preprocessor