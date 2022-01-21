import os
import json
import sys
import numpy as np
import pandas as pd
import pathlib
import tarfile

if __name__ == "__main__":
    model_path = f"/opt/ml/processing/model/model.tar.gz"
    with tarfile.open(model_path, "r:gz") as tar:
        tar.extractall("./model")
    import tensorflow as tf

    model = tf.keras.models.load_model("./model/1")
    datasets_path = "/opt/ml/processing/datasets"
    
    X_test = pd.read_csv(os.path.join(datasets_path, "X_test.csv")).to_numpy()
    y_test = pd.read_csv(os.path.join(datasets_path, "y_test.csv")).to_numpy()
    scores = model.evaluate(X_test, y_test, verbose=2)
    print("Accuracy :", scores[1])
    
    report_dict = {
        "classification_metrics": {
            "accuracy": {"value": scores[1], "standard_deviation": "NaN"},  # scores[1] = accuracy classe positive
        },
    }

    output_dir = "/opt/ml/processing/evaluation"
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

    evaluation_path = f"{output_dir}/evaluation.json"
    with open(evaluation_path, "w") as f:
        f.write(json.dumps(report_dict))