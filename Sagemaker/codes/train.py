import os
import tensorflow as tf
import pandas as pd
import argparse

from tensorflow.keras import layers

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--max_len', type=int, default=60)
    parser.add_argument('--max_words', type=int, default=2000)
    parser.add_argument('--datasets', type=str, default=os.environ.get('SM_CHANNEL_DATASETS'))
    parser.add_argument('--sm-model-dir', type=str, default=os.environ.get('SM_MODEL_DIR'))

    return parser.parse_known_args()

if __name__ == "__main__":
    args, _ = parse_args()
    
    print(args.datasets)
    
    X_train = pd.read_csv(os.path.join(args.datasets, "X_train.csv"))
    X_test = pd.read_csv(os.path.join(args.datasets, "X_test.csv"))
    y_train = pd.read_csv(os.path.join(args.datasets, "y_train.csv"))
    y_test = pd.read_csv(os.path.join(args.datasets, "y_test.csv"))
    
    dense = tf.keras.Sequential([
        layers.Dense(32, input_shape=(7,), activation="relu"),
        layers.Dropout(0.1),
        layers.Flatten(),
        layers.Dense(64, activation="tanh"),
        layers.Dense(1, activation="linear")
    ])
    
    dense.compile(
        optimizer='adam',
        loss="mse",
        metrics=['mean_squared_error']
    )
    
    print(dense.summary())
    
    history = dense.fit(
        x=X_train, y=y_train,
        validation_data=(X_test, y_test),
        epochs=2,
        batch_size=24
    )
    dense.save(args.sm_model_dir + '/1')

    """    
    rnn = tf.keras.Sequential([
        layers.Embedding(args.max_words, 50, input_length=args.max_len, input_shape=(args.max_len,)),
        layers.LSTM(64, activation="tanh", input_shape=(args.max_len,1), return_sequences=True),
        layers.Dropout(0.1),
        layers.Flatten(),
        layers.Dense(64, activation="tanh"),
        layers.Dense(1, activation="sigmoid")
    ])
    
    rnn.compile(
        optimizer='adam',
        loss="binary_crossentropy",
        metrics=['accuracy']
    )
    
    print(rnn.summary())
    
    history = rnn.fit(
        x=X_train, y=y_train,
        validation_data=(X_test, y_test),
        epochs=2,
        batch_size=24
    )
    rnn.save(args.sm_model_dir + '/1')
    """