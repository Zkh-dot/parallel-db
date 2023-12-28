import pandas as pd
import pickle
import os
from ..logger import get_logger
from logging import Logger
# Import the necessary libraries
# from .sklearn.linear_model import LinearRegression
import numpy as np


# so far i have no idea how to use this class
# class TimePredictor:
#     def __init__(self):
#         self.linear_model = LinearRegression()
#         self.periodic_model = None

#     def fit_linear_model(self, symbols, time):
#         X = np.array(symbols).reshape(-1, 1)
#         y = np.array(time)
#         self.linear_model.fit(X, y)

#     def fit_periodic_model(self, datetime, time, period):
#         X = np.array(datetime).reshape(-1, 1)
#         y = np.array(time)
#         # Convert datetime to radians for periodicity
#         X_rad = 2 * np.pi * X / period
#         # Create features using Fourier series
#         X_features = np.column_stack((np.cos(X_rad), np.sin(X_rad)))
#         self.periodic_model = LinearRegression()
#         self.periodic_model.fit(X_features, y)

#     def predict(self, symbols, datetime, period):
#         linear_prediction = self.linear_model.predict(np.array(symbols).reshape(-1, 1))
#         datetime_rad = 2 * np.pi * np.array(datetime) / period
#         periodic_features = np.column_stack((np.cos(datetime_rad), np.sin(datetime_rad)))
#         periodic_prediction = self.periodic_model.predict(periodic_features)
#         return linear_prediction + periodic_prediction



class TimePredictor:
    """
    A class that predicts the time based on the given file_name, symbols, and datetime.

    Attributes:
        history (pandas.DataFrame): A DataFrame to store the history of predictions.

    Methods:
        __init__(self, logger: Logger, history='history.csv'): Initializes the TimePredictor object.
        predict(self, file_name: str, symbols=None, datetime=None) -> tuple: Predicts the time based on the given file_name, symbols, and datetime.
        remember(self, file_name: str, time, symbols=None, datetime=None): Records a prediction in the history.
        save(self): Saves the history to a CSV file_name.
        __del__(self): Saves the history when the object is deleted.
    """

    history = pd.DataFrame(columns=['file_name', 'datetime', 'symbols', 'time'])

    def __init__(self, logger: Logger = None, history_path='history.csv', model_path = 'model.pkl'):
        """
        Initializes the TimePredictor object.

        Args:
            * logger (Logger): The logger object for logging messages.
            * history_path (str): better don't.
            * model_path (str): better don't.

        Returns:
            None
        """
        self.history_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), history_path)
        self.model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), model_path)
        if logger:
            self.__logger = logger
        else:
            self.__logger = get_logger(self.__class__.__name__, log_consol=False, log_file=False, draw_progress=False)
        try:
            self.history = pd.read_csv(self.history_path)
        except:
            self.__logger.info('Не знающий своего прошлого, не имеет будущего!')
        if os.path.exists(self.model_path):
            with open(self.model_path, 'rb') as f:
                self.model = pickle.load(f)
        else:
            self.__logger.info("Ожидайте средненькое время!")
            self.model = None

    def predict(self, file_name: str, symbols=None, datetime=None) -> tuple:
        """
        Predicts the time based on the given file_name, symbols, and datetime.

        Args:
            * file_name (str): The file_name to predict the time for.
            * symbols (optional): The symbols to consider for prediction.
            * datetime (optional): The datetime to consider for prediction.

        Returns:
            tuple: A tuple containing the prediction result. 
            First element is the status code, second is the predicted time.
        """
        file_name.replace('\n', ' ')
        if datetime and symbols:
            return (0,)
        elif not file_name:
            self.__logger.error('Не указан файл!')
            return (0,)
        else:
            
            if self.history[self.history['file_name'] == file_name].shape[0] > 0:
                return (2, self.history[self.history['file_name'] == file_name]['time'].mean())

            else:
                return (0,)

    def remember(self, file_name: str, time, symbols=None, datetime=None):
        """
        Records a prediction in the history.

        Args:
            * file_name (str): The file_name for which the prediction was made.
            * time: The predicted time.
            * symbols (optional): The symbols considered for the prediction.
            * datetime (optional): The datetime considered for the prediction.

        Returns:
            None
        """
        file_name.replace('\n', ' ')
        self.__logger.debug("-----> {}".format(file_name))
        self.history = pd.concat([self.history, pd.DataFrame({'file_name': file_name, 'datetime': datetime, 'symbols': symbols, 'time': time}, index=[0])])
        # self.history.append({'file_name': file_name, 'datetime': datetime, 'symbols': symbols, 'time': time}, ignore_index=True) 

    def save(self):
        """
        Saves the history to a CSV file.

        Returns:
            None
        """
        try:
            self.history.to_csv(self.history_path)
        except ImportError:
            self.__logger.warning("python did not wait 4me ¯\_(ツ)_/¯")
        except Exception as e:
            self.__logger.error(e)
            raise

    def __del__(self):
        """
        Saves the history when the object is deleted.

        Returns:
            None
        """
        self.__logger.warning("Your tables are done, finishing kernel jobs")
        self.save()