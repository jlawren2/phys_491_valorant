from my_random_forest_classifier import MyRandomForestClassifier
import random
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn import metrics
import numpy as np
from sklearn.preprocessing import StandardScaler




class Experiment03:

    @staticmethod
    def run():
        """
        Loads the data, sets up the machine learning model, trains the model,
        gets predictions from the model based on unseen data, assesses the
        accuracy of the model, and prints the results.
        :return: None
        """
        train_X, train_y, test_X, test_y = Experiment03.load_data()
        my_forest = MyRandomForestClassifier()

        my_forest.fit(X=train_X, y=train_y)
        predictions = my_forest.predict(X=test_X)

        print("My decision tree predicted:")
        print(predictions)
        print()
        print("The true values were actually:")
        print(test_y.values.flatten())
        print()
        print("The accuracy was:")
        print(Experiment03._get_accuracy(predictions, test_y.values))
        print()
        print(metrics.classification_report(test_y.values, predictions))
    
    @staticmethod    
    def average_col_by_rounds(df, column_name):
        df[[column_name]] = np.divide(df[[column_name]].values, np.add(df[['offense_rounds_won']].values, df[['defense_rounds_won']].values))
        return df

    @staticmethod
    def load_data(filename="complete.csv"):
        """
        Load the data and partition it into testing and training data.
        :param filename: The location of the data to load from file.
        :return: train_X, train_y, test_X, test_y; each as an iterable object
        (like a list or a numpy array).
        """
        data = pd.read_csv(filename, low_memory=False)
        data = data.drop(columns=['Unnamed: 0', 'Unnamed: 0.1']) 
        data = data.dropna()
        unwanted = ['Astra', 'Audio', 'PDT', 'PST', 'changes', 'patch', 'slots', 'tweaks', 'updates', '']
        data['patch'] = data['patch'].str.replace(''.join(unwanted), '')
        #data = data[['team', 'IGN', 'date', 'offense_rounds_won', 'defense_rounds_won', 'AGENTS', 'patch', 'game_result', 'R_total', 'K_total', 'D_total', 'A_total', 'ADR_total', 'ACS_total', 'P_M_ONE_total', 'KAST_total', 'HS_total', 'FK_total', 'FD_total', 'P_M_2_total']]
        data = data[['team', 'IGN', 'date', 'offense_rounds_won', 'defense_rounds_won', 'AGENTS', 'patch', 'game_result', 'R_total', 'K_total', 'D_total', 'A_total', 'ADR_total', 'ACS_total', 'P_M_ONE_total', 'KAST_total', 'HS_total', 'FK_total', 'FD_total', 'P_M_2_total']]
        import valorant_cleanup_functions as VCF
        data['agent_numeric'] = data['AGENTS'].apply(VCF.add_role).values
        print(np.unique(data['AGENTS']))
        print(np.unique(data['agent_numeric']))
        total_cols = ['K_total', 'D_total', 'A_total', 'FK_total', 'FD_total']
        for col in total_cols:
            data = Experiment03.average_col_by_rounds(data, col)
        data = data.drop(columns=['offense_rounds_won', 'defense_rounds_won'])

        #for column in data[['AGENTS']].columns:
            #data = Experiment03.categorical_to_numeric(data, column)
        
        for column in data[['HS_total', 'KAST_total']].columns:
            data = Experiment03.parse_percentage(data, column)

        #X = data[['AGENTS', 'R_total', 'K_total', 'D_total', 'A_total', 'ADR_total', 'ACS_total', 'P_M_ONE_total', 'KAST_total', 'HS_total', 'FK_total', 'FD_total', 'P_M_2_total']]
        X = data[['game_result', 'agent_numeric', 'R_total', 'K_total', 'D_total', 'A_total', 'ADR_total', 'ACS_total', 'P_M_ONE_total', 'KAST_total', 'HS_total', 'FK_total', 'FD_total', 'P_M_2_total']]

        X = Experiment03.clean_dataset(X)
        Y = X[['agent_numeric']]

        X = X.drop(columns=['agent_numeric'])
        X = X.astype(float).values
        
        #scaler = StandardScaler()
        #standardized_x = scaler.fit_transform(X)
        #from sklearn.decomposition import PCA
        #pca = PCA(n_components=2)
        #principalComponents = pca.fit_transform(X)
        #principalDf = pd.DataFrame(data = principalComponents, columns = ['principal component 1', 'principal component 2'])


        #finalDf = pd.concat([principalDf, data[['game_result']]], axis = 1)
        #print(finalDf)
        #finalDf = finalDf.dropna().astype(float)
        #X = finalDf[['principal component 1', 'principal component 2']]
        #Y = finalDf[['game_result']]
        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=.8, train_size=.05)
        return X_train, Y_train, X_test, Y_test
    
    @staticmethod
    def parse_percentage(df, col):
        df[col] = df[col].str[:-1]
        return df
    
    @staticmethod
    def clean_dataset(df):
        assert isinstance(df, pd.DataFrame), "df needs to be a pd.DataFrame"
        df.dropna(inplace=True)
        indices_to_keep = ~df.isin([np.nan, np.inf, -np.inf]).any(axis=1)
        return df[indices_to_keep].astype(np.float64)
        

    @staticmethod
    def categorical_to_numeric(df, column):
        """
        Converts categorical data to numeric
        :df: the dataframe you wish to modify
        :column: the column from the dataframe you wish to modify
        :return: none simply modifies the passed dataframe
        """
        print(df[column])
        df[column] = pd.factorize(df[column])[0]
        df[column]
        return df


    
    @staticmethod
    def _get_accuracy(pred_y, true_y):
        """
        Calculates the overall percentage accuracy.
        :param pred_y: Predicted values.
        :param true_y: Ground truth values.
        :return: The accuracy, formatted as a number in [0, 1].
        """
        number_of_agreements = 0
        number_of_pairs = len(true_y)

        for individual_prediction_value, individual_truth_value in zip(pred_y, true_y):
            if individual_prediction_value == individual_truth_value:
                number_of_agreements += 1

        accuracy = number_of_agreements / number_of_pairs

        return accuracy


if __name__ == "__main__":
    # Run the experiment 10 times.
    # Common bugs:
    # (1) If the output is identical each time, it means there's an
    # error with your randomized sample selection for training vs testing.
    # (2) If the accuracy is low then there is either a flaw in your model or
    # the y values are not correctly associated with their corresponding x samples.
    for _ in range(1):
        Experiment03.run()
