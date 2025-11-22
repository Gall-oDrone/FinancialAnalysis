import pandas as pd

class DataFrameStore:
    def __init__(self, header=[]):
        self._data_frame = None
        self.header = header
        self.row_index = None

    @property
    def data_frame(self):
        return self._data_frame

    @data_frame.setter
    def data_frame(self, df):
        if df is not None and not isinstance(df, pd.DataFrame):
            raise ValueError("DataFrame must be a pandas DataFrame object")
        self._data_frame = df

    @property
    def data_frame_header(self):
        return self.header
    
    def create_data_frame(self):
        self.data_frame = pd.DataFrame(columns=self.header)

    def update_data_frame(self, data):
        # Check if DataFrame exists
        if self.data_frame is None:
            self.create_data_frame()
            print("DataFrame is not initialized. Creating a DataFrame.")

        # Check if the number of columns matches the length of the data
        if len(self.header) != len(data):
            raise ValueError("Number of columns does not match the length of the data.")

        # Map dictionary keys to DataFrame headers and assign values
        mapped_data = {}
        for header in self.header:
            mapped_data[header] = data.get(header, None)

        # Convert mapped data to DataFrame and concatenate with existing DataFrame
        new_data = pd.DataFrame(mapped_data, index=[0])
        new_data = pd.DataFrame(new_data)

        # Concatenate new data with existing DataFrame
        self.data_frame = pd.concat([self.data_frame, new_data], ignore_index=True)