import os
import psycopg2
import pandas as pd
from psycopg2 import sql, Error
from dotenv import load_dotenv, dotenv_values 

# loading variables from .env file
load_dotenv() 

class PgConn:
    # Class variables for database connection parameters
    dbname = os.getenv("PGDBNAME")
    user = os.getenv("PGDBUSER")
    password = os.getenv("PGDBPASS")
    host = os.getenv("PGDBHOST")
    port = os.getenv("PGDBPORT")

    def __init__(self, tablename=None, dbname=None, user=None, password=None, host=None, port=None):
        # Initialize class variables
        self.dbname = dbname or PgConn.dbname
        self.user = user or PgConn.user
        self.password = password or PgConn.password
        self.host = host or PgConn.host
        self.port = port or PgConn.port

        # Instance variables
        self.connection = None
        self.open_connection()
        self.tablename = tablename
        
    @property
    def tablename(self):
        return self._tablename
    
    @tablename.setter
    def tablename(self, value):
        self._tablename = value
        print(f"Table name set to: {self._tablename}")
        
    def set_table(self, tablename):
        # Dynamically set the tablename
        self.tablename = tablename

    def open_connection(self):
        try:
            if self.connection is None or self.connection.closed != 0:
                self.connection = psycopg2.connect(
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port
                )
                print("Connection to the database successful!")
            else:
                print("Connection is already open.")
        except Exception as e:
            print(f"Error: Unable to connect to the database. {e}")
    
    def is_connection_closed(self):
        if self.connection is None:
            return True
        return self.connection.closed != 0

    def reopen_connection(self):
        if self.is_connection_closed():
            self.open_connection()
        else:
            print("Connection is already open.")
            
    def create_table(self, table_query):
        try:
            cursor = self.connection.cursor()
            # Define your table schema here
            create_table_query = table_query

            # Check if table already exists
            check_table_query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{self.tablename}');"
            
            cursor.execute(check_table_query)
            table_exists = cursor.fetchone()[0]
            if table_exists:
                print(f"Table '{self.tablename}' already exists.")
            else:
                # Print the table query for debugging
                print(f"Executing query: {create_table_query}")

                cursor.execute(create_table_query)

                # Commit the transaction
                self.connection.commit()

                print(f"Table {self.tablename} created successfully!")

                print("Table created successfully!")
        except Exception as e:
            print(f"Error: Unable to create the table. {e}")
        finally:
            cursor.close()

    def delete_table(self):
        cursor = self.connection.cursor()

        try:
            drop_table_sql = f"DROP TABLE IF EXISTS {self.tablename}"
            cursor.execute(drop_table_sql)
            self.connection.commit()
            print(f"The table '{self.tablename}' has been deleted successfully.")
        except Error as e:
            print("Error:", e)
            self.connection.rollback()
        finally:
            cursor.close()

    def delete_all_rows(self):
        cursor = self.connection.cursor()

        try:
            delete_rows_sql = f"DELETE FROM {self.tablename}"
            cursor.execute(delete_rows_sql)
            self.connection.commit()
            print(f"All rows from the table '{self.tablename}' have been deleted successfully.")
        except Exception as e:
            print("Error:", e)
            self.connection.rollback()
        finally:
            cursor.close()

    def save_to_postgres(self, row_data, header):
        print("Saving to postgres db...", end='', flush=True)
        cursor = self.connection.cursor()
        if not isinstance(row_data, dict):
            row_dict = dict(zip(header, row_data))
        else:
            row_dict = row_data
        insert_sql = self.build_insert_sql(row_dict)
        try:
            cursor.execute(insert_sql, list(row_dict.values()))
            self.connection.commit()
            print("done")
        except Exception as e:
            print("error on saving data to pg:", e, "\n row_dict:", row_dict)
            print("failed")
            self.connection.rollback()
        finally:
            cursor.close()

    def build_insert_sql(self, row_dict):
        # Dynamically build the INSERT INTO SQL statement
        return sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING").format(
            sql.Identifier(self.tablename),
            sql.SQL(", ").join(map(sql.Identifier, row_dict.keys())),
            sql.SQL(", ").join(sql.Placeholder() for _ in range(len(row_dict)))
        )

    def get_stocks_prices(self, book_names=[]):
        cursor = self.connection.cursor()
        try:
            if book_names:
                query = sql.SQL("SELECT * FROM {} WHERE book IN {}").format(
                    sql.Identifier(self.tablename),
                    sql.Literal(tuple(book_names))
                )
            else:
                query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(self.tablename))

            cursor.execute(query)
            data = cursor.fetchall()

            columns = ["ref", "book", "date", "open", "high", "low", "close", "adj_close", "volume"]
            df = pd.DataFrame(data, columns=columns)

            return df
        except Exception as e:
            print(f"Error: Unable to retrieve data from the database. {e}")
            return None
        finally:
            cursor.close()
            
    def get_financial_news(self):
        cursor = self.connection.cursor()
        try:
            
            # Specify the column names for uniqueness
            unique_columns = ["headline", "summary"]

            # Construct the SQL query to select distinct rows based on unique columns
            column_names_str = ', '.join(unique_columns)
            query = f"SELECT DISTINCT ON ({column_names_str}) * FROM {self.tablename};"

            # Execute the query
            cursor.execute(query)

            # Fetch all records
            data = cursor.fetchall()

            columns = ["id", "source", "category", "headline", "href", "summary", "content", "datetime"]
            df = pd.DataFrame(data, columns=columns)
            return df

        except Exception as e:
            print(f"Error: Unable to retrieve data from the database. Error is: {e}")
            return None
        finally:
            cursor.close()

    def get_column_values(self, column_names):
        cursor = self.connection.cursor()
        try:
            # Construct the SELECT query dynamically based on the column names provided
            query = sql.SQL("SELECT {} FROM {}").format(
                sql.SQL(', ').join(map(sql.Identifier, column_names)),
                sql.Identifier(self.tablename)
            )
            cursor.execute(query)
            data = cursor.fetchall()

            # Extract values for each column
            column_values = {}
            for i, column_name in enumerate(column_names):
                values = [row[i] for row in data]  # Extract values for the current column
                column_values[column_name] = values

            return column_values
        except Exception as e:
            print(f"Error: Unable to retrieve data from the database. {e}")
            return None
        finally:
            cursor.close()
    
    def filter_and_eliminate_duplicates(self, tablename, column_names):
        if self.connection is None or self.connection.closed != 0:
            print("Connection is not open. Please open the connection first.")
            return

        cursor = self.connection.cursor()

        try:
            # Construct the SQL query
            column_names_str = ', '.join(column_names)
            query = f"SELECT DISTINCT ON ({column_names_str}) * FROM {tablename};"

            # Execute the query
            cursor.execute(query)

            # Fetch all records
            records = cursor.fetchall()

            # Print the filtered and de-duplicated records
            for record in records:
                print(record)

        except psycopg2.Error as e:
            print("Error:", e)
            self.connection.rollback()
        finally:
            cursor.close()

    def get_unique_attributes(self):
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SELECT DISTINCT * FROM {self.tablename};")
            attributes = [desc[0] for desc in cursor.description]
            print("Unique attributes:", attributes)
        except psycopg2.Error as e:
            print(f"Error: Unable to fetch unique attributes. {e}")
        finally:
            cursor.close()

    def delete_records_by_date(self, date_strings):
        cursor = self.connection.cursor()
        try:
            for date_string in date_strings:
                cursor.execute(f"DELETE FROM {self.tablename} WHERE datetime = %s;", (date_string,))
                self.connection.commit()
                print(f"Deleted records for date: {date_string}")
        except psycopg2.Error as e:
            print(f"Error: Unable to delete records. {e}")
        finally:
            cursor.close()
            
    def delete_records_by_ids(self, id_list):
        cursor = self.connection.cursor()

        try:
            # Generate the SQL query to delete records by ids
            query = f"DELETE FROM {self.tablename} WHERE id IN %s"
            cursor.execute(query, (tuple(id_list),))
            self.connection.commit()
            print("Records deleted successfully!")
        except Exception as e:
            print(f"Error: Unable to delete records from the database. {e}")
            self.connection.rollback()
        finally:
            cursor.close()
            
    def init_db(self, table_query):
        if not self.connection:
            return

        # Create the table (if not exists)
        self.create_table(table_query)
        return self.connection

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("Connection closed.")

# Example usage:
# pg_conn = PgConn(tablename="your_custom_table_name", dbname="your_custom_db_name", user="your_custom_user")
# pg_conn.init_db("your_custom_table_creation_query")
# pg_conn.set_table("another_custom_table_name")
# pg_conn.save_to_postgres(row_data, header)
# # Perform other operations using pg_conn
# pg_conn.close_connection()