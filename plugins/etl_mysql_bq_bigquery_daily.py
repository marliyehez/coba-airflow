from google.oauth2.credentials import Credentials
from pipeline import GenericPipelineInterface
from google.oauth2 import service_account
from google.cloud import bigquery
from pathlib import Path

import pandas as pd
import logging

path = Path(__file__)
logging.basicConfig(filename='ingestion.log',
                    filemode='a',
                    format='%(asctime)s - %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

class MySQLToBigQueryPipeline(GenericPipelineInterface):
    
    def __init__(self, project_id: str, source_table: str, dest_table: str, credentials_src: Credentials, credentials_dest: Credentials):
        self.source_table = source_table
        self.dest_table = dest_table
        self.project_id = project_id
        self.credentials_src = credentials_src
        self.credentials_dest = credentials_dest

        self.transform_func_dict = {
            'Dim_Customer': self.__transform_dim_customer,
            'Dim_Date': self.__transform_dim_date,
            'Dim_Employee': self.__transform_dim_employee,
            'Dim_Office': self.__transform_dim_office,
            'Dim_Product': self.__transform_dim_product,
            'Fact_Sales': self.__transform_fact_sales
        }


    def run(self):
        logging.info(f"Extracting the data from {self.source_table}")
        source = self.extract()
        logging.info("Transforming")
        transformed_result = self.transform(source)
        logging.info(f"Load data to {self.dest_table}")
        logging.info(f"Project id {self.project_id}")
        self.load(transformed_result)
    
    
    def extract(self) -> pd.DataFrame:
        client = bigquery.Client.from_service_account_json(self.credentials_src)
        query = f"SELECT * FROM `{self.source_table}`"
        query_job = client.query(query)
        df = query_job.result().to_dataframe()
        return df


    def transform(self, source: pd.DataFrame) -> pd.DataFrame:
        df = source.copy()
        dest_table_name = self.dest_table.split('.')[1]
        transform_func = self.transform_func_dict[dest_table_name]
        df = transform_func(df)
        return df


    def load(self, transformed_result: pd.DataFrame):
        credentials = service_account.Credentials.from_service_account_file(self.credentials_dest)

        transformed_result.to_gbq(
            destination_table=self.dest_table,
            project_id=self.project_id,
            if_exists='replace',
            credentials=credentials
        )


    def __transform_dim_customer(self, df: pd.DataFrame) -> pd.DataFrame:

        df['contactFullName'] = df['contactFirstName'] + ' ' + df['contactLastName']
        df.drop(['contactFirstName','contactLastName','addressLine2','salesRepEmployeeNumber'], axis=1, inplace=True)
        new_order = ['customerNumber','customerName','contactFullName','phone','addressLine1','city','state','postalCode','country','creditLimit']
        df = df.reindex(columns=new_order)
        df[['customerNumber']] = df[['customerNumber']].astype('string')
        df[['creditLimit']] = df[['creditLimit']].astype('float')
        return df

    
    def __transform_dim_date(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        intinya liat min max, bikin value untuk every day (freq='D)

        ini gak kepake source table-nya, jadi `df` juga gakkepake,
        malah di replace
        '''
        start_date = df['orderDate'].min()
        end_date = df['orderDate'].max()
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')

        df = pd.DataFrame({
            'Date': date_range,
            'Day': date_range.day,
            'Month': date_range.month,
            'Quarter': date_range.quarter,
            'Year': date_range.year,
            'Weekday': date_range.weekday < 5,
            'Holiday': False
        })

        df.insert(0, 'Date_Key', range(1, len(df)+1))

        return df

    
    def __transform_dim_employee(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        yang office key keknya masih salah
        tapi keknya asal ada job aja
        '''
        df["Employee_Key"] = range(1, len(df) + 1)
        df["Employee_Name"] = df["firstName"] + " " + df["lastName"]

        rename_cols_dict = {
            'employeeNumber': 'Employee_Number', 'email': 'Email',
            'jobTitle': 'Job_Title', 'officeCode': 'Office_Key'
        }
        df = df.rename(columns=rename_cols_dict)

        use_cols = ['Employee_Key', 'Employee_Number', 'Employee_Name', 'Email', 'Job_Title', 'Office_Key']
        df = df[use_cols]

        return df


    def __transform_dim_office(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Rename doang sama tambah serial kolom 'Office_Key'
        '''
        rename_cols_dict = {
            'Office_Code': 'officeCode', 'City': 'city', 'Phone': 'phone',
            'AddressLine1': 'addressLine1', 'AddressLine2': 'addressLine2',
            'State': 'state', 'Country': 'country', 'PostalCode': 'postalCode',
            'Territory': 'territory'
        }
        df = df.rename(columns=rename_cols_dict)

        df.insert(0, 'Office_Key', range(1, len(df)+1))

        return df

    
    def __transform_dim_product(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Rename doang sama tambah serial kolom 'Product_Key'
        '''
        rename_cols_dict = {
            'Product_Code': 'productCode', 'Product_Name': 'productName',
            'Product_Line': 'productLine', 'Product_Scale': 'productScale',
            'Product_Vendor': 'productVendor', 'Product_Description': 'productDescription',
            'Quantity_In_Stock': 'quantityInStock', 'Buy_Price': 'buyPrice', 'MSRP': 'MSRP'
        }
        df = df.rename(columns=rename_cols_dict)

        df.insert(0, 'Product_Key', range(1, len(df)+1))

        return df


    def __transform_fact_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        kurang puas sama kerjaannya mas reza :(
        '''
        return df
    
