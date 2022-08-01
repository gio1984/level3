# Produce traffic data work items 

from ast import Return
from RPA.Tables import Tables
from RPA.Robocorp.WorkItems import WorkItems
from robot.api import logger
from yaml import load
import shared

http = shared.HTTP()
tables = Tables()
json = shared.JSON()
workitems = WorkItems()
traffic_path = "output/traffic.json"
country_key = "SpatialDim"
gender_key = "Dim1"
rate_key = "NumericValue"
year_key = "TimeDim"

def download_data(url_data):
    http.download(url= url_data, target_file= traffic_path, overwrite=True)

def load_traffic_as_table():
    traffic_json = json.load_json_from_file (traffic_path)
    table = tables.create_table(traffic_json["value"])
    return table

def filter_and_sort_data(input_table):
    tables.filter_table_by_column(table= input_table, column=rate_key, operator="<", value=5.0)
    tables.filter_table_by_column(table= input_table, column=gender_key, operator="==", value="BTSX")
    tables.sort_table_by_column(table=input_table, column=year_key,ascending=False)
    return input_table

def get_latest_data_by_country(input_table):
    groupped_table = tables.group_table_by_column(table=input_table, column=country_key)
    data_by_country = []
    for group in groupped_table:
        first_row = tables.pop_table_row(group)
        data_by_country.append(first_row)
    return data_by_country

def create_work_items_payloads(traffic_data):
    payloads = []
    for row in traffic_data:
        payload = { "country": row[country_key], "year": row[year_key], "rate": row[rate_key] }
        payloads.append(payload)
    return payloads

def save_work_items(payloads):
    workitems.get_input_work_item()
    for payload in payloads:
        save_work_item_payload(payload)

def save_work_item_payload(payload):
        work_item = workitems.create_output_work_item()
        workitems.set_work_item_variable(shared.workitem_key , payload)
        work_item.save()

def produce_work_items():
    logger.info("Producer started", also_console=True)
    download_data("https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json")
    traffic_table = load_traffic_as_table()
    logger.info("Traffic data loaded", also_console=True)
    traffic_filtered_table = filter_and_sort_data(traffic_table)
    traffic_filtered_table = get_latest_data_by_country(traffic_filtered_table)
    logger.info("Data filtered and groupped by", also_console=True)
    payloads = create_work_items_payloads(traffic_filtered_table)
    logger.info("Payloads created", also_console=True)
    save_work_items(payloads)
    logger.info("Work items saved", also_console=True)

if __name__ == "__main__":
    produce_work_items()