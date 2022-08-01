# Consumes traffic data work items

from distutils.log import log
from robot.api import logger
from RPA.HTTP import HTTP
from RPA.Tables import Tables
from  robot.libraries.BuiltIn import BuiltIn
import shared


workitems = shared.WorkItems()
json = shared.JSON()
http = shared.HTTP()
bi = BuiltIn()
post_url = "https://robocorp.com/inhuman-insurance-inc/sales-system-api"

def process_traffic_data():
    #workitems.get_input_work_item()
    payload = workitems.get_work_item_payload()
    traffic_data = payload[shared.workitem_key]
    if (validate_traffic_data(traffic_data=traffic_data)):
        post_traffic_data_to_sales(traffic_data=traffic_data)
    else:
        logger.warn("Invalid traffic data")
        workitems.release_input_work_item(state=shared.State.FAILED, exception_type=shared.Error.BUSINESS, code= "INVALID_TRAFFIC_DATA", message="Invalid traffic data: " + str (traffic_data))

def validate_traffic_data(traffic_data):
    country = json.get_value_from_json(doc=traffic_data, expr="country")
    valid = eval("len('"+country+"') == 3")
    return valid

def post_traffic_data_to_sales(traffic_data):
    http.create_session(alias="postRequest", url=post_url)
    response = None
    try:
        response= http.post_on_session(alias="postRequest", url=post_url, json=traffic_data)
        handle_API_response(response, traffic_data)
    except:
        logger.error("Traffic data post failed")
        workitems.release_input_work_item(state=shared.State.FAILED, exception_type=shared.Error.APPLICATION, code="Traffic_data_post_failed", message="failed")

def handle_API_response(response, traffic_data):
    if (response.status_code == 200):
        workitems.release_input_work_item(shared.State.DONE)
        logger.info("Work item processed", also_console=True)
    else:
        logger.error("Traffic data post failed")
        workitems.release_input_work_item(state=shared.State.FAILED, exception_type=shared.Error.APPLICATION, code="Traffic_data_post_failed", message=response.reason)

def consume_items():
    logger.info("Consume items started", also_console=True)
    i = 1
    workitems.for_each_input_work_item(process_traffic_data)

if __name__ == "__main__":
    consume_items()