""" Main entry point for XWYZ batch script scrapping
For a given processed CSV file of patients, fetch all associated data and interact with the XWYZ portal to submit them
into their database.
When the script ends, it produces a zip file containing :
  * An anonymized Excel sheet containing scrapped patient data with an associated comment if an issue has occured.
  * All available PDF for each patient. The name of each PDF is the PIC of the associated patient.
After the script has ended, send the email with the zipped file back to the hospital.
"""
import ast
import configparser
import csv
import os
from datetime import datetime
from pathlib import Path
import time

import yaml
from dateutil.relativedelta import relativedelta

from common import catch_all_exceptions
from logger import logger
from process_patient import process_patient
from response import Response
from webdriver import WebDriver


@catch_all_exceptions
def main():
    """Main entry point. Read and process all parameters and env vars before calling the main script."""
    # Load config file
    start_time = time.time()

    config = configparser.ConfigParser()
    config.read(Path(__file__).parent.parent / "config.ini")

    # Setup paths
    path_dir_data = config["path"]["path_dir_data"]
    if "{date}" in path_dir_data:
        path_dir_data = path_dir_data.format(date=datetime.now().date().__str__())
    os.makedirs(path_dir_data, exist_ok=True)
    path_file_output_excel = os.path.expanduser(
        os.path.join(path_dir_data, config["path"]["filename_output"])
    )
    path_file_output_zip = os.path.join(
        os.path.dirname(path_file_output_excel),
        ".".join(
            os.path.basename(os.path.basename(path_file_output_excel)).split(".")[:-1]
        )
        + ".zip",
    )

    with open("config.yaml", "r") as file:
        try:
            config_yaml = yaml.safe_load(file)
        except (yaml.YAMLError, FileNotFoundError) as e:
            raise ValueError(f"Error loading YAML file: {e}")

    # Parse input from hospital
    patients = _parse_input_from_hospital(
        os.path.expanduser(
            os.path.join(path_dir_data, config["path"]["filename_input"])
        ),
        config_csv_mapping=config_yaml,
    )

    process_patients(
        patients=patients,
        username=os.getenv("HEALTHFORCE_XWYZ_USERNAME"),
        password=os.getenv("HEALTHFORCE_XWYZ_PASSWORD"),
        filename_output=path_file_output_excel,
        webdriver_headless=config.getboolean("webdriver", "headless"),
        path_dir_output=path_dir_data,
        path_exec_firefox=config["path"]["path_exec_firefox"],
        zip_with_password=config.getboolean("path", "zip_with_password"),
        config_yaml=config_yaml,
    )

    # Stop the timer
    end_time = time.time()

    # Calculate the elapsed time
    elapsed_time = end_time - start_time

    # Print the execution time
    print(f"Elapsed time: {elapsed_time} seconds")


def process_patients(
    patients: list[dict],
    username: str,
    password: str,
    filename_output: str,
    webdriver_headless: bool,
    path_dir_output: str,
    path_exec_firefox: str,
    zip_with_password: bool,
    config_yaml: dict,
):
    """Given already cleaned parameters, initialise all interfaces and start processing patients data."""
    # Initialize webdriver & connect to the insurance web portal
    webdriver = WebDriver(
        path_dir_output=path_dir_output,
        path_exec_firefox=path_exec_firefox,
        headless=webdriver_headless,
    )
    login(webdriver, username, password)

    # Initialize a response object to send to the hospital
    response = Response(
        path_file_output=os.path.join(path_dir_output, filename_output),
        zip_with_password=zip_with_password,
    )

    # Process patients batch
    for patient in patients:
        logger.info(f'Processing patient : "{patient}"')
        process_patient(
            webdriver=webdriver,
            patient_data=patient,
            response=response,
            config=config_yaml,
        )

    # Need to call this method otherwise the driver process stay in memory
    webdriver.quit()

    # Prepare & send the response to the hospital
    response.send_mail_to_hospital()


def login(webdriver: WebDriver, username: str, password: str):
    """Login to the portal with a given web session.

    Parameters
    ----------
    webdriver : WebDriver
        current web session containing cookies and shared between actions.
    username : str
        username used to log-in into XWYZ.
    password : str
        password used to log-in into XWYZ.
    """
    url_login = r"https://app.investire-in-italy/CentriDiagnostici/MenuCentri"

    if username == "" or password == "":
        raise ValueError(
            "The username or password are missing, it's not possible to login to the insurance server"
        )

    # Login into XWYZ is a simple POST on a form, they may try to patch it.
    payload = {"UserName": username, "Password": password}
    webdriver.post(url_login, payload=payload)

    # Login must be made twice on this site for some reason
    return webdriver.post(url_login, payload=payload)


def _parse_input_from_hospital(
    path_file_input: str, config_csv_mapping: dict
) -> list[dict]:
    """Parse the CSV file resulting of the processing executed in the hospital virtual machine.
    Assumes that the CSV contains a header as well as matching keys.
    """

    element_array = []
    with open(path_file_input) as fp:
        for row in csv.DictReader(fp):
            item_dict = {}
            for item in config_csv_mapping["mapping"]["csv"]:
                if item["coltype"] == "string":
                    item_dict[item["var_name"]] = row[item["colname"]]
                elif item["coltype"] == "array":
                    item_dict[item["var_name"]] = ast.literal_eval(row[item["colname"]])
                elif item["coltype"] == "bool":
                    item_dict[item["var_name"]] = row[item["colname"]].lower() in (
                        "true",
                    )
                elif item["coltype"] == "date":
                    item_dict[item["var_name"]] = datetime.strptime(
                        row[item["colname"]], item["date_format"]
                    )
                else:
                    raise ValueError("Unknown type found in the YAML config file")
            element_array.append(item_dict)

    return element_array


if __name__ == "__main__":
    main()
