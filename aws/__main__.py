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
import time
import yaml

from datetime import datetime
from pathlib import Path

from common import catch_all_exceptions
from logger import logger
from process_patient import process_patient
from process import PatientProcessor
from response import Response
from webdriver import WebDriver


class ConfigHandler:
    def __init__(self, config_file_path: str):
        """
        Initialize the ConfigHandler.

        Parameters:
        - config_file_path: The path to the configuration INI file.
        """
        self.config_file_path = config_file_path
        self.config = configparser.ConfigParser()
        self.yaml_config = {}

    @property
    def path_dir_data(self) -> str:
        """
        Get the directory path for data.

        Returns:
        - The directory path for data.
        """
        path_dir_data = self.config["path"]["path_dir_data"]
        if "{date}" in path_dir_data:
            path_dir_data = path_dir_data.format(date=str(datetime.now().date()))
        os.makedirs(path_dir_data, exist_ok=True)

        return path_dir_data

    @property
    def path_file_output_excel(self) -> str:
        """
        Get the file path for the output Excel file.

        Returns:
        - The file path for the output Excel file.
        """
        return os.path.expanduser(os.path.join(self.path_dir_data, self.config["path"]["filename_output"]))

    @property
    def path_file_output_zip(self) -> str:
        """
        Get the file path for the output ZIP file.

        Returns:
        - The file path for the output ZIP file.
        """
        path_file_output_zip = os.path.join(
            os.path.dirname(self.path_file_output_excel),
            ".".join(
                os.path.basename(os.path.basename(self.path_file_output_excel)).split(".")[:-1]
            )
            + ".zip",
        )
        return path_file_output_zip

    @property
    def path_file_input(self) -> str:
        """
        Get the file path for the input file.

        Returns:
        - The file path for the input file.
        """
        return os.path.expanduser(
            os.path.join(self.path_dir_data, self.config["path"]["filename_input"])
        )

    @property
    def path_exec_firefox(self) -> str:
        """
        Get the path to the Firefox executable.

        Returns:
        - The path to the Firefox executable.
        """
        return self.config["path"]["path_exec_firefox"]

    @property
    def webdriver_headless(self) -> bool:
        """
        Check if the WebDriver should run in headless mode.

        Returns:
        - True if the WebDriver should run in headless mode, False otherwise.
        """
        return self.config.getboolean("webdriver", "headless")

    @property
    def zip_with_password(self) -> bool:
        """
        Check if ZIP files should be created with a password.

        Returns:
        - True if ZIP files should be created with a password, False otherwise.
        """
        return self.config.getboolean("path", "zip_with_password")

    def load_config(self):
        """
        Load the configuration from the INI file.
        """
        self.config.read(self.config_file_path)

    def get_value(self, section: str, key: str) -> str:
        """
        Get a value from the configuration.

        Parameters:
        - section: The section name.
        - key: The key name.

        Returns:
        - The value corresponding to the section and key.
        """
        return self.config.get(section, key)

    def load_yaml_config(self, yaml_file_path: str):
        """
        Load the YAML configuration.

        Parameters:
        - yaml_file_path: The path to the YAML configuration file.
        """
        with open(yaml_file_path, "r") as file:
            try:
                self.yaml_config = yaml.safe_load(file)
            except (yaml.YAMLError, FileNotFoundError) as e:
                raise ValueError(f"Error loading YAML file: {e}")

    def get_yaml_value(self, key: str) -> dict:
        """
        Get a value from the YAML configuration.

        Parameters:
        - key: The key name.

        Returns:
        - The value corresponding to the key in the YAML configuration.
        """
        return self.yaml_config.get(key, {})


@catch_all_exceptions
def main():
    """Main entry point. Read and process all parameters and env vars before calling the main script."""
    # Load config file
    start_time = time.time()

    config_handler = ConfigHandler("config.ini")
    config_handler.load_config()
    config_handler.load_yaml_config("config.yaml")

    # Parse input from hospital
    patients = _parse_input_from_hospital(
        path_file_input=config_handler.config_file_path,
        config_csv_mapping=config_handler.yaml_config
    )

    process_patients(
        patients=patients,
        username=os.getenv("HEALTHFORCE_XWYZ_USERNAME"),
        password=os.getenv("HEALTHFORCE_XWYZ_PASSWORD"),
        filename_output=config_handler.path_file_output_excel,
        webdriver_headless=config_handler.webdriver_headless,
        path_dir_output=config_handler.path_dir_data,
        path_exec_firefox=config_handler.path_exec_firefox,
        zip_with_password=config_handler.zip_with_password,
        config_yaml=config_handler.yaml_config,
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
    patient_processor = PatientProcessor(webdriver=webdriver, config=config_yaml)
    for patient in patients:
        logger.info(f'Processing patient : "{patient}"')
        patient_processor.process_patient(patient, response)

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


def _parse_input_from_hospital(path_file_input: str, config_csv_mapping: dict) -> list[dict]:
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
