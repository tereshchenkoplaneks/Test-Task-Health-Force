""" Main code retrieving patient data with webdriver requests and HTML scrapping. """
import time
from datetime import datetime
from typing import List

import rule_engine
from dateutil.relativedelta import relativedelta
from rich.console import Console
from selenium.webdriver.common.by import By

from logger import logger
from pnr_status import PNR_STATUS_MANAGER
from read_pdf import check_pdf
from response import Response
from rules_engine import RULES_ENGINE
from telerik_bypass import fetch_telerik_pdf
from webdriver import WebDriver

console = Console()


def process_patient(
    webdriver: WebDriver, patient_data: dict, response: Response, config: dict
):
    """Given clean parameters, process & submit the data for one patient.
    Try to handle as much edge cases as possible. But still missing data and states never seen during production.
    """

    patient_data["age"] = relativedelta(datetime.now(), patient_data["birthday"]).years

    comments = []  # This are the comments that are going to added to the Excel report

    # Go through the rules that moat the rest of the script from running as there's no sufficient data
    engine = RULES_ENGINE(config)
    engine_results = engine.execute(
        "rules_context.deal_breakers.rules", patient_data, comments
    )

    if engine_results["passed"] > 0:
        patient_data["pic"] = None
        response.add_patient(
            patient_data=patient_data, comments=" / ".join(engine_results["actions"])
        )
        return

    #  ------- Continue with the rest of the process ------------

    pnr_status_manager = PNR_STATUS_MANAGER("config.yaml")

    # Even if the patient doesn't need 2 PNR we process all the PNR that we found to be sure that we are not missing something
    for pnr in patient_data["pnr"]:
        logger.info(f"Processing PNR : {pnr}")
        patient_data, comments = process_pnr(
            webdriver, engine, patient_data, pnr, pnr_status_manager, comments
        )

    response.add_patient(
        patient_data=patient_data,
        comments=" / ".join(comments),
    )


def process_pnr(
    webdriver, engine, patient_data: dict, pnr: str, pnr_status_manager, comments: List
) -> tuple:

    # Fetch the patient PNR state once. This page also contains useful data for a lot of states.
    html_page_identify = fetch_patient_data(webdriver, pnr)

    patient_data["pnr_status"] = pnr_status_manager.get_pnr_status(html_page_identify)

    if rule_engine.Rule("pnr_status in [1,2]").evaluate(patient_data):
        # The status allows the download of a PIC

        if rule_engine.Rule("pnr_status == 1").evaluate(patient_data):
            _check_request_accepted(
                webdriver,
                pnr,
                patient_data["esame"],
                patient_data["prestazioni"],
            )

        patient_data["pic"] = _fetch_pic_from_database(webdriver, patient_pnr=pnr)

        # Download PIC
        fetch_telerik_pdf(webdriver=webdriver, patient_pic=patient_data["pic"])

        error_codes = check_pdf(
            pic_number=patient_data["pic"],
            insurance_name=patient_data["insurance_name"],
            fiscal_code=patient_data["codice_fiscale"],
        )

        engine.execute("rules_context.patient_data.rules", patient_data, comments)

        engine.execute(
            "rules_context.pdf_analysis.rules", {"error_codes": error_codes}, comments
        )

    else:
        patient_data["pic"] = None

    engine.execute("rules_context.webportal.rules", patient_data, comments)

    return patient_data, comments


def fetch_patient_data(webdriver: WebDriver, patient_pnr: str) -> str:
    """In a logged session, check for the patient data wrt. the given PNR.

    Parameters
    ----------
    webdriver : WebDriver
        current web session containing cookies and shared between actions.
    patient_pnr : str
        PNR code of the patient
    """

    return webdriver.get(
        f"https://app.investire-in-italy.it/GestionePNR/CercaQuadro?PNR={patient_pnr}"
    ).text


def _retrieve_pic_from_identify_page(html_body: str) -> str:
    patient_pic = html_body.split("ListaAutorizzazioni?filter=")[-1].split('"')[0]
    logger.info(f'Successfully retrieved PIC : "{patient_pic}"')
    return patient_pic


def _fetch_pic_from_database(webdriver: WebDriver, patient_pnr: str) -> str:
    url_database = r"https://app.investire-in-italy.it/CentriDiagnostici/ControlloQuadri/GridAutorizzazioni_Read"
    payload = {
        "page": 1,
        "pageSize": 50,
        "FieldFilter": patient_pnr,
    }

    result = webdriver.post(url_database, payload=payload)
    patient_pic = result.json()["Data"][0]["NumeroAuth"]

    return patient_pic


def _check_request_accepted(
    webdriver: WebDriver,
    patient_pnr: str,
    patient_esame: str,
    code_prestazioni: str,
):
    url_identify_patient = (
        r"https://app.investire-in-italy.it/CentriDiagnostici/ControlloQuadri/Index2"
    )
    webdriver.get(url_identify_patient, backend="selenium")

    # Find where to input patient PNR
    webdriver.find_element(by=By.ID, value="PNR").send_keys(patient_pnr)

    # Submit search. The button is behind an alert, a javascript click can overcome this.
    webdriver.click_js(webdriver.find_element(by=By.ID, value="cercaQuadro"))

    # Accept that this is our patient, at the bottom of the page
    time.sleep(1)
    webdriver.click_js(webdriver.find_element(by=By.ID, value="btnQuadroOK"))

    # Select correct prestazioni from scroller
    time.sleep(1)
    webdriver.find_element(
        By.CSS_SELECTOR, ".k-widget.k-multiselect.k-multiselect-clearable"
    ).click()

    # This allows to try the code that we have in the list and if it doesn't work try the next one
    # This list comes from the categories to chose on the site in order to create a prior-authorization
    codes_to_try = [code_prestazioni, "Visite specialistiche", "Altre prestazioni"]

    for current_code in codes_to_try:
        for item in webdriver.find_elements(by=By.CLASS_NAME, value="k-item"):
            if item.text == current_code:
                break

        # The 'else' part of a for loop is executed when the loop completed normally (i.e., did not encounter a break statement)
        # In this case, we immediately continue with the next iteration of the outer loop, skipping the rest of this iteration
        else:
            continue

        # If we've reached this point, it means the inner loop broke (i.e., we found the item)
        # So we break the outer loop as well
        break
    # The 'else' part of the outer for loop is executed only if the loop completed normally (i.e., we did not find the item with any of the codes)
    else:
        # If we've reached this point, we did not find the item with any of the codes, so we raise an error
        raise ValueError(f"Unable to find prestazioni with any of the provided codes")

    # If we've reached this point, we've found the item and broken both loops
    # So we click the item
    item.click()

    # Check that the prestazioni is possible for this patient
    # TODO: We are assuming for now that the prestazioni is possible, we need to find PNR for which it is not.
    webdriver.click_js(webdriver.find_element(by=By.ID, value="btnVerifica"))

    # Retrieve the message
    # TODO: We still have not got any PNR for which the insurance refuses the patient. So we cannot assess that it
    #  catches error messages
    time.sleep(1)
    message = webdriver.find_elements(by=By.XPATH, value="//h4/following-sibling::p")[
        0
    ].text
    logger.debug(f"Insurance response : {message}")

    # Input the patient ESAME into
    webdriver.find_element(by=By.ID, value="NoteAuth").send_keys(patient_esame)

    # Submit search. The button is behind an alert, a javascript click can overcome this.
    webdriver.click_js(webdriver.find_element(by=By.ID, value="cercaQuadro"))

    # Submit patient to the database
    webdriver.click_js(webdriver.find_element(by=By.ID, value="btnIstruisci"))
