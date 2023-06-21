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

from datetime import datetime
from typing import List

from logger import logger
from response import Response
from rules_engine import RULES_ENGINE
from webdriver import WebDriver


class PatientProcessor:
    def __init__(self, webdriver: WebDriver, config: dict):
        self.webdriver = webdriver
        self.engine = RULES_ENGINE(config)
        self.pnr_status_manager = PNR_STATUS_MANAGER("config.yaml")

    def process_patient(
            self, patient_data: dict, response: Response
    ):
        patient_data["age"] = self.calculate_patient_age(patient_data["birthday"])

        comments = []

        engine_results = self.engine.execute(
            "rules_context.deal_breakers.rules", patient_data, comments
        )

        if engine_results["passed"] > 0:
            patient_data["pic"] = None
            response.add_patient(
                patient_data=patient_data, comments=" / ".join(engine_results["actions"])
            )
            return

        for pnr in patient_data["pnr"]:
            logger.info(f"Processing PNR : {pnr}")
            patient_data, comments = self.process_pnr(
                patient_data, pnr, comments
            )

        response.add_patient(
            patient_data=patient_data,
            comments=" / ".join(comments),
        )

    def calculate_patient_age(self, birthday: datetime) -> int:
        return relativedelta(datetime.now(), birthday).years

    def process_pnr(
            self, patient_data: dict, pnr: str, comments: List
    ) -> tuple:
        html_page_identify = self.fetch_patient_data(pnr)
        patient_data["pnr_status"] = self.pnr_status_manager.get_pnr_status(html_page_identify)

        if rule_engine.Rule("pnr_status in [1,2]").evaluate(patient_data):
            if rule_engine.Rule("pnr_status == 1").evaluate(patient_data):
                self._check_request_accepted(
                    pnr,
                    patient_data["esame"],
                    patient_data["prestazioni"],
                )

            patient_data["pic"] = self._fetch_pic_from_database(patient_pnr=pnr)

            fetch_telerik_pdf(webdriver=self.webdriver, patient_pic=patient_data["pic"])

            error_codes = check_pdf(
                pic_number=patient_data["pic"],
                insurance_name=patient_data["insurance_name"],
                fiscal_code=patient_data["codice_fiscale"],
            )

            self.engine.execute("rules_context.patient_data.rules", patient_data, comments)

            self.engine.execute(
                "rules_context.pdf_analysis.rules", {"error_codes": error_codes}, comments
            )

        else:
            patient_data["pic"] = None

        return patient_data, comments

    def fetch_patient_data(self, patient_pnr: str) -> str:
        return self.webdriver.get(
            f"https://app.investire-in-italy.it/GestionePNR/CercaQuadro?PNR={patient_pnr}"
        ).text

    def _check_request_accepted(
            self, patient_pnr: str, patient_esame: str, code_prestazioni: str,
    ):
        webdriver = self.webdriver
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

    def _fetch_pic_from_database(self, patient_pnr: str) -> str:
        url_database = r"https://app.investire-in-italy.it/CentriDiagnostici/ControlloQuadri/GridAutorizzazioni_Read"
        payload = {
            "page": 1,
            "pageSize": 50,
            "FieldFilter": patient_pnr,
        }

        result = self.webdriver.post(url_database, payload=payload)
        patient_pic = result.json()["Data"][0]["NumeroAuth"]

        return patient_pic
