from data_pipeline.etl.base import BaseETL
import requests


class CalEnviroScreenETL(BaseETL):
    LOCATION = "https://oehha.ca.gov/media/downloads/calenviroscreen/document/calenviroscreen40resultsdatadictionaryf2021.zip"

    def etl_name(self):
        return "calenviroscreen"

    def extract(self):
        filename = self.LOCATION.split("/")[-1]
        response = requests.get(self.LOCATION)
        return response.content, filename


if __name__ == "__main__":
    etl = CalEnviroScreenETL()
    etl.run()
