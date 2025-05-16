import os
from dotenv import load_dotenv

load_dotenv()

CLOCKIFY_API: str = f"https://api.clockify.me/api/v1/workspaces/{os.getenv("WORKSPACE")}"
CLOCKIFY_REPORTS: str = f"https://reports.api.clockify.me/v1/workspaces/{os.getenv("WORKSPACE")}"
EXPORTS_PROJECTPLANNING: str = f"https://prod-18.brazilsouth.logic.azure.com:443/workflows/{os.getenv("AUTOMATE_KEY")}/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=HOI6Uar-iubuJmSyKanpGiONs8ScJ2---Q5bW7zBwws"