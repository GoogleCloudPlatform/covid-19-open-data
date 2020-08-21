import os
import requests
import sys
from google.cloud import firestore
from googleapiclient.discovery import build
from google.cloud import secretmanager

# Add our library utils to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lib.constants import ISSUES_API_URL
from lib.error_logger import ErrorLogger


class GithubIssueHandler(ErrorLogger):
    """ Handles posting issues to github using the personal access token stored in SecretsManager. """

    def __init__(self, gcs_project_name):
        self._username = "automation"  # This is ignored because we use an access token.
        self._gcs_project_name = gcs_project_name
        self._password = self._get_github_token()

    def _error_group_to_github_issue(self, error_group):
        title = "Automated error report"
        body = error_group["representative"]["message"]
        return {"title": title, "body": body}

    def _get_github_token(self):
        client = secretmanager.SecretManagerServiceClient()
        name = client.secret_version_path(self._gcs_project_name, "github-token", "latest")
        response = client.access_secret_version(name)
        return response.payload.data

    def post_error_to_github(self, error_group):
        """ Returns the issue url if successfully posted, else raises a ConnectionError exception """
        session = requests.Session()
        session.auth = (self._username, self._password)
        response = session.post(ISSUES_API_URL, json=self._error_group_to_github_issue(error_group))
        if response.status_code != 201:
            self.errlog("Could not create github issue.", status_code=response.status_code)
            raise ConnectionError(error_message)
        return response.json()["html_url"]


def register_new_errors(gcs_project_name):
    """ If new error groups are reported, log an issue on github with the details """
    service = build("clouderrorreporting", "v1beta1")
    errors_past_day = (
        service.projects()
        .groupStats()
        .list(projectName="projects/{}".format(gcs_project_name), timeRange_period="PERIOD_1_DAY")
        .execute()
    )
    gh_issue_handler = GithubIssueHandler(gcs_project_name)
    db = firestore.Client()
    errors_db = db.collection("errors")
    for error_group in errors_past_day["errorGroupStats"]:
        group_id = error_group["group"]["groupId"]
        if int(error_group["count"]) < 2:
            # Don't add one-off errors to the db
            continue
        doc = errors_db.document(group_id)
        if not doc.get().exists and error_group["group"]["resolutionStatus"] == "OPEN":
            try:
                error_group["group"]["trackingIssues"] = [
                    {"url": gh_issue_handler.post_error_to_github(error_group)}
                ]
                error_group["group"]["resolutionStatus"] = "ACKNOWLEDGED"
            except ConnectionError:
                # Could not create an issue
                # Don't add it to our known errors db, we can retry on the next scheduled job.
                continue
            # Now set it to acknowledged and link to the issue in Cloud Error Reporting.
            service.projects().groups().update(
                name=error_group["group"]["name"], body=error_group["group"]
            ).execute()
        doc.set(error_group)


if __name__ == "__main__":
    import os

    register_new_errors(os.getenv("GOOGLE_CLOUD_PROJECT"))
