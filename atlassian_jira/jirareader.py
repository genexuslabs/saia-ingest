from datetime import datetime
import re
from typing import Any, Dict, List

from llama_index.readers.base import BaseReader
from llama_index.readers.schema.base import Document
from llama_index.bridge.langchain import Document as LCDocument

from saia_ingest.sanitization import preprocess_text

def safe_value_dict(dict_obj):
    for key, value in dict_obj.items():
        if isinstance(value, (str, int, float)):
            dict_obj[key] = value
        elif isinstance(value, list):
            # Convert lists to strings
            dict_obj[key] = ", ".join(map(str, value))
        elif value is None:
            # Replace None with a default string
            dict_obj[key] = ""
        else:
            # Convert other types to strings
            dict_obj[key] = str(value)
    return dict_obj


class JiraReader(BaseReader):
    """Jira reader. Reads data from Jira issues from passed query.

    Args:
        email (str): Jira email.
        api_token (str): Jira API token.
        server_url (str): Jira server url.
    """

    def __init__(self, email: str, api_token: str, server_url: str) -> None:
        from jira import JIRA
        self.jira = JIRA(basic_auth=(email, api_token), server=f"https://{server_url}")
        self.MIN_TEXT_LENGTH = 100

    def from_iso_format(self, timestamp_str):        
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f%z")
        formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M")
        return formatted_timestamp

    def load_data(self, query: str, startAt=0, maxResults=1000, total=0) -> (List[Document], int):
        relevant_issues = self.jira.search_issues(query, maxResults=maxResults, startAt=startAt)
        total = relevant_issues.total

        issues = []

        for issue in relevant_issues:
            # Iterates through only issues and not epics
            if True:# "parent" in (issue.raw["fields"]):
                assignee = ""
                reporter = ""
                epic_key = ""
                epic_summary = ""
                epic_descripton = ""

                comments = ""

                if issue.fields.assignee:
                    assignee = issue.fields.assignee.displayName

                if issue.fields.reporter:
                    reporter = issue.fields.reporter.displayName

                if issue.fields.comment and issue.fields.comment.comments:
                    for comment in issue.fields.comment.comments:
                        comments += f"{comment.body}\n"
                
                if "parent" in (issue.raw["fields"]):
                    if  issue.raw["fields"]["parent"]["key"]:
                        epic_key = issue.raw["fields"]["parent"]["key"]

                    if issue.raw["fields"]["parent"]["fields"]["summary"]:
                        epic_summary = issue.raw["fields"]["parent"]["fields"]["summary"]

                    if issue.raw["fields"]["parent"]["fields"]["status"]["description"]:
                        epic_descripton = issue.raw["fields"]["parent"]["fields"]["status"][
                            "description"
                        ]
                summary = issue.fields.summary
                description = issue.fields.description

                created_at = self.from_iso_format(issue.fields.created)
                updated_at = self.from_iso_format(issue.fields.updated)
                labels = issue.fields.labels
                labels_str = "Labels: " + ', '.join(labels) if labels else ""
                status = issue.fields.status.name
                assignee = assignee
                reporter = reporter
                project = issue.fields.project.name
                issue_type = issue.fields.issuetype.name
                priority = issue.fields.priority.name

                footer_text1 = f"Assignee: {assignee}\nReporter: {reporter}\nStatus: {status}\nType: {issue_type}\nPriority: {priority}"
                footer_text2 = f"{labels_str}\nCreated: {created_at}\nUpdated: {updated_at}\nProject: {project}"
                text = f"{summary}\n{description}\n{comments}\n{footer_text1}\n{footer_text2}"

                text = preprocess_text(text)

                # text, extra_info
                issues.append(
                    Document(
                        text=text,
                        extra_info=safe_value_dict(
                            {
                                "id": issue.id,
                                "name": issue.id,
                                "description": summary,
                                "url": issue.permalink(),
                                "source": issue.permalink(),
                                "created_at": created_at,
                                "updated_at": updated_at,
                                "labels": labels,
                                "status": status,
                                "assignee": assignee,
                                "reporter": reporter,
                                "project": project,
                                "issue_type": issue_type,
                                "priority": priority,
                                "epic_key": epic_key,
                                "epic_summary": epic_summary,
                                "epic_description": epic_descripton,
                                "domain": 'jira',
                            }
                        ),
                    )
                )

        return issues, total

    def load_langchain_documents(self, **load_kwargs: Any) -> (List[LCDocument], int):
        """Load data in LangChain document format."""
        docs, total = self.load_data(**load_kwargs)
        lc_docs = [d.to_langchain_format() for d in docs]
        return lc_docs, total

