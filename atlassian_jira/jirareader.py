from datetime import datetime
import re
from typing import Any, Dict, List, Tuple

from llama_index.core.readers.base import BaseReader
from llama_index.core.schema import Document

from saia_ingest.sanitization import preprocess_text
from jira.resources import PropertyHolder, CustomFieldOption, User, Issue

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

    def __init__(self, email: str, api_token: str, server_url: str, debug:bool = False) -> None:
        from jira import JIRA
        self.jira = JIRA(basic_auth=(email, api_token), server=f"https://{server_url}")
        self.MIN_TEXT_LENGTH = 100
        self.debug = debug
        self.custom_tag_exclude_pattern = ["[CHART] Date of First Response", "[CHART] Time in Status", "Rank"]

    def from_iso_format(self, timestamp_str):        
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f%z")
        formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M")
        return formatted_timestamp

    def get_all_custom_fields(self, pattern: str) ->  dict[str, str]:
        custom_list = []
        custom_str = ""
        troubleshooting_issues = ["BI-10392", "GEST-39589", "MESA-107000", "MESA-116419", "MESA-116405", "MESA-116304", "PLA-23725", "PLA-23591", "SOPSI-6531", "SOPSI-5299", "SOPSI-6226", "FOR-22187", "LOG-11334", "LOG-11304"]

        all_fields = self.jira.fields()
        nameMap = {self.jira.field['name']:self.jira.field['id'] for self.jira.field in all_fields}

        for troubleshooting_issue in troubleshooting_issues:
            try:
                issue = self.jira.issue(troubleshooting_issue)
                custom_list = self.get_custom_fields(pattern, nameMap, issue)
            except Exception as e:
                # Element not valid in current issue
                if self.debug:
                    print(e)
                pass
        return custom_list

    def get_custom_fields(self, pattern: str, nameMap: dict[Any, Any], issue: Issue) -> dict[str, str]:

        custom_dict = {}
        for value in nameMap.keys():
            if nameMap[value].startswith(pattern):
                getvalue = None
                try:
                    getvalue = getattr(issue.fields, nameMap[value])
                    if getvalue is not None:

                        if isinstance(getvalue, list) and len(getvalue) > 0:
                            getvalue = getvalue[0]

                        strValue = ""
                        if isinstance(getvalue, str):
                            strValue = getvalue
                        elif isinstance(getvalue, CustomFieldOption):
                            strValue = getvalue.value
                        elif isinstance(getvalue, PropertyHolder) or isinstance(getvalue, User):
                            if hasattr(getvalue, 'value'):
                                strValue = getvalue.value
                            elif hasattr(getvalue, 'displayName'):
                                strValue = getvalue.displayName
                            elif hasattr(getvalue, 'requestType'):
                                strValue = getvalue.requestType.name
                            elif hasattr(getvalue, 'name'):
                                strValue = getvalue.name
                            else:
                                if self.debug:
                                    print('skip', value, getvalue)
                                continue
                        else:
                            if self.debug:
                                print('unknown', value, getvalue)
                            continue
                        if strValue not in [None, '[]', ''] and value not in self.custom_tag_exclude_pattern:
                            custom_dict[value] = strValue
                except Exception as e:
                    pass
        return custom_dict

    def custom_fields_str(self, lst: dict[str, str]) -> str:
        custom_fields_template = ""
        for key, value in lst.items():
            custom_fields_template += f"{key}: {value}\n"
        return custom_fields_template

    def load_data(self, query: str, startAt=0, maxResults=1000, total=0) -> (List[Document], int):
        relevant_issues = self.jira.search_issues(query, maxResults=maxResults, startAt=startAt)
        total = relevant_issues.total

        issues = []

        all_fields = self.jira.fields()
        nameMap = {self.jira.field['name']:self.jira.field['id'] for self.jira.field in all_fields}

        for issue in relevant_issues:
            # Iterates through only issues and not epics

            if True:# "parent" in (issue.raw["fields"]):
                assignee = ""
                reporter = ""
                resolution = ""
                epic_key = ""
                epic_summary = ""
                epic_descripton = ""

                comments = ""

                if issue.fields.assignee:
                    assignee = issue.fields.assignee.displayName

                if issue.fields.reporter:
                    reporter = issue.fields.reporter.displayName

                if issue.fields.resolution:
                    resolution = issue.fields.resolution.name

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

                custom_list = self.get_custom_fields("customfield_", nameMap, issue)

                footer_text1 = f"Assignee: {assignee}\nReporter: {reporter}\nStatus: {status}\nType: {issue_type}\nPriority: {priority} \nResolution: {resolution}"
                footer_text2 = f"{labels_str}\nCreated: {created_at}\nUpdated: {updated_at}\nProject: {project}"
                footer_text3 = self.custom_fields_str(custom_list)
                text = f"{summary}\n{description}\n{comments}\n{footer_text1}\n{footer_text2}\n\n{footer_text3}"

                text = preprocess_text(text)

                extra_info=safe_value_dict({
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
                })
                extra_info.update(custom_list)

                # text, extra_info
                issues.append(
                    Document(
                        text=text,
                        extra_info=extra_info
                    )
                )

        return issues, total

    def load_langchain_documents(self, **load_kwargs: Any) -> Tuple[List[Any], int]:
        """Load data in LangChain document format."""
        docs, total = self.load_data(**load_kwargs)
        lc_docs = [d.to_langchain_format() for d in docs]
        return lc_docs, total

