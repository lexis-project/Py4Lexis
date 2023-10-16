
from __future__ import annotations
from typing import Generator, Optional
from py4lexis.custom_types.directory_tree import TreeDirectoryObject
from py4lexis.directory_tree import DirectoryTree
from py4lexis.session import LexisSession
from pandas import DataFrame


def printProgressBar(iteration: int, 
                     total: int, 
                     prefix: Optional[str]="", 
                     suffix: Optional[str]="", 
                     decimals: Optional[int]=1, 
                     length: Optional[int]=100, 
                     fill: Optional[str]="█", 
                     printEnd: Optional[str]="\r") -> None:
    """
        Call in a loop to create terminal progress bar. Source: https://stackoverflow.com/questions/3173320/text-progress-bar-in-terminal-with-block-characters

        Parameters
        ----------
        iteration : int
            Current iteration.
        total : int
            Total iterations.
        prefix : str, optional
            Prefix string.
        suffix : str, optional
            Suffix string.
        decimals : int, optional
            Positive number of decimals in percent complete.
        length : int, optional
            Character length of bar.
        fill : str, optional
            Bar fill character.
        printEnd : str, optional
            End character (e.g. "\r", "\r\n").

        Return
        ------
        None    
    """
    if total == 0:
        total = 1 
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + "-" * (length - filledLength)
    print(f"\r{prefix} |{bar}| {percent}% {suffix}", end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


def convert_get_datasets_status_to_pandas(session: LexisSession, 
                                          content: list[dict], 
                                          supress_print: Optional[bool]=False) -> DataFrame | None:
    """
        Convert HTTP response content of GET datasets status from JSON format to pandas DataFrame.
        
        Parameters
        ----------
        session : LexisSession
            Current Lexis Session.
        content : list[str]
            HTTP response content.
        suppress_print : bool, optional
            If True all prints are suppressed.

        Return
        ------
        DataFrame | None
            Status information table of all datasets. None is returned when some errors have occured.
    """

    cols: list[str] = ["Filename", "Project", "TaskState", "TaskResult",
                       "TransferType", "DatasetID", "RequestID", "CreatedAt"]
    
    datasets_table: DataFrame = DataFrame(columns=cols)

    is_error: bool = False
    try:
        session.logging.debug(f"Converting HTTP content from JSON to pandas Dataframe -- PROGRESS")

        if not supress_print:
            print(f"Converting HTTP content from JSON to pandas Dataframe...")

        for i in range(len(content)):
            if "filename" in content[i]:
                filename: str = content[i]["filename"]
            else:
                filename: str = str("UKNOWN Filename")

            if "project" in content[i]:
                project: str = content[i]["project"]
            else:
                project: str = str("UKNOWN Project")                

            if "task_state" in content[i]:
                task_state: str = content[i]["task_state"]
            else:
                task_state: str = str("UKNOWN Task State")

            if "task_result" in content[i]:
                task_result: list[str] | str = content[i]["task_result"]
            else:
                task_result: str = str("UKNOWN Task Result")

            if "transfer_type" in content[i]:
                transfer_type: str = content[i]["transfer_type"]
            else:
                transfer_type: str = str("UKNOWN Transfer Type")

            if "dataset_id" in content[i]:
                dataset_id: str = content[i]["dataset_id"]
            else:
                dataset_id: str = str("UKNOWN DatasetID")

            if "request_id" in content[i]:
                request_id: str = content[i]["request_id"]
            else:
                request_id: str = str("UKNOWN RequestID")

            if "created_at" in content[i]:
                created_at: str = content[i]["created_at"]
            else:
                created_at: str = str("UKNOWN Creation Date")
            
            datasets_table.loc[i] = [filename, project, task_state, task_result,
                                     transfer_type, dataset_id, request_id, created_at]
        
    except:
        is_error = True
        session.logging.error(f"Unexpected error while converting datasets' upload status from content to pandas DataFrame.")
        
        if not supress_print:
            print(f"Unexpected error while converting  datasets' upload status from content to pandas DataFrame.")

    if is_error:
        return None
    else:
        return datasets_table
 

def convert_get_all_datasets_to_pandas(session: LexisSession, 
                                       content: list[dict],
                                       supress_print: Optional[bool]=False) -> DataFrame | None:
    """
        Convert HTTP response content of GET all datasets from JSON format to pandas DataFrame.
        
        Parameters
        ----------
        session : LexisSession
            Current Lexis Session.
        content : list[str]
            HTTP response content.
        suppress_print : bool, optional
            If True all prints are suppressed.

        Return
        ------
        DataFrame | None
            Information table of all datasets. None is returned when some errors have occured.
    """

    cols: list[str] = ["Title", "Access", "Project", "Zone", "InternalID", "CreationDate",
                        "Owner", "Creator", "Contributor", "Publisher", "PublicationYear", 
                        "ResourceType", "Compression", "Encryption"]
    
    datasets_table: DataFrame = DataFrame(columns=cols)

    is_error: bool = False
    try:
        session.logging.debug(f"Converting HTTP content from JSON to pandas Dataframe -- PROGRESS")

        if not supress_print:
            print(f"Converting HTTP content from JSON to pandas Dataframe...")

        for i in range(len(content)):
            if "title" in content[i]["metadata"]:
                title: list[str] | str = content[i]["metadata"]["title"]
                if type(title) == list:
                    if len(title) <= 1:
                        if len(title) == 1:
                            title: str = title[0]
                        else: 
                            title: str = str("UKNOWN Title")
                    else:
                        tmp_title: str = ""
                        for i, title_i in enumerate(title):
                            if i < (len(title) - 1):
                                tmp_title = tmp_title + title_i + " "
                            else:
                                tmp_title = tmp_title
                        title = tmp_title
            else:
                title: str = str("UKNOWN Title")

            if "access" in content[i]["location"]:
                access: str = content[i]["location"]["access"]
            else:
                access: str = str("UKNOWN Access")

            if "project" in content[i]["location"]:
                project: str = content[i]["location"]["project"]
            else:
                project: str = str("UKNOWN Project")

            if "zone" in content[i]["location"]:
                zone: str = content[i]["location"]["zone"]
            else:
                zone: str = str("UKNOWN Zone")

            if "internalID" in content[i]["location"]:
                internalID: str = content[i]["location"]["internalID"]
            else:
                internalID: str = str("UKNOWN InternalID")

            if "CreationDate" in content[i]["metadata"]:
                creation_date: str = content[i]["metadata"]["CreationDate"]
            else:
                creation_date: str = str("UKNOWN Creation Date")

            if "owner" in content[i]["metadata"]:
                owner: list[str] = content[i]["metadata"]["owner"]
            else:
                owner: list[str] = ["UKNOWN Owner"]

            if "creator" in content[i]["metadata"]:
                creator: list[str] = content[i]["metadata"]["creator"]
            else:
                creator: list[str] = ["UKNOWN Creator"]

            if "contributor" in content[i]["metadata"]:
                contributor: list[str] = content[i]["metadata"]["contributor"]
            else:
                contributor: list[str] = ["UKNOWN Contributor"]
            
            if "publisher" in content[i]["metadata"]:
                publisher: list[str] = content[i]["metadata"]["publisher"]
            else:
                publisher: list[str] = ["UKNOWN Publisher"]

            if "publicationYear" in content[i]["metadata"]:
                publication_year: list[str] | str = content[i]["metadata"]["publicationYear"]
                if type(publication_year) == list:
                    if len(publication_year) <= 1:
                        if len(publication_year) == 1:
                            publication_year: str = publication_year[0]
                        else:
                            publication_year: str = str("UKNOWN Publication Year")
                    else:
                        tmp_year: str = ""
                        for i, year_i in enumerate(publication_year):
                            if i < (len(publication_year) - 1):
                                tmp_year = tmp_year + year_i + " "
                            else:
                                tmp_year = tmp_year
                        publication_year = tmp_year
            else:
                publication_year: str = str("UKNOWN Publication Year")

            if "resourceType" in content[i]["metadata"]:
                resource_type: list[str] | str = content[i]["metadata"]["resourceType"]
                if type(resource_type) == list:
                    if len(resource_type) <= 1:
                        if len(resource_type) == 1:
                            resource_type: str = resource_type[0]
                        else:
                            resource_type: str = str("UKNOWN Resource Type")
                    else:
                        tmp_resource_type: str = ""
                        for i, resource_type_i in enumerate(resource_type):
                            if i < (len(resource_type) - 1):
                                tmp_resource_type = tmp_resource_type + resource_type_i + " "
                            else:
                                tmp_resource_type = tmp_resource_type
                        resource_type = tmp_resource_type
            else:
                resource_type: list = [str("UKNOWN Resource Type")]

            if "compression" in content[i]["metadata"]:
                compression: str = content[i]["metadata"]["compression"]
            else:
                compression: str = str("UKNOWN Compression")

            if "encryption" in content[i]["metadata"]:
                encryption: str = content[i]["metadata"]["encryption"]
            else:
                encryption: str = str("UKNOWN Encryption")


            datasets_table.loc[i] = [title, access, project, zone, internalID, creation_date, owner,
                                     creator, contributor, publisher, publication_year, resource_type,
                                     compression, encryption]
    
    except:
        is_error = True
        session.logging.error(f"Unexpected error while converting information about datasets from content to pandas DataFrame.")
        
        if not supress_print:
            print(f"Unexpected error while converting information about datasets from content to pandas DataFrame.")

    if is_error:
        return None
    else:
        return datasets_table
      

def convert_dir_tree_to_pandas(session: LexisSession, 
                               content: list[dict],
                               supress_print: Optional[bool]=False) -> DataFrame | None:
    """
        Convert HTTP response content of GET list of files in dataset from JSON format to pandas DataFrame.
        
        Parameters
        ----------
        session : LexisSession
            Current Lexis Session.
        content : list[str]
            HTTP response content.
        suppress_print : bool, optional
            If True all prints are suppressed.

        Return
        ------
        DataFrame | None
            List of files in dataset formated into DataFrame table. None is returned when some errors have occured.
    """

    cols: list[str] = ["Filename", "Path", "Size", "CreateTime", "Checksum"]
    
    datasets_table: DataFrame = DataFrame(columns=cols)

    is_error: bool = False
    try:
        session.logging.debug(f"Converting HTTP content from JSON to pandas Dataframe -- PROGRESS")

        if not supress_print:
            print(f"Converting HTTP content from JSON to pandas Dataframe...")
        

        tree_content: TreeDirectoryObject = TreeDirectoryObject(content)
        tree_items: Generator[DirectoryTree, None, None] = DirectoryTree.make_tree(tree_content)
        row_id: int = 0
        for item in tree_items:
            row: list[str | int] | None = item.to_dataframe_row()
            if row is not None:
                datasets_table.loc[row_id] = item.to_dataframe_row()
                row_id = row_id + 1
        
    except:
        is_error = True
        session.logging.error(f"Unexpected error while converting directory tree from content to pandas DataFrame.")
        
        if not supress_print:
            print(f"Unexpected error while converting directory tree from content to pandas DataFrame.")

    if is_error:
        return None
    else:
        return datasets_table  