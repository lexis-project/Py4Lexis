
from py4lexis.session import LexisSession
from pandas import DataFrame
import json

def printProgressBar(iteration: int, total: int, prefix: str="", suffix: str="", decimals: int=1, length: int=100, fill: str="█", printEnd: str="\r") -> None:
    """
        Call in a loop to create terminal progress bar. Source: https://stackoverflow.com/questions/3173320/text-progress-bar-in-terminal-with-block-characters

        Parameters
        ----------
        iteration : int, required
            Current iteration.
        total : int, required
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

def convert_content_to_pandas(session: LexisSession, content: list[dict], supress_print: bool=False) -> DataFrame:
    """
        Convert HTTP response content from JSON to pandas DataFrame.
        
        Parameters
        ----------
        session : LexisSession, required
            Current Lexis Session.
        content : list[str], required
            HTTP response content.
        suppress_print : bool, optional
            If True all prints are suppressed.

        Return
        ------
        datasets_table : DataFrame
            Table of all datasets
    """

    cols: list[str] = ["Title", "Access", "Project", "Zone", "InternalID", "CreationDate",
                        "Owner", "Creator", "Contributor", "Publisher", "PublicationYear", 
                        "ResourceType", "Compression", "Encryption"]
    
    datasets_table: DataFrame = DataFrame(columns=cols)

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

    except KeyError as kerr:
        session.logging.debug(f"Converting datasets to list pandas Dataframe -- FAIL")
        session.logging.debug(f"Wrong or missing key '{kerr}' in JSON response content!!!")
        session.logging.debug(f"Printing HTTP request content:")
        session.logging.debug(json.dumps(content, indent=4))
        
        if not supress_print:
            print(f"Wrong or missing key '{kerr}' in JSON response content!!!")
            print(f"Printing HTTP request content:")
            print(json.dumps(content, indent=4))

    return datasets_table