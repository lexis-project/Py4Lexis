from __future__ import annotations
from typing import Any


class TreeDirectoryObject(object):
    """
        Class which holds the directory returned in HTTP content
        needed for the function: py4lexis.cli.get_list_of_files_in_dataset().

        Attributes
        ----------
        directory: dict
            Directory as dictionary returned in the content of HTTP request in the function
            py4lexis.ddi.get_list_of_files_in_dataset().

        Methods
        -------
        is_dir() -> bool
            Returns if the object is directory or not.
    """

    def __init__(self, directory: dict) -> None:
        self.contents: list[dict] = directory["contents"]
        self.type: str = "directory"
        
        if "name" in directory.keys():
            self.name: int = directory["name"] + "/"
        else:
            self.name: str = str("UKNOWN_dir/")
    
    @classmethod
    def is_dir(cls) -> bool:
        """
            As the class is a directory, it returns True.
        """
        return True


class TreeFileObject(object):
    """
        Class which holds the file returned in HTTP content
        needed for the function: py4lexis.cli.get_list_of_files_in_dataset().

        Attributes
        ----------
        file: dict
            File as dictionary returned in the content of HTTP request in the function
            py4lexis.ddi.get_list_of_files_in_dataset().

        Methods
        -------
        is_dir() -> bool
            Returns if the object is directory or not.
    """

    def __init__(self, file: dict) -> None:
        if "checksum" in file.keys():
            if file["checksum"] is None:
                self.checksum: str = "None"  
            else:
                self.checksum: Any = file["checksum"] 
        else:
            self.checksum: str = str("UKNOWN Checksum")
        
        if "create_time" in file.keys():
            self.create_time: str = file["create_time"]  
        else:
            self.create_time: str = str("UKNOWN Create Time")

        if "size" in file.keys():
            self.size: int = file["size"]
        else:
            self.size: str = str("UKNOWN size")

        if "name" in file.keys():
            self.name: int = file["name"]
        else:
            self.name: str = str("UKNOWN name")

        self.type: str = "file",
        
    @classmethod
    def is_dir(cls) -> bool:
        """
            As the class is NOT a directory, it returns False.
        """
        return False