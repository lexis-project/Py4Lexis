
from __future__ import annotations
from typing import Literal
from requests import Response, get
from py4lexis.exceptions import Py4LexisException
from py4lexis.helper import Clr, sfouiro, _itbbra
from requests import get
from py4lexis.session import LexisSession
from irods.session import iRODSSession
from irods.data_object import iRODSDataObject
from irods.collection import iRODSCollection
from py4lexis.utils import assemble_dataset_path, check_if_uuid


class iRODS():

    def __init__(self, session: LexisSession) -> iRODSSession:
        """
            A class holds methods to manage objects and collections within iRODSSession.

            Attributes
            ----------
            session : class
                Class that holds LEXIS session.

            Methods
            -------
            put_object_to_dataset_collection(self,
                                             local_filepath: str,
                                             irods_filepath: str,
                                             access: Literal["public", "project", "user"],
                                             project: str,
                                             internal_id: str,
                                             zone: str="") -> iRODSDataObject | None:
                Put an object from local to dataset's iRODS path.

            get_object_from_dataset_collection(self,
                                               local_filepath: str,
                                               irods_filepath: str,
                                               access: Literal["public", "project", "user"],
                                               project: str,
                                               internal_id: str,
                                               zone: str="",) -> iRODSDataObject | None:
                Get an object from dataset's iRODS path to local.

            create_collection_within_dataset(self,
                                             collection_name: str,
                                             access: Literal["public", "project", "user"],
                                             project: str,
                                             internal_id: str,
                                             zone: str="",
                                             recurse: bool=True) -> iRODSCollection | None:
                Create a collection within dataset's iRODS path.

            get_project_collection(self,
                                   access: Literal["public", "project", "user"],
                                   project: str,
                                   zone: str="") -> iRODSCollection | None:
                Get LEXIS project as a collection.

            get_dataset_collection(self,
                                   access: Literal["public", "project", "user"],
                                   project: str,
                                   internal_id: str,
                                   zone: str="") -> iRODSCollection | None:
                Get a dataset as a collection.
        """

        self._session: LexisSession = session
        self._irods: iRODSSession = self._get_irods_session()
        self._Clr: Clr = Clr()
    

    def _validate_irods(self) -> bool:
        """
            Validate token at iRODS broker.
        """
        try:
            status_solved: bool = False
            content: list[dict] | None = None
            is_error: bool = True
            while not status_solved:          
                response: Response = get(self.Clr.yhbrr(sfouiro),
                                        params={
                                            "provider": self._Clr.yhbrr(_itbbra),
                                            "access_token": self.TOKEN
                                        }, verify=False)
                
                content, status_solved, is_error = self._session.handle_request_status(response, 
                                                                                       f"GET -- {self._Clr.yhbrr(sfouiro)}", 
                                                                                       to_json=False,
                                                                                       suppress_print=True if self.supress_print else False)
                
                if is_error:
                    message: str = f"Some errors occurred while validating token on iRODS. See log file, please."
                    if not self._sessionsupress_print:
                        print(message)
                    
                    if self._session.exception_on_error:
                        raise Py4LexisException(message)
                    else:
                        return is_error
                else:
                    if not self._session.supress_print:
                        print(f"Validate token on iRODS was successfull...")
                    else:
                        self._session.logging.debug("GET -- VALIDATE IRODS -- OK")

                    return is_error
            
        except Exception as ke:
            message: str = f"GET -- VALIDATE IRODS -- Error when connecting to Keycloak: {ke}"
            self._session.logging.error(message)
            if self._session.exception_on_error:
                raise Py4LexisException(message)


    def _get_irods_session(self) -> iRODSSession:
        """
            Retrieve iRODS session.
        """

        is_error = self._validate_irods()
        if not is_error and is_error is not None:
            irods_session = self._session.uc.get_irods_session(self._session.USERNAME, 
                                                               self._session.TOKEN, 
                                                               self._session.zone)

            if not self.supress_print:
                print(f"The iRODS session was successfully initialised.")
            
            self._session.logging.debug("iRODS -- INITIALISED -- OK")

            return irods_session
        else:
            if not self._session.supress_print:
                print(f"Some problems occurred while initialising iRODS session. Please, see log file.")
            
            self._session.logging.error("iRODS -- INITIALISED -- FAILED")


    def put_object_to_dataset_collection(self,
                                         local_filepath: str,
                                         irods_filepath: str,
                                         access: Literal["public", "project", "user"],
                                         project: str,
                                         internal_id: str,
                                         zone: str="") -> iRODSDataObject | None:
        """
            Put an object from local to dataset's iRODS path.

            Parameters
            ----------
            local_filepath: str
                Path of a file in local machine.
            irods_filepath: str
                Relative path within dataset's iRODS path where the file will be put.
            access: Literal["public", "project", "user"]
                Access of the LEXIS project.
            project: str
                LEXIS project's shortname.
            internal_id: str
                Dataset's internal ID (UUID).
            zone: str, optional
                iRODS zonename. By default: 'IT4ILexisZone'.

            Returns
            -------
            iRODSDataObject | None
        """

        if not check_if_uuid(internal_id):
            return None
        
        if access == "user":
            irods_path: str = assemble_dataset_path(access,
                                                    project,
                                                    internal_id,
                                                    self._session.USERNAME)
        else:
            irods_path: str = assemble_dataset_path(access,
                                                    project,
                                                    internal_id)
        
        if zone == "":
            zone = self._session.zone

        irods_path = f"/{zone}/{irods_path}/{irods_filepath}"

        return self._irods.data_objects.put(local_filepath, 
                                            irods_path)


    def get_object_from_dataset_collection(self,
                                           local_filepath: str,
                                           irods_filepath: str,
                                           access: Literal["public", "project", "user"],
                                           project: str,
                                           internal_id: str,
                                           zone: str="",) -> iRODSDataObject | None:
        """
            Get an object from dataset's iRODS path to local.

            Parameters
            ----------
            local_filepath: str
                Path of a file in local machine.
            irods_filepath: str
                Relative path within dataset's iRODS path where the file can be found.
            access: Literal["public", "project", "user"]
                Access of the LEXIS project.
            project: str
                LEXIS project's shortname.
            internal_id: str
                Dataset's internal ID (UUID).
            zone: str, optional
                iRODS zonename. By default: 'IT4ILexisZone'.

            Returns
            -------
            iRODSDataObject | None
        """
        
        if not check_if_uuid(internal_id):
            return None
        
        if access == "user":
            irods_path: str = assemble_dataset_path(access,
                                                    project,
                                                    internal_id,
                                                    self._session.USERNAME)
        else:
            irods_path: str = assemble_dataset_path(access,
                                                    project,
                                                    internal_id)
        
        if zone == "":
            zone = self._session.zone

        irods_path = f"/{zone}/{irods_path}/{irods_filepath}"

        return self._irods.data_objects.get(irods_path, 
                                            local_filepath)


    def create_collection_within_dataset(self,
                                         collection_name: str,
                                         access: Literal["public", "project", "user"],
                                         project: str,
                                         internal_id: str,
                                         zone: str="",
                                         recurse: bool=True) -> iRODSCollection | None:
        """
            Create a collection within dataset's iRODS path.

            Parameters
            ----------
            collection_name: str
                Name of new collection.
            access: Literal["public", "project", "user"]
                Access of the LEXIS project.
            project: str
                LEXIS project's shortname.
            internal_id: str
                Dataset's internal ID (UUID).
            zone: str, optional
                iRODS zonename. By default: 'IT4ILexisZone'.
            recurse: bool, optional
                Use recurse option or not. By default: True.

            Returns
            -------
            iRODSCollection | None
        """
        
        if not check_if_uuid(internal_id):
            return None
        
        if access == "user":
            irods_path: str = assemble_dataset_path(access,
                                                    project,
                                                    internal_id,
                                                    self._session.USERNAME)
        else:
            irods_path: str = assemble_dataset_path(access,
                                                    project,
                                                    internal_id)
        
        if zone == "":
            zone = self._session.zone

        irods_path = f"/{zone}/{irods_path}/{collection_name}"

        return self._irods.collections.create(irods_path,
                                              recurse)
    

    def get_project_collection(self,
                               access: Literal["public", "project", "user"],
                               project: str,
                               zone: str="") -> iRODSCollection | None:
        """
            Get LEXIS project as a collection.

            Parameters
            ----------
            access: Literal["public", "project", "user"]
                Access of the LEXIS project.
            project: str
                LEXIS project's shortname.
            zone: str, optional
                iRODS zonename. By default: 'IT4ILexisZone'.

            Returns
            -------
            iRODSCollection | None
        """
        
        if access == "user":
            irods_path: str = assemble_dataset_path(access,
                                                    project,
                                                    username=self._session.USERNAME)
        else:
            irods_path: str = assemble_dataset_path(access,
                                                    project)
        
        if zone == "":
            zone = self._session.zone

        irods_path = f"/{zone}/{irods_path}"

        return self._irods.collections.get(irods_path)


    def get_dataset_collection(self,
                               access: Literal["public", "project", "user"],
                               project: str,
                               internal_id: str,
                               zone: str="") -> iRODSCollection | None:
        """
            Get a dataset as a collection.

            Parameters
            ----------
            access: Literal["public", "project", "user"]
                Access of the LEXIS project.
            project: str
                LEXIS project's shortname.
            internal_id: str
                Dataset's internal ID (UUID).
            zone: str, optional
                iRODS zonename. By default: 'IT4ILexisZone'.

            Returns
            -------
            iRODSCollection | None
        """                       
        
        if not check_if_uuid(internal_id):
            return None
        
        if access == "user":
            irods_path: str = assemble_dataset_path(access,
                                                    project,
                                                    internal_id=internal_id,
                                                    username=self._session.USERNAME)
        else:
            irods_path: str = assemble_dataset_path(access,
                                                    project,
                                                    internal_id=internal_id)
        
        if zone == "":
            zone = self._session.zone

        irods_path = f"/{zone}/{irods_path}"

        return self._irods.collections.get(irods_path)