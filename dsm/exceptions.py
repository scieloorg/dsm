class MissingPidV3Error(Exception):
    ...


class DocumentDoesNotExistError(Exception):
    ...


class FetchDocumentError(Exception):
    ...


class DBFetchDocumentPackageError(Exception):
    ...


class DBCreateDocumentError(Exception):
    ...


class DBSaveDataError(Exception):
    ...


class DBConnectError(Exception):
    ...


class RemoteAndLocalFileError(Exception):
    ...


class ReceivedPackageRegistrationError(Exception):
    ...


class FilesStorageRegisterError(Exception):
    ...


class DBFetchMigratedDocError(Exception):
    ...
