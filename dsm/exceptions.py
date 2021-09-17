class MissingPidV3Error(Exception):
    ...


class DocumentDoesNotExistError(Exception):
    ...


class FetchDocumentError(Exception):
    ...


class DBFetchDocumentError(Exception):
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


class MissingCisisPathEnvVarError(Exception):
    ...


class CisisPathNotFoundMigrationError(Exception):
    ...


class MissingI2IdCommandPathEnvVarError(Exception):
    ...


class IsisDBNotFoundError(Exception):
    ...


class IdFileNotFoundError(Exception):
    ...


class NotApplicableInfo(Exception):
    ...


