from dsm.extdeps import db


class JournalsManager:

    def __init__(self, db_url):
        """
        Instancia objeto da classe JournalsManager

        Parameters
        ----------
        db_url : str
            Data to connect to a mongodb. Expected pattern:
                "mongodb://my_user:my_password@127.0.0.1:27017/my_db"
        """
        self._db_url = db_url

    def db_connect(self):
        db.mk_connection(self._db_url)

    def get_journal(self, _id):
        """
        Get journal data

        Parameters
        ----------
        _id : str
            Journal ID

        Returns
        -------
        Journal
            journal

        """
        return db.fetch_journal(_id)

    def get_journals(self, **kwargs):
        """
        Get journals

        Returns
        -------
        QuerySet
            journals

        """
        return db.fetch_journals(**kwargs)
