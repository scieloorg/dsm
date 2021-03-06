from dsm.extdeps import db



class IssuesManager:

    def __init__(self, db_url):
        """
        Instancia objeto da classe IssuesManager

        Parameters
        ----------
        db_url : str
            Data to connect to a mongodb. Expected pattern:
                "mongodb://my_user:my_password@127.0.0.1:27017/my_db"
        """
        self._db_url = db_url

    def db_connect(self):
        db.mk_connection(self._db_url)

    def get_issue(self, _id):
        """
        Get issue data

        Parameters
        ----------
        _id : str
            Issue ID

        Returns
        -------
        Issue
            issue

        """
        return db.fetch_issue(_id)

    def get_issues_by_issn(self, issn, only_aop=False):
        """
        Get issues

        Parameters
        ----------
        issn : str
            ISSN
        only_aop : boolean
            Fetch issues containing only documents at the 'ahead of print' stage

        Returns
        -------
        QuerySet
            issues

        """
        if only_aop:
            return db.fetch_issues(journal=issn, type='ahead')
        return db.fetch_issues(journal=issn)

def get_bundle_id(issn_id, year, volume=None, number=None, supplement=None):
    """
        Gera Id utilizado na ferramenta de migração
        para cadastro do documentsbundle.
    """
    items = (
        ("v", volume),
        ("n", number),
        ("s", supplement),
    )
    label = "-".join([f"{prefix}{value}"
                      for prefix, value in items
                      if value])
    if label:
        return f"{issn_id}-{year}-{label}"
    return f"{issn_id}-aop"
