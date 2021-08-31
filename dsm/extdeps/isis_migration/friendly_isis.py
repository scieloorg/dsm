"""
Not defined;coordinator;inventor;publisher;organizer;translator
nd;coord;inventor;ed;org;tr
"""
import os

CONTRIB_ROLES = {
    "ND": "author",
    "nd": "author",
    "coord": "coordinator",
    "inventor": "inventor",
    "tr": "translator",
    "ed": "editor",
    "org": "organizer",
}


def fix_windows_path(windows_path):
    """
    It can not handle `o`, `x`, `N`, `u`, `U`
    For the combination of slash and this letters,
    the correction have to be done manually
    """
    path = os.path.normpath(windows_path).replace("\\", "/")
    special = ("\x08", "\a", "\b", "\f", "\n", "\r", "\t", "\v", )
    correction = ("/b", "/a", "/b", "/f", "/n", "/r", "/t", "/v", )
    for ch, correct in zip(special, correction):
        path = path.replace(ch, correct)
    return path


def _get_value(data, tag):
    """
    Returns first value of field `tag`
    """
    # data['v880'][0]['_']
    try:
        _data = data[tag][0]
        if len(_data) > 1:
            return _data
        else:
            return _data['_']
    except (KeyError, IndexError):
        return None


def _get_items(data, tag):
    """
    Returns first value of field `tag`
    """
    # data['v880'][0]['_']
    try:
        for item in data[tag]:
            if len(item) > 1:
                yield item
            else:
                yield item['_']
    except KeyError:
        return []


class FriendlyISISDocument:
    """
    Interface amigável para obter os dados da base isis
    que estão no formato JSON
    """
    def __init__(self, _id, records):
        self._id = _id
        self._records = records
        self._pages = self._get_article_meta_item_("v014")
        if self._pages is None:
            raise ValueError("missing v014: %s" % str(records[1]))
        self._set_file_name()
        self._paragraphs = FriendlyISISParagraphs(self._records)

    def _get_article_meta_item_(self, tag, formatted=False):
        if formatted:
            # record = f
            return _get_value(self._records[2], tag)
        return _get_value(self._records[1], tag)

    def _get_article_meta_items_(self, tag, formatted=False):
        if formatted:
            # record = f
            return list(_get_items(self._records[2], tag))
        return list(_get_items(self._records[1], tag))

    @property
    def records(self):
        return self._records

    @property
    def data(self):
        _data = {}
        _data["article"] = self._records
        return _data

    @property
    def raw_file_path(self):
        return self._get_article_meta_item_("v702")

    def _set_file_name(self):
        file_path = self.raw_file_path
        file_path = fix_windows_path(file_path)
        self._basename = os.path.basename(file_path)
        self._filename, self._ext = os.path.splitext(self._basename)

    @property
    def file_name(self):
        return self._filename

    @property
    def file_type(self):
        return "xml" if self._ext == ".xml" else "html"

    @property
    def volume(self):
        return self._get_article_meta_item_("v031")

    @property
    def number(self):
        return self._get_article_meta_item_("v032")

    @property
    def suppl(self):
        return (
            self._get_article_meta_item_("v131") or
            self._get_article_meta_item_("v132")
        )

    @property
    def issue_folder(self):
        if self.number == "ahead":
            return self.year + "nahead"

        pairs = (
            ("v", remove_leading_zeros(self.volume)),
            ("n", remove_leading_zeros(self.number)),
            ("s", remove_leading_zeros(self.suppl)),
        )
        return "".join([prefix + value for prefix, value in pairs if value])

    @property
    def document_pubdate(self):
        return self._get_article_meta_item_("v223")

    @property
    def collection_pubdate(self):
        return self._get_article_meta_item_("v065")

    @property
    def doi(self):
        return self._get_article_meta_item_("v237")

    @property
    def language(self):
        return self._get_article_meta_item_("v040")

    @property
    def document_type(self):
        return self._get_article_meta_item_("v071")

    @property
    def isis_dates(self):
        dates = (
            _get_value(self._records[0], "v091"),
            _get_value(self._records[0], "v093")
        )
        return [d[:8] for d in dates if d]

    @property
    def isis_updated_date(self):
        return max(self.isis_dates)

    @property
    def isis_created_date(self):
        return min(self.isis_dates)

    @property
    def html_body_items(self):
        _html_body_items = self.html_body
        _html_body_items.update(self.translated_html_body_items)
        return _html_body_items

    @property
    def html_body(self):
        _paragraphs = []
        for rec in self._records:
            p = _get_value(rec, "v704")
            if p:
                _paragraphs.append(p)
        return {self.language: "".join(_paragraphs)}

    @property
    def translated_html_body_items(self):
        # TODO
        return {}

    @property
    def _mixed_citations(self):
        # TODO
        mixed_citations = {}
        for rec in self._records:
            p = _get_value(rec, "v704")
            ref_number = _get_value(rec, "v118")
            if p and ref_number:
                mixed_citations.update(
                    {ref_number: p}
                )
        return mixed_citations

    @property
    def pdfs(self):
        # TODO
        pass

    @property
    def images(self):
        # TODO
        pass

    @property
    def translated_body_items(self):
        # TODO
        pass

    @property
    def material_supplementar(self):
        # TODO
        pass

    @property
    def journal_pid(self):
        return self._get_article_meta_item_("v035")

    @property
    def issue_pid(self):
        return self.scielo_pid_v2[1:18]

    @property
    def journal(self):
        return self._journal

    @journal.setter
    def journal(self, value):
        self._journal = value

    @property
    def issue(self):
        return self._issue

    @issue.setter
    def issue(self, value):
        self._issue = value

    @property
    def original_section(self):
        code = self._get_article_meta_item_("v049")
        return self.issue.get_section(code, self.language)

    @property
    def translated_sections(self):
        return self.issue.get_section(self.section_code)

    @property
    def section_code(self):
        return self._get_article_meta_item_("v049")

    @property
    def article_type(self):
        return self._get_article_meta_item_("v071")

    @property
    def scielo_pid_v1(self):
        return self._get_article_meta_item_("v002")

    @property
    def scielo_pid_v2(self):
        return self._get_article_meta_item_("v880")

    @property
    def scielo_pid_v3(self):
        return self._get_article_meta_item_("v885")

    @property
    def ahead_of_print_pid(self):
        return self._get_article_meta_item_("v881")

    @property
    def doi_with_lang(self):
        return {
            item.get("l"): item.get("d")
            for item in self._get_article_meta_items_("v337")
        }

    @property
    def doi(self):
        return self.doi_with_lang.get(self.language)

    @property
    def order(self):
        return self._get_article_meta_item_("v121").zfill(5)

    @property
    def languages(self):
        return [self.language] + self.translated_languages

    @property
    def translated_languages(self):
        return (
            self._get_article_meta_items_("v601") or
            list((self.translated_htmls() or {}).keys())
        )

    @property
    def original_title(self):
        return self.titles.get(self.language)

    @property
    def titles(self):
        # TODO manter formatação itálico e maths
        return {
            item['l']: item['_']
            for item in self._get_article_meta_items_("v012", True)
        }

    @property
    def translated_titles(self):
        return {
            lang: title
            for lang, title in self.titles.items()
            if lang != self.language
        }

    @property
    def titles(self):
        # TODO manter formatação itálico e maths
        return {
            item['l']: item['_']
            for item in self._get_article_meta_items_("v012")
        }

    def translated_htmls(self, iso_format=None):
        if not self.body:
            return None

        fmt = iso_format or self._iso_format

        translated_bodies = {}
        for language, body in self.data.get('body', {}).items():
            if language != self.original_language(iso_format=fmt):
                translated_bodies[language] = body

        if len(translated_bodies) == 0:
            return None

    @property
    def contrib_group(self):
        for item in self._get_article_meta_items_("v010"):
            aff = None
            xref_items = contrib_xref(item.get("1")) or []
            for xref_type, xref in xref_items:
                if xref_type == "aff":
                    aff = self.affiliations.get(xref)
            yield (
                {
                    "surname": item.get("s"),
                    "given_names": item.get("n"),
                    "role": CONTRIB_ROLES.get(item.get("r")),
                    "xref": xref_items,
                    "orcid": item.get("k"),
                    "affiliation": aff,
                }
            )

    @property
    def affiliations(self):
        """

        """
        affs = {}
        for item in self._get_article_meta_items_("v070"):
            aff = {
                "label": item.get('l'),
                "id": item.get('i'),
                "email": item.get('e'),
                "orgdiv3": item.get('3'),
                "orgdiv2": item.get('2'),
                "orgdiv1": item.get('1'),
                "country": item.get('p'),
                "city": item.get('c'),
                "state": item.get('s'),
                "orgname": item.get('_'),
            }
            affs[item.get('i')] = aff
        return affs

    @property
    def norm_affiliations(self):
        affs = {}
        for item in self._get_article_meta_items_("v240"):
            aff = {
                "id": item.get('i'),
                "country": item.get('p'),
                "city": item.get('c'),
                "state": item.get('s'),
                "orgname": item.get('_'),
            }
            affs[item.get('i')] = aff
        return affs

    @property
    def keywords_groups(self):
        kwdg = {}
        for item in self._get_article_meta_items_("v085", formatted=True):
            if not item.get("k"):
                continue
            lang = item.get("l")
            kwdg.setdefault(lang, [])
            kwd = f'{item.get("k")} {item.get("s") or ""}'.strip()
            kwdg[lang].append(kwd)
        return kwdg

    @property
    def elocation_id(self):
        return self._pages.get("e")

    @property
    def fpage(self):
        return self._pages.get("f")

    @property
    def fpage_seq(self):
        return self._pages.get("s")

    @property
    def lpage(self):
        return self._pages.get("l")

    @property
    def body(self):
        # TODO
        return None

    @property
    def abstract(self):
        return self.abstracts.get(self.language)

    @property
    def abstracts(self):
        _abstracts = {}
        for abstract in self._get_article_meta_items_("v083", formatted=True):
            _abstracts[abstract.get("l")] = abstract.get("a")
        return _abstracts

    @property
    def p_records(self):
        return self._paragraphs.paragraphs

    @p_records.setter
    def p_records(self, _p_records):
        self._paragraphs.replace_paragraphs(_p_records)


def contrib_xref(xrefs):
    for xref in (xrefs or "").split():
        if xref.startswith("aff") or xref.startswith("a0"):
            xref_type = "aff"
        elif xref.startswith("fn"):
            xref_type = "fn"
        else:
            xref_type = "author-notes"
        yield (xref_type, xref)


def complete_uri(text, website_url):
    return text.replace("/img/revistas", f"{website_url}/img/revistas")


class FriendlyISISParagraphs:
    """
    Interface amigável para obter os dados da base isis
    que estão no formato JSON
    """
    def __init__(self, _id, doc_records):
        self._id = _id
        self._doc_records = doc_records
        self._select_paragraph_records()

    def _del_paragraphs(self):
        for rec in self._before_refs:
            self._doc_records.remove(rec)
        for rec in self._refs:
            self._doc_records.remove(rec)
        for rec in self._after_refs:
            self._doc_records.remove(rec)

    def replace_paragraphs(self, p_records):
        self._del_paragraphs()
        self._doc_records.extend(p_records)
        self._select_paragraph_records()

    @property
    def references(self):
        return "".join([
            _get_value(record, "v704").get("_")
            for record in self._refs
        ])

    def _select_paragraph_records(self):
        self._before_refs = []
        self._refs = []
        self._after_refs = []
        _list = self._before_refs
        for rec in self._doc_records:
            rec_type = _get_value(rec, "v706")
            if rec_type != "p":
                continue
            ref_id = _get_value(rec, "v888")
            if ref_id:
                _list = self._refs
            elif self._refs:
                _list = self._after_refs
            else:
                _list = self._before_refs
            _list.append(rec)

    @property
    def paragraphs(self):
        return (
            self._before_refs or [] +
            self._refs or [] +
            self._after_refs or []
        )

    @property
    def text(self):
        return "".join(
            [
                _get_value(record, "v704").get("_")
                for record in self.paragraphs
            ]
        )


class FriendlyISISJournal:
    """
    Interface amigável para obter os dados da base isis
    que estão no formato JSON
    """
    def __init__(self, _id, record):
        self._id = _id
        self._record = record
        self._issns = get_issns(self._record) or {}

    def _get_items_(self, tag):
        return list(_get_items(self._record, tag))

    def _get_item_(self, tag):
        return _get_value(self._record, tag)

    @property
    def record(self):
        return self._record

    @property
    def acronym(self):
        return self._get_item_("v068").lower()

    @property
    def title(self):
        return self._get_item_("v100")

    @property
    def iso_abbreviated_title(self):
        return self._get_item_("v151")

    @property
    def abbreviated_title(self):
        return self._get_item_("v150")

    @property
    def print_issn(self):
        return self._issns.get("PRINT")

    @property
    def electronic_issn(self):
        return self._issns.get("ONLIN")

    @property
    def raw_publisher_names(self):
        return self._get_items_('v480')

    def get_publisher_names(self, sep="; "):
        return sep.join(self.raw_publisher_names)

    @property
    def publisher_city(self):
        return self._get_item_('v490')

    @property
    def publisher_state(self):
        return self._get_item_('v320')

    def get_publisher_loc(self, sep=", "):
        loc = [item
               for item in [self.publisher_city, self.publisher_state]
               if item]
        return sep.join(loc)

    @property
    def new_title(self):
        return self._get_item_("v710")

    @property
    def old_title(self):
        return self._get_item_("v610")

    @property
    def isis_created_date(self):
        return self._get_item_("v940")

    @property
    def isis_updated_date(self):
        return self._get_item_("v941")

    @property
    def subject_descriptors(self):
        return self._get_items_("v440")

    @property
    def subject_categories(self):
        return self._get_items_("v854")

    @property
    def study_areas(self):
        return self._get_items_("v441")

    @property
    def copyright_holder(self):
        return self._get_item_("v062")

    @property
    def online_submission_url(self):
        return self._get_item_("v692")

    @property
    def other_titles(self):
        return self._get_item_("v240")

    @property
    def publisher_country(self):
        return self._get_item_("v310")

    @property
    def publisher_address(self):
        return self._get_item_("v063")

    @property
    def publication_status(self):
        return self._get_item_("v050")

    @property
    def email(self):
        return self._get_item_("v064")

    @property
    def mission(self):
        return {
            m['l']: m['_']
            for m in self._get_items_("v901")
        }

    @property
    def index_at(self):
        # ListField(field=StringField())
        return self._get_items_("v450")

    @property
    def sponsors(self):
        return self._get_items_("v140")

    @property
    def status_history(self):
        """
        subfield a: initial date, ISO format
        subfield b: status which value is C
        subfield c: final date, ISO format
        subfield d: status which value is D or S
        """
        for item in self._get_items_("v051"):
            if item.get("a"):
                yield {
                    "date": item.get("a"),
                    "status": item.get("b"),
                }
            if item.get("c"):
                yield {
                    "date": item.get("c"),
                    "status": item.get("d"),
                    "reason": item.get("e"),
                }

    @property
    def current_status(self):
        _hist = sorted([
            (item.get("date"), item.get("status"))
            for item in self.status_history
        ])
        return _hist[-1][1]

    @property
    def unpublish_reason(self):
        _hist = sorted([
            (item.get("date"), item.get("status"))
            for item in self.status_history
        ])
        return _hist[-1][1] if _hist[-1][1] != 'C' else None


class FriendlyISISIssue:
    """
    Interface amigável para obter os dados da base isis
    que estão no formato JSON
    """
    def __init__(self, _id, record):
        self._id = _id
        self._record = record
        self._sections = None

    @property
    def record(self):
        return self._record

    def _get_items_(self, tag):
        return list(_get_items(self._record, tag))

    def _get_item_(self, tag):
        return _get_value(self._record, tag)

    @property
    def sections(self):
        if self._sections is None:
            self._sections = {}
            for item in _get_items(self._record, "v049"):
                self._sections.setdefault(item["c"], {})
                self._sections[item["c"]][item["l"]] = item["t"]
        return self._sections

    def get_section(self, code, lang=None):
        try:
            if lang:
                return self.sections[code][lang]
            return self.sections[code]
        except KeyError:
            return None

    @property
    def isis_created_date(self):
        return self._get_item_("NotAvailable")

    @property
    def isis_updated_date(self):
        return self._get_item_("NotAvailable")

    @property
    def iid(self):
        return self._id

    @property
    def journal_pid(self):
        return self._get_item_("v035")

    @property
    def journal(self):
        return self._journal

    @journal.setter
    def journal(self, value):
        self._journal = value

    @property
    def volume(self):
        return self._get_item_("v031")

    @property
    def number(self):
        return self._get_item_("v032")

    @property
    def suppl(self):
        return self._get_item_("v131") or self._get_item_("v132")

    @property
    def start_month(self):
        return self._get_item_("v065")[4:6]

    @property
    def end_month(self):
        return self._get_item_("v065")[4:6]

    @property
    def year(self):
        return self._get_item_("v065")[:4]

    @property
    def issue_folder(self):
        if self.number == "ahead":
            return self.year + "nahead"

        pairs = (
            ("v", remove_leading_zeros(self.volume)),
            ("n", remove_leading_zeros(self.number)),
            ("s", remove_leading_zeros(self.suppl)),
        )
        return "".join([prefix + value for prefix, value in pairs if value])

    @property
    def order(self):
        _order = self._get_item_("v036")
        return _order[:4] + _order[4:].zfill(4)

    @property
    def is_public(self):
        return self._get_item_("v042") == 1

    @property
    def pid(self):
        return self._id or f"{self.journal_pid}{self.order}"

    @property
    def unpublish_reason(self):
        return self._get_item_("NotAvailable")

    @property
    def url_segment(self):
        return self._get_item_("NotAvailable")

    @property
    def assets_code(self):
        return self._get_item_("NotAvailable")

    @property
    def type(self):
        return self._get_item_("NotAvailable")

    @property
    def suppl_text(self):
        return self._get_item_("NotAvailable")

    @property
    def spe_text(self):
        return self._get_item_("NotAvailable")

    @property
    def cover_url(self):
        return self._get_item_("NotAvailable")


def remove_leading_zeros(data):
    try:
        return str(int(data))
    except:
        return data


def get_issns(data):
    _issns = {}
    for item in data.get("v435") or []:
        _issns[item.get("t")] = item['_']
    if _issns:
        return _issns

    issn_type = _get_value(data, "v035")
    if not issn_type:
        return None

    if 'v935' in data:
        _issns[issn_type] = _get_value(data, "v935")
        v400 = _get_value(data, "v400")
        if _issns[issn_type] != v400:
            _issns["PRINT" if issn_type != "PRINT" else "ONLIN"] = v400
        return _issns

    # ISSN and Other Complex Stuffs from the old version
    issn_type = _get_value(data, "v035")
    _issns[issn_type] = _get_value(data, "v400")
    return _issns
