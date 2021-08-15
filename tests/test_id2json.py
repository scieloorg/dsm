from unittest import TestCase
from dsm.extdeps.isis_migration import id2json


class TestIsisIdToJson(TestCase):

    def test_build_record(self):
        expected = {
            "10": [
                {"s": "surname", "n": "name"},
                {"s": "surname", "n": "name", "o": "xxx"},
            ],
            "12": [
                {"l": "en", "_": "title"},
                {"l": "es", "_": "título"},
            ],
        }
        data = [
            ["10", {"s": "surname", "n": "name"}],
            ["10", {"s": "surname", "n": "name", "o": "xxx"}],
            ["12", {"l": "en", "_": "title"}],
            ["12", {"l": "es", "_": "título"}],
        ]
        result = id2json._build_record(data)
        self.assertEqual(expected, result)

    def test_parse_field_content(self):
        expected = {"s": "surname", "n": "name", "o": "xxx", "_": "bla"}
        data = "bla^ssurname^nname^oxxx"
        result = id2json._parse_field_content(data)
        self.assertDictEqual(expected, result)

    def test_parse_field(self):
        expected = (
            "v9999", {"_": "bla", "s": "surname", "n": "name", "o": "xxx"})
        data = "!v9999!bla^ssurname^nname^oxxx"
        result = id2json._parse_field(data)
        self.assertEqual(expected[0], result[0])
        self.assertDictEqual(expected[1], result[1])
