"""Unit tests for openapi_validate (inline schemas only)."""

import unittest

from . import __main__ as checker
from .openapi_validate import resolve_ref, validate, validate_with_warnings

SPEC = {
    "components": {
        "schemas": {
            "Name": {"type": "string", "minLength": 2},
            "User": {
                "type": "object",
                "required": ["name"],
                "properties": {"name": {"$ref": "#/components/schemas/Name"}},
            },
        }
    }
}


class ValidateTest(unittest.TestCase):
    def assert_ok(self, schema, data, spec=None):
        self.assertEqual(validate(schema, data, spec or {}), [])

    def assert_bad(self, schema, data, spec=None):
        self.assertTrue(validate(schema, data, spec or {}))

    # $ref
    def test_ref_pass(self):
        self.assert_ok({"$ref": "#/components/schemas/User"},
                       {"name": "ann"}, SPEC)

    def test_ref_fail(self):
        self.assert_bad({"$ref": "#/components/schemas/User"},
                        {"name": "x"}, SPEC)

    # type
    def test_type_object_pass(self):
        self.assert_ok({"type": "object"}, {})

    def test_type_object_fail(self):
        self.assert_bad({"type": "object"}, [])

    def test_type_array_pass(self):
        self.assert_ok({"type": "array"}, [1])

    def test_type_array_fail(self):
        self.assert_bad({"type": "array"}, {})

    def test_type_string_pass(self):
        self.assert_ok({"type": "string"}, "a")

    def test_type_string_fail(self):
        self.assert_bad({"type": "string"}, 3)

    def test_type_integer_pass(self):
        self.assert_ok({"type": "integer"}, 3)

    def test_type_integer_fail_bool(self):
        self.assert_bad({"type": "integer"}, True)

    def test_type_integer_fail(self):
        self.assert_bad({"type": "integer"}, 3.5)

    def test_type_number_pass(self):
        self.assert_ok({"type": "number"}, 3.5)

    def test_type_number_fail(self):
        self.assert_bad({"type": "number"}, "x")

    def test_type_boolean_pass(self):
        self.assert_ok({"type": "boolean"}, False)

    def test_type_boolean_fail(self):
        self.assert_bad({"type": "boolean"}, 0)

    # nullable / null
    def test_nullable_pass(self):
        self.assert_ok({"type": "string", "nullable": True}, None)

    def test_nullable_fail(self):
        self.assert_bad({"type": "string"}, None)

    def test_null_untyped_pass(self):
        self.assert_ok({}, None)

    def test_null_description_only_pass(self):
        self.assert_ok({"description": "anything"}, None)

    def test_null_typed_fail(self):
        self.assert_bad({"type": "string"}, None)

    def test_null_nullable_typed_pass(self):
        self.assert_ok({"type": "string", "nullable": True}, None)

    def test_null_enum_with_null_pass(self):
        self.assert_ok({"enum": ["a", None]}, None)

    def test_null_enum_without_null_fail(self):
        self.assert_bad({"enum": ["a", "b"]}, None)

    # properties
    def test_properties_pass(self):
        self.assert_ok({"properties": {"a": {"type": "string"}}}, {"a": "x"})

    def test_properties_fail(self):
        errors = validate({"properties": {"a": {"type": "string"}}}, {"a": 1})
        self.assertTrue(errors and errors[0].startswith("/a:"))

    # required
    def test_required_pass(self):
        self.assert_ok({"required": ["a"]}, {"a": 1})

    def test_required_fail(self):
        self.assert_bad({"required": ["a"]}, {})

    # additionalProperties
    def test_additional_false_pass(self):
        self.assert_ok({"properties": {"a": {}},
                        "additionalProperties": False}, {"a": 1})

    def test_additional_false_fail(self):
        self.assert_bad({"properties": {"a": {}},
                         "additionalProperties": False}, {"b": 1})

    def test_additional_schema_pass(self):
        self.assert_ok({"additionalProperties": {"type": "string"}}, {"b": "x"})

    def test_additional_schema_fail(self):
        self.assert_bad({"additionalProperties": {"type": "string"}}, {"b": 1})

    # items
    def test_items_pass(self):
        self.assert_ok({"items": {"type": "integer"}}, [1, 2])

    def test_items_fail_pointer(self):
        errors = validate({"items": {"type": "integer"}}, [1, "x"])
        self.assertEqual(len(errors), 1)
        self.assertTrue(errors[0].startswith("/1:"))

    # enum
    def test_enum_pass(self):
        self.assert_ok({"enum": ["a", "b"]}, "a")

    def test_enum_fail(self):
        self.assert_bad({"enum": ["a", "b"]}, "c")

    # oneOf
    def test_oneof_pass(self):
        self.assert_ok({"oneOf": [{"type": "string"}, {"type": "integer"}]}, 4)

    def test_oneof_fail_none(self):
        self.assert_bad({"oneOf": [{"type": "string"}, {"type": "integer"}]},
                        4.5)

    def test_oneof_fail_both_warns(self):
        errors, warnings = validate_with_warnings(
            {"oneOf": [{"type": "number"}, {"type": "integer"}]}, 4)
        self.assertEqual(errors, [])
        self.assertEqual(len(warnings), 1)

    def test_oneof_overlap_single_branch_no_warning(self):
        errors, warnings = validate_with_warnings(
            {"oneOf": [{"type": "string"}, {"type": "integer"}]}, 4)
        self.assertEqual(errors, [])
        self.assertEqual(warnings, [])

    # anyOf
    def test_anyof_pass(self):
        self.assert_ok({"anyOf": [{"type": "string"}, {"type": "integer"}]}, 4)

    def test_anyof_fail(self):
        self.assert_bad({"anyOf": [{"type": "string"}, {"type": "integer"}]},
                        4.5)

    # allOf
    def test_allof_pass(self):
        self.assert_ok({"allOf": [{"type": "integer"}, {"minimum": 2}]}, 4)

    def test_allof_fail(self):
        self.assert_bad({"allOf": [{"type": "integer"}, {"minimum": 5}]}, 4)

    # minimum / maximum
    def test_minimum_pass(self):
        self.assert_ok({"minimum": 2}, 2)

    def test_minimum_fail(self):
        self.assert_bad({"minimum": 2}, 1)

    def test_maximum_pass(self):
        self.assert_ok({"maximum": 5}, 5)

    def test_maximum_fail(self):
        self.assert_bad({"maximum": 5}, 6)

    # minLength / maxLength
    def test_minlength_pass(self):
        self.assert_ok({"minLength": 2}, "ab")

    def test_minlength_fail(self):
        self.assert_bad({"minLength": 2}, "a")

    def test_maxlength_pass(self):
        self.assert_ok({"maxLength": 2}, "ab")

    def test_maxlength_fail(self):
        self.assert_bad({"maxLength": 2}, "abc")

    # format date-time
    def test_datetime_pass(self):
        self.assert_ok({"format": "date-time"}, "2024-01-01T12:34:56.789+08:00")

    def test_datetime_fail(self):
        self.assert_bad({"format": "date-time"}, "not a date")

    def test_other_format_ignored(self):
        self.assert_ok({"format": "password"}, "anything")

    def test_resolve_ref_missing(self):
        with self.assertRaises(KeyError):
            resolve_ref(SPEC, "#/components/schemas/Nope")


class DecideCasesTest(unittest.TestCase):
    def test_get_with_path_params_needs_no_mutations(self):
        op = {"method": "GET", "path": "/clients/{clientid}",
              "tag": "Clients", "operation": {}, "params": []}
        cases = {"GET /clients/{clientid}":
                 [{"operation": "GET /clients/{clientid}",
                   "path_params": {"clientid": "c1"}}]}
        planned = checker.decide_cases(op, cases, False)
        self.assertEqual(len(planned), 1)
        action, case, index, _ = planned[0]
        self.assertEqual(action, "call")
        self.assertIsNotNone(case)
        self.assertEqual(index, 0)

    def test_delete_with_path_params_needs_allow_mutations(self):
        op = {"method": "DELETE", "path": "/clients/{clientid}",
              "tag": "Clients", "operation": {}, "params": []}
        cases = {"DELETE /clients/{clientid}":
                 [{"operation": "DELETE /clients/{clientid}",
                   "path_params": {"clientid": "c1"}}]}
        planned = checker.decide_cases(op, cases, False)
        self.assertEqual(len(planned), 1)
        action, _, _, reason = planned[0]
        self.assertEqual(action, "skip")
        self.assertEqual(reason, "skipped:needs-allow-mutations")

    def test_two_cases_select_both_in_order(self):
        op = {"method": "GET", "path": "/items",
              "tag": "Items", "operation": {}, "params": []}
        cases = {"GET /items":
                 [{"operation": "GET /items", "query": {"page": 1}},
                  {"operation": "GET /items", "query": {"page": 2}}]}
        planned = checker.decide_cases(op, cases, False)
        self.assertEqual(len(planned), 2)
        self.assertEqual([p[0] for p in planned], ["call", "call"])
        self.assertEqual([p[2] for p in planned], [0, 1])

    def test_two_cases_report_two_rows(self):
        op = {"method": "GET", "path": "/items",
              "tag": "Items",
              "operation": {"responses": {"200": {"description": "ok"}}}}
        cases = {"GET /items":
                 [{"operation": "GET /items", "query": {"page": 1}},
                  {"operation": "GET /items", "query": {"page": 2}}]}

        class StubRunner:
            base = "http://127.0.0.1:1/api/v5"
            headers = {}

            def _resolve_value(self, value):
                return value

            def run_setup(self, _case):
                pass

            def run_cleanup(self, _case):
                pass

        def stub_api_json(method, base, path, headers, query=None, body=None):
            return 200, [], b"[]"

        planned = checker.decide_cases(op, cases, False)
        rows = []
        for action, case, index, _ in planned:
            if action == "call":
                rows.append(checker.check_one(
                    {}, op, case, StubRunner(), False,
                    api_json_fn=stub_api_json, case_index=index))
        self.assertEqual(len(rows), 2)
        self.assertEqual([r["case_index"] for r in rows], [0, 1])

    def test_case_without_params_skips_only_that_case(self):
        op = {"method": "GET", "path": "/clients/{clientid}",
              "tag": "Clients", "operation": {}, "params": []}
        cases = {"GET /clients/{clientid}":
                 [{"operation": "GET /clients/{clientid}",
                   "path_params": {"clientid": "c1"}},
                  {"operation": "GET /clients/{clientid}"}]}
        planned = checker.decide_cases(op, cases, False)
        self.assertEqual(len(planned), 2)
        self.assertEqual(planned[0][0], "call")
        self.assertEqual(planned[1][0], "skip")
        self.assertEqual(planned[1][3], "skipped:needs-case")

    def test_mutation_skip_applies_per_case(self):
        op = {"method": "DELETE", "path": "/clients/{clientid}",
              "tag": "Clients", "operation": {}, "params": []}
        cases = {"DELETE /clients/{clientid}":
                 [{"operation": "DELETE /clients/{clientid}",
                   "path_params": {"clientid": "c1"}},
                  {"operation": "DELETE /clients/{clientid}",
                   "path_params": {"clientid": "c2"}}]}
        planned = checker.decide_cases(op, cases, False)
        self.assertEqual(len(planned), 2)
        self.assertTrue(all(p[0] == "skip" for p in planned))
        self.assertEqual([p[2] for p in planned], [0, 1])

    def test_unaided_get_has_no_case_index(self):
        op = {"method": "GET", "path": "/items",
              "tag": "Items", "operation": {}, "params": []}
        planned = checker.decide_cases(op, {}, False)
        self.assertEqual(len(planned), 1)
        self.assertEqual(planned[0][0], "call")
        self.assertIsNone(planned[0][1])
        self.assertIsNone(planned[0][2])


class CheckOneTest(unittest.TestCase):
    def test_row_carries_case_index(self):
        op = {"method": "GET", "path": "/items",
              "tag": "Items",
              "operation": {"responses": {"200": {"description": "ok"}}}}
        case = {"operation": "GET /items"}

        class StubRunner:
            base = "http://127.0.0.1:1/api/v5"
            headers = {}

            def _resolve_value(self, value):
                return value

            def run_setup(self, _case):
                pass

            def run_cleanup(self, _case):
                pass

        def stub_api_json(method, base, path, headers, query=None, body=None):
            return 200, [], b"[]"

        row = checker.check_one({}, op, case, StubRunner(), False,
                                api_json_fn=stub_api_json, case_index=3)
        self.assertEqual(row["case_index"], 3)
        self.assertEqual(row["method"], "GET")
        self.assertEqual(row["path"], "/items")

    def test_each_case_runs_setup_and_cleanup(self):
        op = {"method": "GET", "path": "/items",
              "tag": "Items",
              "operation": {"responses": {"200": {"description": "ok"}}}}
        first = {"operation": "GET /items", "query": {"page": 1}}
        second = {"operation": "GET /items", "query": {"page": 2}}
        seen = []

        class StubRunner:
            base = "http://127.0.0.1:1/api/v5"
            headers = {}

            def _resolve_value(self, value):
                return value

            def run_setup(self, case):
                seen.append(("setup", case["query"]["page"]))

            def run_cleanup(self, case):
                seen.append(("cleanup", case["query"]["page"]))

        def stub_api_json(method, base, path, headers, query=None, body=None):
            return 200, [], b"[]"

        for index, case in enumerate((first, second)):
            checker.check_one({}, op, case, StubRunner(), False,
                              api_json_fn=stub_api_json, case_index=index)
        self.assertEqual(seen, [("setup", 1), ("cleanup", 1),
                                ("setup", 2), ("cleanup", 2)])

    def test_missing_route_empty_body_is_not_implemented(self):
        op = {"method": "GET", "path": "/clients/{clientid}",
              "tag": "Clients",
              "operation": {"responses": {
                  "200": {"description": "ok"},
                  "404": {"description": "not found"}}}}
        case = {"operation": "GET /clients/{clientid}",
                "path_params": {"clientid": "c1"}}

        class StubRunner:
            base = "http://127.0.0.1:1/api/v5"
            headers = {}

            def _resolve_value(self, value):
                return value

            def run_setup(self, _case):
                pass

            def run_cleanup(self, _case):
                pass

        def stub_api_json(method, base, path, headers, query=None, body=None):
            return 404, None, b""

        row = checker.check_one({}, op, case, StubRunner(), False,
                                api_json_fn=stub_api_json)
        self.assertEqual(row["outcome"], "not-implemented")
        self.assertEqual(row["status"], 404)


if __name__ == "__main__":
    unittest.main()
