"""Template to Markdown converter for ML-research-science campaign templates.

This module fetches campaign template SQL files from GitHub, parses them,
maps IDs to human-readable names using lookup tables, and generates a
comprehensive markdown documentation file.
"""

import argparse
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple
import urllib.request
import urllib.error


def _load_env_file(path: str = ".env") -> None:
  """Load key=value pairs from a .env file into os.environ."""
  if not os.path.exists(path):
    return
  with open(path) as f:
    for line in f:
      line = line.strip()
      if not line or line.startswith("#") or "=" not in line:
        continue
      key, _, value = line.partition("=")
      os.environ.setdefault(key.strip(), value.strip())

_load_env_file()


# Base GitHub raw content URL
GITHUB_BASE_URL = "https://raw.githubusercontent.com/cognitiv/ML-research-science"
REPO_OWNER = "cognitiv"
REPO_NAME = "ML-research-science"

# File paths relative to repo root
TEMPLATE_FILE_PATH = "libs/ml_automation/src/rds/ddl/templates/core_campaign_template.sql"
LOOKUPS_DIR = "libs/ml_automation/src/rds/ddl/lookups"

# Lookup file names
LOOKUP_FILES = {
    "kpi_type_lookup": "kpi_type_lookup.sql",
    "creative_type_lookup": "creative_type_lookup.sql",
    "platform_type_lookup": "platform_type_lookup.sql",
    "event_source_type_lookup": "event_source_type_lookup.sql",
    "data_source_provider_lookup": "data_source_provider_lookup.sql",
    "line_item_type_lookup": "line_item_type_lookup.sql",
    "model_type_lookup": "model_type_lookup.sql",
    "budget_type_lookup": "budget_type_lookup.sql",
    "bid_algo_type_lookup": "bid_algo_type_lookup.sql",
    "tuner_algo_type_lookup": "tuner_algo_type_lookup.sql",
}


def fetch_file_from_github(branch: str, file_path: str) -> str:
  """Fetch a file from GitHub raw content URL.

  Args:
    branch: Git branch name (e.g., 'dev/enum_pull')
    file_path: Path to file relative to repo root

  Returns:
    File content as string

  Raises:
    urllib.error.URLError: If file cannot be fetched
    urllib.error.HTTPError: If HTTP error occurs (e.g., 404)
  """
  url = f"{GITHUB_BASE_URL}/{branch}/{file_path}"
  token = os.environ.get("GITHUB_TOKEN")
  if not token:
    raise ValueError("GITHUB_TOKEN environment variable is not set")
  request = urllib.request.Request(url, headers={"Authorization": f"token {token}"})
  try:
    with urllib.request.urlopen(request) as response:
      return response.read().decode("utf-8")
  except urllib.error.HTTPError as e:
    raise urllib.error.HTTPError(
        url, e.code, f"Failed to fetch {file_path}: {e.reason}",
        e.hdrs, e.fp
    )
  except urllib.error.URLError as e:
    raise urllib.error.URLError(
        f"Failed to fetch {file_path}: {e.reason}"
    )


def parse_lookup_table(content: str, lookup_name: str) -> Dict[int, str]:
  """Parse a lookup table SQL file and return ID to name mapping.

  Args:
    content: SQL file content
    lookup_name: Name of the lookup table (e.g., 'kpi_type_lookup')

  Returns:
    Dictionary mapping IDs to names

  Raises:
    ValueError: If lookup table cannot be parsed
  """
  mapping = {}

  # Extract INSERT VALUES block — handles optional column list before VALUES
  insert_pattern = (
      rf"INSERT INTO {lookup_name}\s*(?:\([^)]*\))?\s*VALUES\s*(.*?);"
  )
  match = re.search(insert_pattern, content, re.DOTALL | re.IGNORECASE)

  if not match:
    raise ValueError(f"Could not find INSERT statement in {lookup_name}")

  values_block = match.group(1)

  # Parse each row: (id, 'name', ...) — extra columns after name are ignored
  row_pattern = r"\(\s*(\d+)\s*,\s*['\"]([^'\"]+)['\"][^)]*\)"
  rows = re.findall(row_pattern, values_block)

  for row_id, row_name in rows:
    mapping[int(row_id)] = row_name

  if not mapping:
    raise ValueError(f"No mappings found in {lookup_name}")

  return mapping


def load_all_lookups(branch: str) -> Dict[str, Dict[int, str]]:
  """Load all lookup tables from GitHub.

  Args:
    branch: Git branch name

  Returns:
    Dictionary of lookup tables {lookup_name: {id: name}}

  Raises:
    urllib.error.URLError: If files cannot be fetched
    ValueError: If lookup tables cannot be parsed
  """
  lookups = {}

  for lookup_key, lookup_file in LOOKUP_FILES.items():
    file_path = f"{LOOKUPS_DIR}/{lookup_file}"
    print(f"Fetching {lookup_file}...")
    content = fetch_file_from_github(branch, file_path)
    lookups[lookup_key] = parse_lookup_table(content, lookup_key)

  return lookups


def extract_insert_statements(
    content: str
) -> List[Tuple[str, Dict[str, Any]]]:
  """Extract uncommented INSERT statements from template SQL file.

  Args:
    content: SQL file content

  Returns:
    List of tuples (template_name, parsed_values) where template_name
    is extracted from preceding comment

  Raises:
    ValueError: If INSERT statements cannot be parsed
  """
  inserts = []

  # Pattern to match comment + INSERT block
  # Matches: --N. Template Name followed by uncommented INSERT
  pattern = (
      r"--([\d.]+)\s*([^\n]+?)\s*\n"
      r"(?:--(?!\d)[^\n]*\n)*?"
      r"\s*INSERT INTO core_campaign_template\s*\((.*?)\)\s*VALUES\s*\("
      r"(.*?)\);"
  )

  matches = re.finditer(pattern, content, re.DOTALL | re.IGNORECASE)

  for match in matches:
    template_name = match.group(2).strip().rstrip("- \t")
    columns_str = match.group(3)
    values_str = match.group(4)

    # Parse columns
    columns = [
        col.strip() for col in columns_str.split(",")
        if col.strip()
    ]

    # Parse values - careful handling of arrays and JSON
    values = parse_sql_values(values_str)

    if len(values) != len(columns):
      raise ValueError(
          f"Column count ({len(columns)}) does not match "
          f"value count ({len(values)}) for template: {template_name}\n"
          f"  Columns: {columns}\n"
          f"  Values raw: {values_str[:300]!r}"
      )

    parsed_insert = dict(zip(columns, values))
    inserts.append((template_name, parsed_insert))

  return inserts


def parse_sql_values(values_str: str) -> List[Any]:
  """Parse SQL VALUES clause into Python values.

  Handles ARRAY[], NULL, numbers, strings, and JSONB.

  Args:
    values_str: SQL VALUES content

  Returns:
    List of parsed values

  Raises:
    ValueError: If values cannot be parsed
  """
  values = []
  current = ""
  in_array = False
  in_string = False
  in_json = False
  brace_count = 0

  i = 0
  while i < len(values_str):
    char = values_str[i]

    # Handle strings
    if char == "'" and (i == 0 or values_str[i - 1] != "\\"):
      in_string = not in_string
      current += char
      i += 1
      continue

    if in_string:
      current += char
      i += 1
      continue

    # Handle ARRAY
    if values_str[i:i+5] == "ARRAY":
      in_array = True
      current += char
      i += 1
      continue

    if in_array:
      current += char
      if char == "]":
        in_array = False
      i += 1
      continue

    # Handle type cast (::jsonb, ::text, etc.) — consume and append, don't enter json mode
    if values_str[i:i+2] == "::":
      while i < len(values_str) and (
          values_str[i] in ":_" or values_str[i].isalpha()
      ):
        current += values_str[i]
        i += 1
      continue

    # Handle bare JSON object not inside a string
    if char == "{" and not in_json:
      in_json = True
      brace_count = 1
      current += char
      i += 1
      continue

    if in_json:
      if char == "{":
        brace_count += 1
      elif char == "}":
        brace_count -= 1
      current += char
      if brace_count == 0:
        in_json = False
      i += 1
      continue

    # Handle value separator
    if char == ",":
      values.append(parse_single_value(current.strip()))
      current = ""
      i += 1
      continue

    current += char
    i += 1

  # Add last value
  if current.strip():
    values.append(parse_single_value(current.strip()))

  return values


def parse_single_value(value_str: str) -> Any:
  """Parse a single SQL value.

  Args:
    value_str: String representation of value

  Returns:
    Parsed Python value (list, dict, str, float, None, etc.)
  """
  value_str = value_str.strip()

  # NULL
  if value_str.upper() == "NULL":
    return None

  # Boolean
  if value_str.lower() == "true":
    return True
  if value_str.lower() == "false":
    return False

  # Array: ARRAY[1, 2, 3]
  if value_str.startswith("ARRAY["):
    array_content = value_str[6:-1]  # Remove ARRAY[ and ]
    elements = [elem.strip() for elem in array_content.split(",")]
    try:
      return [int(e) for e in elements]
    except ValueError:
      return elements

  # JSON/JSONB: '{"key": "value"}'::jsonb
  if "{" in value_str and ("::jsonb" in value_str or "::json" in value_str):
    json_str = value_str.split("::")[0].strip("'\"")
    try:
      return json.loads(json_str)
    except json.JSONDecodeError:
      return value_str

  # String: 'value' or "value"
  if (value_str.startswith("'") and value_str.endswith("'")) or (
      value_str.startswith('"') and value_str.endswith('"')
  ):
    return value_str[1:-1]

  # Number
  try:
    if "." in value_str:
      return float(value_str)
    return int(value_str)
  except ValueError:
    return value_str


def map_array_ids_to_names(
    ids: Optional[List[int]],
    lookup: Dict[int, str]
) -> str:
  """Map array of IDs to comma-separated names.

  Args:
    ids: List of IDs or None
    lookup: ID to name mapping dictionary

  Returns:
    Comma-separated names, or "NULL" if ids is None
  """
  if ids is None:
    return "NULL"

  names = []
  for id_val in ids:
    if id_val in lookup:
      names.append(lookup[id_val])
    else:
      names.append(f"ID_{id_val}")

  return ", ".join(names)


def format_json_value(value: Any) -> str:
  """Format JSON value with indentation.

  Args:
    value: JSON value (dict, list, etc.)

  Returns:
    Formatted JSON string with indentation
  """
  if value is None:
    return "NULL"

  if isinstance(value, dict):
    indent = "&nbsp;" * 4
    items = value.items()
    inner = "<br>".join(
        f'{indent}"{k}": {json.dumps(v)}{"," if i < len(value) - 1 else ""}'
        for i, (k, v) in enumerate(items)
    )
    return "{<br>" + inner + "<br>}"

  if isinstance(value, (list, str)):
    return json.dumps(value)

  return str(value)


def format_auxiliary_models(
    models_json: Optional[Dict[str, Any]],
    model_lookup: Dict[int, str]
) -> str:
  """Format auxiliary models JSON as key-value pairs.

  Args:
    models_json: Auxiliary models JSON object

  Returns:
    Formatted string with key-value pairs
  """
  if models_json is None:
    return "NULL"

  if not isinstance(models_json, dict):
    return str(models_json)

  # Extract auxiliary_models array
  if "auxiliary_models" in models_json:
    models = models_json["auxiliary_models"]
    if isinstance(models, list):
      lines = []
      for model in models:
        if isinstance(model, dict):
          model_type_id = model.get("model_type_id", "?")
          model_name = model_lookup.get(model_type_id, f"Model_{model_type_id}")
          required = model.get("required", False)
          lines.append(f"{model_name}: {required}")
      return ", <br>".join(lines) if lines else "NULL"

  return format_json_value(models_json)


def build_markdown_table(
    inserts: List[Tuple[str, Dict[str, Any]]],
    lookups: Dict[str, Dict[int, str]]
) -> str:
  """Build markdown table from parsed templates and lookups.

  Args:
    inserts: List of (template_name, values_dict) tuples
    lookups: Dictionary of lookup tables

  Returns:
    Markdown table string
  """
  # Extract lookups for convenience
  kpi_lookup = lookups.get("kpi_type_lookup", {})
  creative_lookup = lookups.get("creative_type_lookup", {})
  platform_lookup = lookups.get("platform_type_lookup", {})
  event_source_lookup = lookups.get("event_source_type_lookup", {})
  data_source_lookup = lookups.get("data_source_provider_lookup", {})
  line_item_lookup = lookups.get("line_item_type_lookup", {})
  model_lookup = lookups.get("model_type_lookup", {})
  budget_lookup = lookups.get("budget_type_lookup", {})
  bid_algo_lookup = lookups.get("bid_algo_type_lookup", {})
  tuner_algo_lookup = lookups.get("tuner_algo_type_lookup", {})

  # Column headers
  headers = [
      "Row_No",
      "Template_Name",
      "KPI_type",
      "creative_type",
      "is_rich_media",
      "platform_type",
      "event_source_type",
      "data_source_provider",
      "line_item_type",
      "Model_type",
      "budget_weight",
      "budget_type",
      "margin_target",
      "Bid_algo",
      "Bid_algo_params",
      "Tuning_algo",
      "Tuning_algo_params",
      "Auxiliary_Models",
  ]

  # Build rows
  rows = []
  for row_num, (template_name, values) in enumerate(inserts, 1):
    # Map IDs to names
    kpi_names = map_array_ids_to_names(
        values.get("kpi_type_ids"), kpi_lookup
    )
    creative_names = map_array_ids_to_names(
        values.get("creative_type_ids"), creative_lookup
    )
    platform_names = map_array_ids_to_names(
        values.get("platform_type_ids"), platform_lookup
    )
    event_source_names = map_array_ids_to_names(
        values.get("event_source_type_ids"), event_source_lookup
    )
    data_source_names = map_array_ids_to_names(
        values.get("data_source_provider_ids"), data_source_lookup
    )

    line_item_id = values.get("line_item_type_id")
    line_item_name = (
        line_item_lookup.get(line_item_id, f"ID_{line_item_id}")
        if line_item_id else "NULL"
    )

    model_id = values.get("model_type_id")
    model_name = (
        model_lookup.get(model_id, f"ID_{model_id}")
        if model_id else "NULL"
    )

    budget_type_id = values.get("budget_type")
    budget_type_name = (
        budget_lookup.get(budget_type_id, f"ID_{budget_type_id}")
        if budget_type_id else "NULL"
    )
    budget_type_str = (
        f"{budget_type_id} ({budget_type_name})"
        if budget_type_id else "NULL"
    )

    bid_algo_id = values.get("bid_algo_type_id")
    bid_algo_name = (
        bid_algo_lookup.get(bid_algo_id, f"ID_{bid_algo_id}")
        if bid_algo_id else "NULL"
    )

    tuner_algo_id = values.get("tuner_algo_type_id")
    tuner_algo_name = (
        tuner_algo_lookup.get(tuner_algo_id, f"ID_{tuner_algo_id}")
        if tuner_algo_id else "NULL"
    )

    bid_params = values.get("bid_parameters")
    bid_params_str = (
        format_json_value(bid_params)
        if bid_params else "NULL"
    )

    tuner_params = values.get("tuner_parameters")
    tuner_params_str = (
        format_json_value(tuner_params)
        if tuner_params else "NULL"
    )

    aux_models = values.get("auxiliary_model_types")
    aux_models_str = format_auxiliary_models(aux_models, model_lookup)

    row = [
        str(row_num),
        template_name,
        kpi_names,
        creative_names,
        str(values.get("is_rich_media", "NULL")),
        platform_names,
        event_source_names,
        data_source_names,
        line_item_name,
        model_name,
        str(values.get("budget_weight", "NULL")),
        budget_type_str,
        str(values.get("margin_target", "NULL")),
        bid_algo_name,
        bid_params_str,
        tuner_algo_name,
        tuner_params_str,
        aux_models_str,
    ]
    rows.append(row)

  def _cell(value: str) -> str:
    return value.replace("\n", " ").replace("|", "\\|")

  # Build markdown table
  markdown = "| " + " | ".join(headers) + " |\n"
  markdown += "|" + "|".join(["---"] * len(headers)) + "|\n"

  for row in rows:
    markdown += "| " + " | ".join(_cell(v) for v in row) + " |\n"

  return markdown


def main(branch: str) -> None:
  """Main function to orchestrate template to markdown conversion.

  Args:
    branch: Git branch name (e.g., 'dev/enum_pull')

  Raises:
    urllib.error.URLError: If files cannot be fetched
    ValueError: If parsing fails
  """
  print(f"Fetching files from branch: {branch}")
  print()

  # Load lookups
  print("Loading lookup tables...")
  try:
    lookups = load_all_lookups(branch)
    print(f"Successfully loaded {len(lookups)} lookup tables")
  except Exception as e:
    print(f"Error loading lookups: {e}")
    raise

  print()

  # Fetch and parse template file
  print("Fetching core_campaign_template.sql...")
  try:
    template_content = fetch_file_from_github(branch, TEMPLATE_FILE_PATH)
  except Exception as e:
    print(f"Error fetching template file: {e}")
    raise

  print("Parsing INSERT statements...")
  try:
    inserts = extract_insert_statements(template_content)
    print(f"Found {len(inserts)} uncommented templates")
  except Exception as e:
    print(f"Error parsing INSERT statements: {e}")
    raise

  print()

  # Build markdown
  print("Building markdown table...")
  try:
    markdown = build_markdown_table(inserts, lookups)
  except Exception as e:
    print(f"Error building markdown: {e}")
    raise

  # Write to file
  output_file = "campaign_templates_documentation.md"
  print(f"Writing output to {output_file}...")
  try:
    with open(output_file, "w", encoding="utf-8") as f:
      f.write("# Campaign Templates Documentation\n\n")
      f.write(f"Generated from branch: `{branch}`\n\n")
      f.write(markdown)
    print(f"Successfully wrote {output_file}")
  except IOError as e:
    print(f"Error writing output file: {e}")
    raise

  print()
  print("✓ Conversion complete!")


if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      description="Convert campaign templates to markdown documentation"
  )
  parser.add_argument(
      "branch",
      help="Git branch name (e.g., dev/enum_pull)",
  )

  args = parser.parse_args()

  try:
    main(args.branch)
  except Exception as e:
    print(f"\n✗ Error: {e}")
    exit(1)