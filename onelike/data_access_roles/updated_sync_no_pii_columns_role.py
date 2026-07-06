def sync_no_pii_columns_role(
    self,
    members: List[Dict],
    pii_tables_and_safe_columns: Dict[str, List[str]],
    role_name: str = "NoPiiColumns"
) -> bool:
    """
    Smart sync for NoPiiColumns role.
    - Merges new/updated tables into existing role.
    - Never removes tables that already exist in the role.
    - Designed for single-table processing per notebook run.
    """

    if not pii_tables_and_safe_columns:
        logger.info("No tables to process for NoPiiColumns role.")
        return False

    # Build the new column rules for incoming tables
    new_column_rules = []
    for table_path, visible_columns in pii_tables_and_safe_columns.items():
        new_column_rules.append({
            "tablePath": table_path,
            "columnNames": visible_columns,
            "columnEffect": "Permit",
            "columnAction": ["Read"]
        })

    current_role = self.get_role(role_name)

    if not current_role:
        # Role doesn't exist yet → create it fresh
        decision_rules = [{
            "effect": "Permit",
            "permission": [
                {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
                {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
            ],
            "constraints": {
                "columns": new_column_rules
            }
        }]
        return self.sync_role(role_name=role_name, members=members, decision_rules=decision_rules)

    # Role exists → MERGE (do not remove existing tables)
    existing_constraints = current_role.get("decisionRules", [{}])[0].get("constraints", {})
    existing_columns = existing_constraints.get("columns", [])

    # Create a lookup of existing table paths
    existing_table_paths = {rule["tablePath"]: rule for rule in existing_columns}

    # Merge: update existing tables or add new ones
    final_column_rules = []
    for rule in new_column_rules:
        existing_table_paths[rule["tablePath"]] = rule   # overwrite if exists, add if new

    final_column_rules = list(existing_table_paths.values())

    # Rebuild decision rules with merged columns
    decision_rules = [{
        "effect": "Permit",
        "permission": [
            {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
            {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
        ],
        "constraints": {
            "columns": final_column_rules
        }
    }]

    # Use sync_role for change detection + update
    return self.sync_role(
        role_name=role_name,
        members=members,
        decision_rules=decision_rules
    )
