def sync_role(
    self,
    role_name: str,
    members: List[Dict],
    decision_rules: List[Dict],
    role_kind: str = "Policy",
    use_batch: bool = False          # ← NEW PARAMETER (default = False)
) -> bool:
    """
    Smart sync for any OneLake Data Access Role.

    Default behavior: Uses single-role operations (more efficient).
    Set use_batch=True to force full batch replace (safer in some edge cases).
    """

    logger.info(f"🔄 Starting smart sync for role: {role_name} (use_batch={use_batch})")

    current_role = self.get_role(role_name)

    # Build the role payload
    new_role = {
        "name": role_name,
        "kind": role_kind,
        "decisionRules": decision_rules,
        "members": {"microsoftEntraMembers": members}
    }

    if not current_role:
        # Role does not exist → create it
        if use_batch:
            self._upsert_role_via_batch(new_role)
        else:
            self.create_or_update_role(new_role)
        logger.info(f"✅ Role '{role_name}' created")
        return True

    # Role exists → check if anything changed
    current_decision_rules = current_role.get("decisionRules", [])
    current_members = current_role.get("members", {}).get("microsoftEntraMembers", [])

    if current_decision_rules == decision_rules and current_members == members:
        logger.info(f"✅ Role '{role_name}' is already up to date. No update needed.")
        return False

    # Update the role
    if use_batch:
        self._upsert_role_via_batch(new_role)
    else:
        self.create_or_update_role(new_role)

    logger.info(f"✅ Role '{role_name}' synced successfully")
    return True

Also Update create_or_update_role() (Recommended)
Make create_or_update_role() prefer single-role operations:

def sync_no_pii_columns_role(
    self,
    members: List[Dict],
    pii_tables_and_safe_columns: Dict[str, List[str]],
    role_name: str = "NoPiiColumns",
    use_batch: bool = False          # ← NEW: pass through to sync_role
) -> bool:

    if not pii_tables_and_safe_columns:
        logger.info("No tables require PII column restrictions.")
        return False

    # Build decision rules (same as before)...
    column_rules = []
    for table_path, visible_columns in pii_tables_and_safe_columns.items():
        column_rules.append({
            "tablePath": table_path,
            "columnNames": visible_columns,
            "columnEffect": "Permit",
            "columnAction": ["Read"]
        })

    decision_rules = [{
        "effect": "Permit",
        "permission": [
            {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
            {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
        ],
        "constraints": {"columns": column_rules}
    }]

    return self.sync_role(
        role_name=role_name,
        members=members,
        decision_rules=decision_rules,
        use_batch=use_batch          # ← Pass the parameter
    )

