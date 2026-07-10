def _create_or_update_single_role(self, role: Dict) -> Dict:
    """
    Create or update a **single** data access role using the granular API.
    This is more efficient than batch when you only need to touch one role.
    """
    url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{self.item_id}/dataAccessRoles"
    
    if self.use_preview:
        url += "?preview=true"
    
    # Use Overwrite policy so it creates if missing or updates if exists
    params = {"dataAccessRoleConflictPolicy": "Overwrite"}

    try:
        resp = self._request(
            method="POST",
            url=url,
            json_body=role,
            params=params
        )
        logger.debug(f"Single-role operation successful for '{role.get('name')}'")
        return {"status": resp.status_code, "role": resp.json() if resp.content else None}
    
    except OneLakeSecurityError as e:
        logger.warning(f"Single-role POST failed for '{role.get('name')}': {e}")
        raise  # Let the caller decide to fallback to batch

def create_or_update_role(self, role: Dict) -> Dict:
    """
    Create or update a role.
    Default: Uses efficient single-role API.
    Falls back to batch if single-role fails.
    """
    try:
        return self._create_or_update_single_role(role)
    except Exception as e:
        logger.warning(f"Single-role operation failed for '{role.get('name')}', falling back to batch mode: {e}")
        return self._upsert_role_via_batch(role)

def sync_role(
    self,
    role_name: str,
    members: List[Dict],
    decision_rules: List[Dict],
    role_kind: str = "Policy",
    use_batch: bool = False
) -> bool:
    """
    Smart sync for any OneLake Data Access Role.

    - Default: Uses single-role API (efficient)
    - use_batch=True: Forces full batch replace (safer in rare cases)
    """

    logger.info(f"🔄 Syncing role '{role_name}' (use_batch={use_batch})")

    current_role = self.get_role(role_name)

    new_role = {
        "name": role_name,
        "kind": role_kind,
        "decisionRules": decision_rules,
        "members": {"microsoftEntraMembers": members}
    }

    if not current_role:
        if use_batch:
            self._upsert_role_via_batch(new_role)
        else:
            self.create_or_update_role(new_role)
        logger.info(f"✅ Created role '{role_name}'")
        return True

    # Check if update is needed
    current_decision = current_role.get("decisionRules", [])
    current_members = current_role.get("members", {}).get("microsoftEntraMembers", [])

    if current_decision == decision_rules and current_members == members:
        logger.info(f"✅ Role '{role_name}' is already up to date")
        return False

    # Perform update
    if use_batch:
        self._upsert_role_via_batch(new_role)
    else:
        self.create_or_update_role(new_role)

    logger.info(f"✅ Updated role '{role_name}'")
    return True

Method,Behavior,Default
sync_role(),Prefers single-role API,use_batch=False
create_or_update_role(),"Tries single-role first, falls back to batch",Single
_create_or_update_single_role(),New method – calls granular POST API,—
sync_no_pii_columns_role(),Passes through use_batch parameter,False
