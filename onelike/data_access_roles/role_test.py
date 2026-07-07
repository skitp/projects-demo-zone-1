current_role = manager.get_role("NoPiiColumns")

print("Current tables in NoPiiColumns role:")
if current_role:
    columns = current_role.get("decisionRules", [{}])[0].get("constraints", {}).get("columns", [])
    for col in columns:
        print(f"  - {col['tablePath']}")
else:
    print("Role does not exist")
