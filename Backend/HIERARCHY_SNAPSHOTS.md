# Hierarchy snapshots

Export the current hierarchy directly from the database:

```bat
cd C:\Users\InCalVBeyond\Incentive_Calc_PostgreSQL\Backend
npm run export-hierarchy
```

The command creates a timestamped file under `Backend\hierarchy_snapshots`. It
does not modify `Backend\hierarchy_data.json` or the database. Before writing a
snapshot, it checks that the `User` and `EmployeeProfile` manager assignments
match, every team has one L1 owner, reporting chains contain no cycles, and no
team member would be omitted.

To save directly to another drive or synced folder:

```bat
npm run export-hierarchy -- --output "D:\IncentiveBackups\hierarchy-current.json"
```

Keep at least one copy outside the VPS. Snapshot files are intentionally ignored
by Git so routine pulls cannot conflict with them.

To reuse a verified snapshot later, copy it over `Backend\hierarchy_data.json`,
review the change with `git diff`, and run the existing hierarchy sync in dry-run
mode first:

```bat
npm run sync-hierarchy-assignments
```

Only after reviewing the dry-run output should changes be applied:

```bat
npm run sync-hierarchy-assignments -- --apply
```
