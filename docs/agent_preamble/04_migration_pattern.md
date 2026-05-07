### 4. Migration pattern

Every new migration must include the GRANT footer (see `migrations/_TEMPLATE.sql`):

```sql
GRANT ALL ON <new_table> TO grid;
GRANT USAGE, SELECT ON SEQUENCE <new_table>_id_seq TO grid;
```

[[migrations|Migrations]] without this footer break the `grid` runtime role. Migrations target the **`griddb`** database (not `grid`) — apply via:

```bash
ssh grid@100.75.185.36 "psql -d griddb -f /data/grid_v4/grid_repo/migrations/<file>.sql"
```
