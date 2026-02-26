select (valuntil - current_date) as expiry_days
  from pg_shadow
 where usename = current_user