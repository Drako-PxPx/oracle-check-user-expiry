select case
         when rolvaliduntil is null or rolvaliduntil = 'infinity' then null
         else extract(epoch from rolvaliduntil - now()) / 86400
       end as expiry_days
  from pg_roles
 where rolname = current_user
