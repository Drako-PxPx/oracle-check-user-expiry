select expiry_date - sysdate as expiry_days
  from dba_users
 where username = sys_context(
   'USERENV',
   'SESSION_USER'
)