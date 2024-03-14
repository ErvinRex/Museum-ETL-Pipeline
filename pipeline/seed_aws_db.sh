source db.env
psql -h $DATABASE_IP -U $DATABASE_USERNAME -d $DATABASE_NAME -p $DATABASE_PORT -f schema.sql