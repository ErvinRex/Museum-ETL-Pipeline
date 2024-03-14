source db.env
psql -h $DATABASE_IP -U $DATABASE_USERNAME -d $DATABASE_NAME -p $DATABASE_PORT -c "DELETE FROM rating_instance"
psql -h $DATABASE_IP -U $DATABASE_USERNAME -d $DATABASE_NAME -p $DATABASE_PORT -c "DELETE FROM support_instance"
echo 'Instance tables reset'