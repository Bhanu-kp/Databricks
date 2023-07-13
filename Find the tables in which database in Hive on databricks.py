#For specific tables to find in which DB
tablenames=['Place the tables name as a list']
# collecting databses names
databases=spark.sql("SHOW DATABASES").select("databaseName").collect()
for db in databases:
    db=db.databaseName
    #collecting tables names
    tables_by_db = spark.sql("SHOW TABLES IN {db}".format(db=db)).select("tableName").collect()
    for table in tables_by_db:
        table_name = table.tableName
        for names in tablenames:
            #if re.search((names.lower()),table_name):
            if ('pbc_'+names.lower()+'_view') == table_name:
                df=spark.sql('select count(*) as c from {db}.{v}'.format(db=db,v=table_name))
                #df.show()
                df1=[row['c'] for row in df.select("c").collect()]
                print('"{i}" table is availble in {db} database and count:{df}'.format(i=table_name,db=db,df=df1[0]))
