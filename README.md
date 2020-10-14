# DynamoRepilcateMongo
Guide: https://www.percona.com/blog/2020/05/20/real-time-replication-from-dynamodb-to-mongodb/

###Deploy the Lambda Function
In root folder, Install libraries in a new, project-local package directory with pip’s –target option.
```
 pip install --target ./package pymongo
 pip install --target ./package dynamodb_json
```

Create a ZIP archive of the dependencies
```
cd package
zip -r9 ${OLDPWD}/function.zip .
```

Add your function code to the archive
```
cd $OLDPWD
zip -g function.zip replicator.py
```
After that, upload function.zip to AWS lambda
